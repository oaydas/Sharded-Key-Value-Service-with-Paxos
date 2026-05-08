package shardkv

import (
	"time"

	"umich.edu/eecs491/proj4/common"
)

const (
	OpClient  = 1
	OpInstall = 2
	OpRelease = 3
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
type Op struct {
	ID   int64
	Kind int

	// Client op (Get / Put / Append)
	ClientID int64
	OpID     int64
	Key      string
	Value    string
	PutOp    string // "Put", "Append", or "Get"

	// Install / Release
	ConfigNum int
	Shard     int
	KVData    map[string]string // Install only
	OpCache   map[int64]int64   // Install only
}

// Method used by PaxosRSM to determine if two Op values are identical.
func equals(v1 interface{}, v2 interface{}) bool {
	return v1.(Op).ID == v2.(Op).ID
}

// Channel message types flowing into stateLoop.

type applyMsg struct {
	op  Op
	ack chan struct{}
}

// Used by Get/PutAppend RPC handlers to read a committed op's result
// after AddOp has returned.
type clientResultMsg struct {
	clientID int64
	opID     int64
	reply    chan clientResult
}

type clientResult struct {
	err   Err
	value string
}

// lastResult tracks the most recent op's result per client, captured at
// apply time. Because clients issue ops sequentially, one entry per
// client suffices.
type lastResult struct {
	opID   int64
	result clientResult
}

// Used by AssignShard to check whether a shard has already been installed
// at this configNum so the RPC can be idempotent.
type installedQueryMsg struct {
	shard     int
	configNum int
	reply     chan bool
}

// Used by PullShard to snapshot a released shard's data + dup-cache.
type pullSnapshotMsg struct {
	shard     int
	configNum int
	reply     chan pullSnapshot
}

type pullSnapshot struct {
	err     Err
	kvData  map[string]string
	opCache map[int64]int64
}

// additions to ShardKV state
type ShardKVImpl struct {
	reqCh chan interface{}
}

// initialize kv.impl.*
func (kv *ShardKV) InitImpl() {
	kv.impl.reqCh = make(chan interface{})
	go kv.stateLoop()
}

// stateLoop owns every piece of replicated state as locals. No other
// goroutine ever touches them, so no mutexes needed.
func (kv *ShardKV) stateLoop() {
	// shard -> key -> value
	store := make(map[int]map[string]string, common.NShards)
	// shard -> owned by this group right now?
	owned := make(map[int]bool, common.NShards)
	// shard -> clientID -> last applied opID (for duplicate detection)
	opCache := make(map[int]map[int64]int64, common.NShards)
	// shard -> highest configNum at which an install or release landed
	lastConfig := make(map[int]int, common.NShards)
	// clientID -> most recent (opID, result), populated at apply time
	results := make(map[int64]lastResult)

	for s := 0; s < common.NShards; s++ {
		store[s] = map[string]string{}
		opCache[s] = map[int64]int64{}
	}

	for {
		select {
		case <-kv.term:
			return
		case raw := <-kv.impl.reqCh:
			switch m := raw.(type) {
			case applyMsg:
				kv.applyOne(store, owned, opCache, lastConfig, results, m.op)
				close(m.ack)

			case clientResultMsg:
				r, ok := results[m.clientID]
				if !ok || r.opID != m.opID {
					// Shouldn't happen: AddOp blocks until apply,
					// and clients serialize their own ops.
					select {
					case m.reply <- clientResult{err: OK}:
					case <-kv.term:
						return
					}
				} else {
					select {
					case m.reply <- r.result:
					case <-kv.term:
						return
					}
				}

			case installedQueryMsg:
				installed := lastConfig[m.shard] >= m.configNum
				select {
				case m.reply <- installed:
				case <-kv.term:
					return
				}

			case pullSnapshotMsg:
				var snap pullSnapshot
				if lastConfig[m.shard] == m.configNum && !owned[m.shard] {
					// Released at this configNum — return a fresh copy
					// of the shard's data and dup cache. Copying so the
					// caller can't mutate our local state.
					kvCopy := make(map[string]string, len(store[m.shard]))
					for k, v := range store[m.shard] {
						kvCopy[k] = v
					}
					cacheCopy := make(map[int64]int64, len(opCache[m.shard]))
					for c, id := range opCache[m.shard] {
						cacheCopy[c] = id
					}
					snap = pullSnapshot{err: OK, kvData: kvCopy, opCache: cacheCopy}
				} else {
					// lastConfig < configNum: Release hasn't applied yet
					// (shouldn't happen post-AddOp), or
					// lastConfig > configNum: we've moved past this
					// snapshot and the data no longer matches.
					snap.err = ErrOld
				}
				select {
				case m.reply <- snap:
				case <-kv.term:
					return
				}
			}
		}
	}
}

// applyOne mutates state based on the decided op. Each case is
// idempotent so repeated applies (from retries) are safe.
func (kv *ShardKV) applyOne(
	store map[int]map[string]string,
	owned map[int]bool,
	opCache map[int]map[int64]int64,
	lastConfig map[int]int,
	results map[int64]lastResult,
	op Op,
) {
	switch op.Kind {
	case OpClient:
		shard := common.Key2Shard(op.Key)

		// Not our shard: record ErrWrongGroup without touching the
		// dup cache. The client will retry at the correct group with
		// the same OpID.
		if !owned[shard] {
			if existing, ok := results[op.ClientID]; !ok || op.OpID >= existing.opID {
				results[op.ClientID] = lastResult{
					opID:   op.OpID,
					result: clientResult{err: ErrWrongGroup},
				}
			}
			return
		}

		// Apply only if not a duplicate. Puts/Appends mutate; Gets
		// read-only. Cache is updated only on successful apply.
		// Use existence + value check because OpIDs start at 0 and
		// zero-valued missing-map entries would otherwise look like
		// a duplicate of the very first op.
		stored, seen := opCache[shard][op.ClientID]
		if !seen || stored < op.OpID {
			switch op.PutOp {
			case "Put":
				store[shard][op.Key] = op.Value
			case "Append":
				store[shard][op.Key] += op.Value
			case "Get":
				// nothing to mutate
			}
			opCache[shard][op.ClientID] = op.OpID
		}

		// Capture the result that the proposing replica's RPC handler
		// will read back. For Get, snapshot the current value so it
		// reflects this op's position in the log.
		var r clientResult
		if op.PutOp == "Get" {
			v, ok := store[shard][op.Key]
			if !ok {
				r = clientResult{err: ErrNoKey}
			} else {
				r = clientResult{err: OK, value: v}
			}
		} else {
			r = clientResult{err: OK}
		}
		// Guard against late Paxos duplicates of an older opID
		// overwriting a newer op's result. results is keyed by
		// clientID only, so without this a seq-N+2 apply of op 5
		// (from a slow straggler RPC) could clobber the op 6 result
		// that a concurrent op 6 handler is about to read.
		if existing, ok := results[op.ClientID]; !ok || op.OpID >= existing.opID {
			results[op.ClientID] = lastResult{opID: op.OpID, result: r}
		}

	case OpInstall:
		// Idempotency: skip if this shard already transitioned at or
		// past this config number.
		if lastConfig[op.Shard] >= op.ConfigNum {
			return
		}

		owned[op.Shard] = true

		// Replace the shard's data with a fresh copy of op.KVData.
		freshStore := make(map[string]string, len(op.KVData))
		for k, v := range op.KVData {
			freshStore[k] = v
		}
		store[op.Shard] = freshStore

		// Same treatment for the dup-detection cache.
		freshCache := make(map[int64]int64, len(op.OpCache))
		for c, id := range op.OpCache {
			freshCache[c] = id
		}
		opCache[op.Shard] = freshCache

		lastConfig[op.Shard] = op.ConfigNum

	case OpRelease:
		// Idempotent: skip if we've already transitioned at this config
		// or later.
		if lastConfig[op.Shard] >= op.ConfigNum {
			return
		}
		// Can't release a shard we don't currently own. Happens when
		// the shardmaster dispatches an out-of-order AssignShard for a
		// future config to a new owner that asks us to pull, but we
		// haven't installed the intermediate config yet. Leaving
		// lastConfig unchanged makes pullSnapshot return ErrOld so the
		// puller retries after we eventually install and re-release.
		if !owned[op.Shard] {
			return
		}

		// Stop accepting client ops for this shard, but keep store and
		// opCache intact so PullShard can still hand the data back (the
		// new owner may retry after transient failures).
		owned[op.Shard] = false
		lastConfig[op.Shard] = op.ConfigNum
	}
}

// Execute operation encoded in decided value v and update local state.
func (kv *ShardKV) ApplyOp(v interface{}) {
	op := v.(Op)
	ack := make(chan struct{})
	select {
	case <-kv.term:
		return
	case kv.impl.reqCh <- applyMsg{op: op, ack: ack}:
	}
	select {
	case <-kv.term:
	case <-ack:
	}
}

// RPC handler for client Get requests.
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.rsm.AddOp(Op{
		ID:       common.Nrand(),
		Kind:     OpClient,
		ClientID: args.ClientID,
		OpID:     args.OpID,
		Key:      args.Key,
		PutOp:    "Get",
	})

	r, ok := kv.readResult(args.ClientID, args.OpID)
	if !ok {
		return nil
	}
	reply.Err = r.err
	reply.Value = r.value
	return nil
}

// RPC handler for client Put and Append requests.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.rsm.AddOp(Op{
		ID:       common.Nrand(),
		Kind:     OpClient,
		ClientID: args.ClientID,
		OpID:     args.OpID,
		Key:      args.Key,
		Value:    args.Value,
		PutOp:    args.Op,
	})

	r, ok := kv.readResult(args.ClientID, args.OpID)
	if !ok {
		return nil
	}
	reply.Err = r.err
	return nil
}

// readResult asks stateLoop for the last applied op's result for this
// client. Safe to call only after AddOp has returned, since the apply
// for (clientID, opID) has landed by then.
func (kv *ShardKV) readResult(clientID, opID int64) (clientResult, bool) {
	ch := make(chan clientResult, 1)
	select {
	case <-kv.term:
		return clientResult{}, false
	case kv.impl.reqCh <- clientResultMsg{clientID: clientID, opID: opID, reply: ch}:
	}
	select {
	case <-kv.term:
		return clientResult{}, false
	case r := <-ch:
		return r, true
	}
}

// Assign a shard to this group. Called by ShardMaster.
func (kv *ShardKV) AssignShard(args *common.AssignArgs, reply *common.AssignReply) error {
	// Idempotency short-circuit: if we've already installed this shard
	// at this configNum (or later), return OK without another Paxos round.
	if kv.alreadyInstalled(args.Shard, args.ConfigNum) {
		return nil
	}

	var kvData map[string]string
	var cache map[int64]int64

	if len(args.Servers) == 0 {
		// No prior owner (e.g. transitioning from GID 0 at the first
		// Join). Install empty state.
		kvData = map[string]string{}
		cache = map[int64]int64{}
	} else {
		// Pull the shard from one of the previous owners. Try every
		// server per round; if none answer, back off and retry. Block
		// until a pull succeeds — the shardmaster considers AssignShard
		// "delivered" as soon as the RPC returns, so we must not return
		// before the Install is logged.
		for {
			if kv.isdead() {
				return nil
			}
			got := false
			for _, srv := range args.Servers {
				pa := PullArgs{ConfigNum: args.ConfigNum, Shard: args.Shard}
				var pr PullReply
				if common.Call(srv, "ShardKV.PullShard", &pa, &pr) && pr.Err == OK {
					kvData = pr.KVStore
					cache = pr.OpIDCache
					if kvData == nil {
						kvData = map[string]string{}
					}
					if cache == nil {
						cache = map[int64]int64{}
					}
					got = true
					break
				}
			}
			if got {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	kv.rsm.AddOp(Op{
		ID:        common.Nrand(),
		Kind:      OpInstall,
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
		KVData:    kvData,
		OpCache:   cache,
	})
	return nil
}

// alreadyInstalled asks stateLoop whether a shard has already been
// installed at configNum (or a later config).
func (kv *ShardKV) alreadyInstalled(shard, configNum int) bool {
	ch := make(chan bool, 1)
	select {
	case <-kv.term:
		return false
	case kv.impl.reqCh <- installedQueryMsg{shard: shard, configNum: configNum, reply: ch}:
	}
	select {
	case <-kv.term:
		return false
	case b := <-ch:
		return b
	}
}

// Pull a shard from this group. Called by another shardkv server.
func (kv *ShardKV) PullShard(args *PullArgs, reply *PullReply) error {
	// Log an OpRelease at args.ConfigNum before snapshotting. applyOne
	// is idempotent via lastConfig[shard] >= ConfigNum, so a retrying
	// puller won't cause duplicate work. Once Release applies, the shard
	// is frozen (owned=false) so no client mutation can race the pull.
	kv.rsm.AddOp(Op{
		ID:        common.Nrand(),
		Kind:      OpRelease,
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
	})

	snap, ok := kv.pullSnapshot(args.Shard, args.ConfigNum)
	if !ok {
		return nil
	}
	reply.Err = snap.err
	reply.KVStore = snap.kvData
	reply.OpIDCache = snap.opCache
	return nil
}

// pullSnapshot asks stateLoop for a frozen copy of shard's data + dup
// cache at configNum. Safe to call only after OpRelease(configNum) has
// been AddOp'd, so the expected state (lastConfig == configNum, !owned)
// has been established.
func (kv *ShardKV) pullSnapshot(shard, configNum int) (pullSnapshot, bool) {
	ch := make(chan pullSnapshot, 1)
	select {
	case <-kv.term:
		return pullSnapshot{}, false
	case kv.impl.reqCh <- pullSnapshotMsg{shard: shard, configNum: configNum, reply: ch}:
	}
	select {
	case <-kv.term:
		return pullSnapshot{}, false
	case s := <-ch:
		return s, true
	}
}
