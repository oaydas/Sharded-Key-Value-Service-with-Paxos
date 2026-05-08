package shardmaster

import (
	"slices"
	"time"

	"umich.edu/eecs491/proj4/common"
)

const (
	OpJoin  = 1
	OpLeave = 2
	OpMove  = 3
	OpQuery = 4
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
type Op struct {
	ID       int64
	Kind     int
	GID      int64
	Servers  []string
	Shard    int
	QueryNum int
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	return v1.(Op).ID == v2.(Op).ID
}

type applyMsg struct {
	op  Op
	ack chan struct{}
}

type queryMsg struct {
	num   int
	reply chan Config
}

// opConfigMsg asks stateLoop which configNum an opID produced (or
// effected, for duplicate Joins/Leaves).
type opConfigMsg struct {
	opID  int64
	reply chan int
}

// additions to ShardMaster state
type ShardMasterImpl struct {
	reqCh chan interface{}
}

// initialize sm.impl.*
func (sm *ShardMaster) InitImpl() {
	sm.impl.reqCh = make(chan interface{})
	go sm.stateLoop()
}

// sortedGIDs returns the keys of groups in ascending order. Used everywhere
// we iterate groups to guarantee determinism across replicas (Go's native
// map iteration order is randomized).
func sortedGIDs(groups map[int64][]string) []int64 {
	gids := make([]int64, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	slices.Sort(gids)
	return gids
}

// shardCounts returns gid -> number of shards currently assigned to it.
// Every gid present in nc.Groups is included, even if it owns zero shards,
// so callers looking for "group with the fewest shards" see empty groups
// as valid recipients. Shards assigned to gids not in Groups (e.g. GID 0
// in config 0) are ignored.
func shardCounts(nc Config) map[int64]int {
	counts := make(map[int64]int, len(nc.Groups))
	for gid := range nc.Groups {
		counts[gid] = 0
	}
	for _, gid := range nc.Shards {
		if _, ok := counts[gid]; ok {
			counts[gid]++
		}
	}
	return counts
}

// copy config and increment num
func copyConfig(prev Config, newNum int) Config {
	nc := Config{
		Num:    newNum,
		Shards: prev.Shards,
		Groups: make(map[int64][]string, len(prev.Groups)),
	}

	for gid, srvs := range prev.Groups {
		nc.Groups[gid] = srvs
	}

	return nc
}

// stateLoop is the sole owner of the configs history and the opConfig
// map. All reads and writes happen on this goroutine.
func (sm *ShardMaster) stateLoop() {
	configs := []Config{{
		Num:    0,
		Groups: map[int64][]string{},
	}}
	// opID -> configNum that the op produced (or, for duplicate
	// Joins/Leaves, the earlier config that effected the change).
	// Entries are removed by the handler when it calls opConfigFor.
	opConfig := map[int64]int{}

	for {
		select {
		case <-sm.term:
			return
		case raw := <-sm.impl.reqCh:
			switch m := raw.(type) {
			case applyMsg:
				if num := sm.applyOne(&configs, m.op); num > 0 {
					opConfig[m.op.ID] = num
				}
				close(m.ack)
			case queryMsg:
				var c Config
				if m.num < 0 || m.num >= len(configs) {
					c = configs[len(configs)-1]
				} else {
					c = configs[m.num]
				}
				select {
				case m.reply <- c:
				case <-sm.term:
					return
				}
			case opConfigMsg:
				n := opConfig[m.opID]
				delete(opConfig, m.opID)
				select {
				case m.reply <- n:
				case <-sm.term:
					return
				}
			}
		}
	}
}

// applyOne mutates configs based on op. Returns the configNum that
// effects this op (0 if nothing needs to be dispatched: Query, or a
// duplicate whose effecting config couldn't be located).
func (sm *ShardMaster) applyOne(configs *[]Config, op Op) int {
	switch op.Kind {
	case OpQuery:
		return 0

	case OpJoin:
		prev := (*configs)[len(*configs)-1]
		// Duplicate Join of an already-present GID: find the earlier
		// config that added this GID so the handler can re-dispatch
		// its transition (covers the case where the original handler
		// crashed mid-dispatch).
		if _, exists := prev.Groups[op.GID]; exists {
			for i := 1; i < len(*configs); i++ {
				_, was := (*configs)[i-1].Groups[op.GID]
				_, now := (*configs)[i].Groups[op.GID]
				if !was && now {
					return i
				}
			}
			return 0
		}

		nc := copyConfig(prev, prev.Num+1)
		nc.Groups[op.GID] = op.Servers

		target := common.NShards / len(nc.Groups)
		claimed := 0

		// Phase 1: claim shards that aren't owned by any current group
		// (e.g. GID 0 in config 0, or orphans left behind somehow).
		// Iterating in shard-index order keeps replicas in lockstep.
		for s := 0; s < common.NShards && claimed < target; s++ {
			if _, ok := nc.Groups[nc.Shards[s]]; !ok {
				nc.Shards[s] = op.GID
				claimed++
			}
		}

		// Phase 2: steal one shard at a time from whichever group
		// currently holds the most.
		for claimed < target {
			sc := shardCounts(nc)
			var maxGID int64
			maxCount := -1
			for _, gid := range sortedGIDs(nc.Groups) {
				if gid == op.GID {
					continue
				}
				if sc[gid] > maxCount {
					maxCount = sc[gid]
					maxGID = gid
				}
			}
			for s := 0; s < common.NShards; s++ {
				if nc.Shards[s] == maxGID {
					nc.Shards[s] = op.GID
					claimed++
					break
				}
			}
		}

		*configs = append(*configs, nc)
		return nc.Num

	case OpLeave:
		prev := (*configs)[len(*configs)-1]
		// Duplicate Leave (or Leave of a never-joined GID): find the
		// config where the GID was actually removed.
		if _, exists := prev.Groups[op.GID]; !exists {
			for i := 1; i < len(*configs); i++ {
				_, was := (*configs)[i-1].Groups[op.GID]
				_, now := (*configs)[i].Groups[op.GID]
				if was && !now {
					return i
				}
			}
			return 0
		}

		nc := copyConfig(prev, prev.Num+1)
		delete(nc.Groups, op.GID)

		// If the last group left, revert every shard to the invalid
		// GID 0, matching the shape of config 0.
		if len(nc.Groups) == 0 {
			for s := 0; s < common.NShards; s++ {
				nc.Shards[s] = 0
			}
			*configs = append(*configs, nc)
			return nc.Num
		}

		// Reassign every shard whose current owner is no longer a valid
		// group. Recompute counts per shard so placements track with
		// the running tally.
		for s := 0; s < common.NShards; s++ {
			if _, ok := nc.Groups[nc.Shards[s]]; ok {
				continue
			}
			sc := shardCounts(nc)
			var minGID int64
			minCount := common.NShards + 1
			for _, gid := range sortedGIDs(nc.Groups) {
				if sc[gid] < minCount {
					minCount = sc[gid]
					minGID = gid
				}
			}
			nc.Shards[s] = minGID
		}
		*configs = append(*configs, nc)
		return nc.Num

	case OpMove:
		prev := (*configs)[len(*configs)-1]
		nc := copyConfig(prev, prev.Num+1)
		nc.Shards[op.Shard] = op.GID
		*configs = append(*configs, nc)
		return nc.Num
	}
	return 0
}

// Execute operation encoded in decided value v and update local state
func (sm *ShardMaster) ApplyOp(v interface{}) {
	op := v.(Op)
	ack := make(chan struct{})
	select {
	case <-sm.term:
		return
	case sm.impl.reqCh <- applyMsg{op: op, ack: ack}:
	}
	select {
	case <-sm.term:
	case <-ack:
	}
}

// opConfigFor looks up which configNum this op produced.
func (sm *ShardMaster) opConfigFor(opID int64) int {
	ch := make(chan int, 1)
	select {
	case <-sm.term:
		return 0
	case sm.impl.reqCh <- opConfigMsg{opID: opID, reply: ch}:
	}
	select {
	case <-sm.term:
		return 0
	case n := <-ch:
		return n
	}
}

// getConfig reads a single config snapshot from stateLoop. Configs are
// append-only so reading configs[n-1] and configs[n] separately is safe.
func (sm *ShardMaster) getConfig(num int) Config {
	ch := make(chan Config, 1)
	select {
	case <-sm.term:
		return Config{}
	case sm.impl.reqCh <- queryMsg{num: num, reply: ch}:
	}
	select {
	case <-sm.term:
		return Config{}
	case c := <-ch:
		return c
	}
}

// dispatchFor sends AssignShard RPCs for each shard whose owner changed
// between configs[configNum-1] and configs[configNum]. Shards are
// dispatched strictly one at a time per the spec.
func (sm *ShardMaster) dispatchFor(configNum int) {
	if configNum <= 0 {
		return
	}
	prev := sm.getConfig(configNum - 1)
	latest := sm.getConfig(configNum)

	for s := 0; s < common.NShards; s++ {
		newGID := latest.Shards[s]
		oldGID := prev.Shards[s]
		if newGID == oldGID || newGID == 0 {
			continue
		}
		newOwners := latest.Groups[newGID]
		if len(newOwners) == 0 {
			continue
		}
		var prevOwners []string
		if oldGID != 0 {
			prevOwners = prev.Groups[oldGID]
		}
		args := &common.AssignArgs{
			ConfigNum: latest.Num,
			Shard:     s,
			Servers:   prevOwners,
		}
		for !sm.isdead() {
			for _, srv := range newOwners {
				var reply common.AssignReply
				if common.Call(srv, "ShardKV.AssignShard", args, &reply) {
					goto next
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return
	next:
	}
}

// handle runs the common path for Join/Leave/Move: commit the op via
// Paxos, look up which configNum it effected, then dispatch that
// specific transition. By dispatching for *this* op's config (not
// blindly "latest - 1 → latest"), concurrent handlers can't clobber
// each other and skip transitions.
func (sm *ShardMaster) handle(op Op) {
	sm.rsm.AddOp(op)
	sm.dispatchFor(sm.opConfigFor(op.ID))
}

// RPC handlers for Join, Leave, Move, and Query RPCs
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.handle(Op{ID: common.Nrand(), Kind: OpJoin, GID: args.GID, Servers: args.Servers})
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.handle(Op{ID: common.Nrand(), Kind: OpLeave, GID: args.GID})
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.handle(Op{ID: common.Nrand(), Kind: OpMove, Shard: args.Shard, GID: args.GID})
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.rsm.AddOp(Op{ID: common.Nrand(), Kind: OpQuery, QueryNum: args.Num})
	reply.Config = sm.getConfig(args.Num)
	return nil
}
