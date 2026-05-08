package paxos

import (
	"math/rand"
	"time"

	"umich.edu/eecs491/proj4/common"
)

// Per-instance Paxos state (acceptor + decision state).
type instance struct {
	n_p   int         // highest ballot promised
	n_a   int         // highest ballot accepted
	v_a   interface{} // value of highest accepted ballot
	fate  Fate
	value interface{} // decided value (valid only when fate == Decided)
}

///////////////////////////////////////////////////////////////////////////
// Message types sent to the state-owning goroutine.
// Each request with a reply channel uses capacity 1 so the state
// goroutine never blocks if the requester has been terminated.
///////////////////////////////////////////////////////////////////////////

type prepareReq struct {
	args  PrepareArgs
	reply chan PrepareReply
}

type acceptReq struct {
	args  AcceptArgs
	reply chan AcceptReply
}

type informReq struct {
	args  InformArgs
	reply chan InformReply
}

type statusReq struct {
	seq   int
	reply chan statusResp
}
type statusResp struct {
	fate  Fate
	value interface{}
}

type doneReq struct{ seq int }

type maxReq struct{ reply chan int }

type minReq struct{ reply chan int }

type getDoneReq struct{ reply chan int }

type updateDoneReq struct {
	peerID int
	done   int
}

type startCheckReq struct {
	seq   int
	reply chan bool // true → ok to start proposer
}

type proposerCheckReq struct {
	seq   int
	reply chan proposerCheckResp
}
type proposerCheckResp struct {
	stop    bool
	decided bool
	value   interface{}
}

// additions to Paxos state.
type PaxosImpl struct {
	reqCh chan interface{} // all requests to the state goroutine
}

// your px.impl.* initializations here.
func (px *Paxos) initImpl() {
	px.impl.reqCh = make(chan interface{})
	go px.stateLoop()
}

// stateLoop is the single goroutine that owns all mutable Paxos state
// via lexical confinement. instances, doneVals, and maxSeq are local
// variables here and are never shared with any other goroutine.
// Every access goes through the reqCh message channel.
func (px *Paxos) stateLoop() {
	instances := make(map[int]*instance)
	doneVals := make([]int, len(px.peers))
	for i := range doneVals {
		doneVals[i] = -1
	}
	maxSeq := -1

	// Confined helpers — closures over local state.

	// return instance for given sequence number
	getInstance := func(seq int) *instance {
		if seq > maxSeq {
			maxSeq = seq
		}
		inst, ok := instances[seq]
		if !ok {
			inst = &instance{fate: Pending}
			instances[seq] = inst
		}
		return inst
	}

	// return the minimum done value among all peers
	minDone := func() int {
		m := doneVals[0]
		for _, d := range doneVals {
			if d < m {
				m = d
			}
		}
		return m + 1
	}

	// update the done value for a given peer
	updateDone := func(peerID int, done int) {
		if done > doneVals[peerID] {
			doneVals[peerID] = done
		}
	}

	for {
		select {
		case <-px.term:
			return
		case raw := <-px.impl.reqCh:
			switch r := raw.(type) {

			case prepareReq:
				updateDone(r.args.PeerID, r.args.Done)
				inst := getInstance(r.args.Seq)
				reply := PrepareReply{PeerID: px.me, Done: doneVals[px.me]}
				if inst.fate == Decided {
					reply.Reply = Reject
					reply.N_p = -1
					reply.V_a = inst.value
				} else if r.args.N > inst.n_p {
					inst.n_p = r.args.N
					reply.Reply = OK
					reply.N_a = inst.n_a
					reply.V_a = inst.v_a
					reply.N_p = inst.n_p
				} else {
					reply.Reply = Reject
					reply.N_p = inst.n_p
				}
				r.reply <- reply

			case acceptReq:
				updateDone(r.args.PeerID, r.args.Done)
				inst := getInstance(r.args.Seq)
				reply := AcceptReply{PeerID: px.me, Done: doneVals[px.me]}
				if r.args.N >= inst.n_p {
					inst.n_p = r.args.N
					inst.n_a = r.args.N
					inst.v_a = r.args.Value
					reply.Reply = OK
				} else {
					reply.Reply = Reject
				}
				reply.N_p = inst.n_p
				r.reply <- reply

			case informReq:
				updateDone(r.args.PeerID, r.args.Done)
				inst := getInstance(r.args.Seq)
				inst.fate = Decided
				inst.value = r.args.Value
				r.reply <- InformReply{Reply: OK, PeerID: px.me, Done: doneVals[px.me]}

			case statusReq:
				if r.seq < minDone() {
					r.reply <- statusResp{Forgotten, nil}
				} else if inst, ok := instances[r.seq]; ok && inst.fate == Decided {
					r.reply <- statusResp{Decided, inst.value}
				} else {
					r.reply <- statusResp{Pending, nil}
				}

			case doneReq:
				if r.seq > doneVals[px.me] {
					doneVals[px.me] = r.seq
				}

			case maxReq:
				r.reply <- maxSeq

			case minReq:
				mv := minDone()
				for seq := range instances {
					if seq < mv {
						delete(instances, seq)
					}
				}
				r.reply <- mv

			case getDoneReq:
				r.reply <- doneVals[px.me]

			case updateDoneReq:
				updateDone(r.peerID, r.done)

			case startCheckReq:
				if r.seq < minDone() {
					r.reply <- false
				} else {
					if r.seq > maxSeq {
						maxSeq = r.seq
					}
					r.reply <- true
				}

			case proposerCheckReq:
				if r.seq < minDone() {
					r.reply <- proposerCheckResp{stop: true}
				} else {
					inst := getInstance(r.seq)
					if inst.fate == Decided {
						r.reply <- proposerCheckResp{stop: true, decided: true, value: inst.value}
					} else {
						r.reply <- proposerCheckResp{stop: false}
					}
				}
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////
// Thin wrappers that send a message to stateLoop and await a reply.
// Every send/recv is guarded by select on px.term so that a killed
// peer never blocks.
///////////////////////////////////////////////////////////////////////////

func (px *Paxos) sendRecvInt(req interface{}, ch chan int, fallback int) int {
	select {
	case <-px.term:
		return fallback
	case px.impl.reqCh <- req:
	}
	select {
	case <-px.term:
		return fallback
	case v := <-ch:
		return v
	}
}

func (px *Paxos) sendFire(req interface{}) {
	select {
	case <-px.term:
	case px.impl.reqCh <- req:
	}
}

///////////////////////////////////////////////////////////////////////////
// Public API
///////////////////////////////////////////////////////////////////////////

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	ch := make(chan bool, 1)
	select {
	case <-px.term:
		return
	case px.impl.reqCh <- startCheckReq{seq: seq, reply: ch}:
	}
	select {
	case <-px.term:
		return
	case ok := <-ch:
		if ok {
			go px.proposer(seq, v)
		}
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.sendFire(doneReq{seq: seq})
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	ch := make(chan int, 1)
	return px.sendRecvInt(maxReq{reply: ch}, ch, -1)
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
func (px *Paxos) Min() int {
	ch := make(chan int, 1)
	return px.sendRecvInt(minReq{reply: ch}, ch, 0)
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	ch := make(chan statusResp, 1)
	select {
	case <-px.term:
		return Pending, nil
	case px.impl.reqCh <- statusReq{seq: seq, reply: ch}:
	}
	select {
	case <-px.term:
		return Pending, nil
	case resp := <-ch:
		return resp.fate, resp.value
	}
}

///////////////////////////////////////////////////////////////////////////
// RPC handlers — forward to the state goroutine for acceptor logic
///////////////////////////////////////////////////////////////////////////

// Prepare (paxos phase one)
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	ch := make(chan PrepareReply, 1)
	select {
	case <-px.term:
		return nil
	case px.impl.reqCh <- prepareReq{args: *args, reply: ch}:
	}
	select {
	case <-px.term:
		return nil
	case r := <-ch:
		*reply = r
		return nil
	}
}

// Accept (paxos phase two)
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	ch := make(chan AcceptReply, 1)
	select {
	case <-px.term:
		return nil
	case px.impl.reqCh <- acceptReq{args: *args, reply: ch}:
	}
	select {
	case <-px.term:
		return nil
	case r := <-ch:
		*reply = r
		return nil
	}
}

// Inform (the Decided optimization)
func (px *Paxos) Inform(args *InformArgs, reply *InformReply) error {
	ch := make(chan InformReply, 1)
	select {
	case <-px.term:
		return nil
	case px.impl.reqCh <- informReq{args: *args, reply: ch}:
	}
	select {
	case <-px.term:
		return nil
	case r := <-ch:
		*reply = r
		return nil
	}
}

///////////////////////////////////////////////////////////////////////////
// Internal helpers used by the proposer goroutine
///////////////////////////////////////////////////////////////////////////

// getDone returns this peer's highest Done value.
func (px *Paxos) getDone() int {
	ch := make(chan int, 1)
	return px.sendRecvInt(getDoneReq{reply: ch}, ch, -1)
}

// updatePeerDone records a peer's Done value learned from an RPC reply.
func (px *Paxos) updatePeerDone(peerID int, done int) {
	px.sendFire(updateDoneReq{peerID: peerID, done: done})
}

// proposerCheck asks the state goroutine whether the proposer for seq
// should stop (instance decided or forgotten).
func (px *Paxos) proposerCheck(seq int) proposerCheckResp {
	ch := make(chan proposerCheckResp, 1)
	select {
	case <-px.term:
		return proposerCheckResp{stop: true}
	case px.impl.reqCh <- proposerCheckReq{seq: seq, reply: ch}:
	}
	select {
	case <-px.term:
		return proposerCheckResp{stop: true}
	case resp := <-ch:
		return resp
	}
}

// nextProposalNumber generates a ballot number unique to this peer
// and strictly greater than highestSeen, using n = round*numPeers + me.
func (px *Paxos) nextProposalNumber(highestSeen int) int {
	np := len(px.peers)
	return (highestSeen/np+1)*np + px.me
}

///////////////////////////////////////////////////////////////////////////
// Proposer
///////////////////////////////////////////////////////////////////////////

// proposer drives the Paxos protocol for a single instance.
// Runs in its own goroutine, spawned by Start().
func (px *Paxos) proposer(seq int, v interface{}) {
	highestSeen := 0

	for !px.isdead() {
		resp := px.proposerCheck(seq)
		if resp.stop {
			return
		}

		n := px.nextProposalNumber(highestSeen)
		highestSeen = n

		// ---- Phase 1: Prepare ----
		prepareOKs := 0
		highestNA := 0
		var highestVA interface{}
		alreadyDecided := false
		var decidedValue interface{}

		for i := 0; i < len(px.peers); i++ {
			if px.isdead() {
				return
			}
			args := &PrepareArgs{
				Seq: seq, N: n, PeerID: px.me, Done: px.getDone(),
			}
			reply := &PrepareReply{}

			ok := false
			if i == px.me {
				px.Prepare(args, reply)
				ok = true
			} else {
				ok = common.Call(px.peers[i], "Paxos.Prepare", args, reply)
			}
			if ok {
				if reply.N_p == -1 {
					alreadyDecided = true
					decidedValue = reply.V_a
				}
				if reply.Reply == OK {
					prepareOKs++
					if reply.N_a > highestNA {
						highestNA = reply.N_a
						highestVA = reply.V_a
					}
				}
				if reply.N_p > highestSeen {
					highestSeen = reply.N_p
				}
				px.updatePeerDone(reply.PeerID, reply.Done)
			}
		}

		if alreadyDecided {
			px.sendInforms(seq, decidedValue)
			return
		}

		if prepareOKs <= len(px.peers)/2 {
			time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
			continue
		}

		// Between phases: if another proposer decided, disseminate and stop.
		resp = px.proposerCheck(seq)
		if resp.stop {
			if resp.decided {
				px.sendInforms(seq, resp.value)
			}
			return
		}

		vPrime := v
		if highestNA > 0 {
			vPrime = highestVA
		}

		// ---- Phase 2: Accept ----
		acceptOKs := 0

		for i := 0; i < len(px.peers); i++ {
			if px.isdead() {
				return
			}
			args := &AcceptArgs{
				Seq: seq, N: n, Value: vPrime, PeerID: px.me, Done: px.getDone(),
			}
			reply := &AcceptReply{}

			ok := false
			if i == px.me {
				px.Accept(args, reply)
				ok = true
			} else {
				ok = common.Call(px.peers[i], "Paxos.Accept", args, reply)
			}
			if ok {
				if reply.Reply == OK {
					acceptOKs++
				}
				if reply.N_p > highestSeen {
					highestSeen = reply.N_p
				}
				px.updatePeerDone(reply.PeerID, reply.Done)
			}
		}

		if acceptOKs <= len(px.peers)/2 {
			time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
			continue
		}

		// ---- Phase 3: Inform — value is decided ----
		px.sendInforms(seq, vPrime)
		return
	}
}

// sendInforms broadcasts the decided value to all peers (local via
// channel, remote via RPC), piggybacking Done values.
func (px *Paxos) sendInforms(seq int, value interface{}) {
	for i := 0; i < len(px.peers); i++ {
		if px.isdead() {
			return
		}
		args := &InformArgs{
			Seq: seq, Value: value, PeerID: px.me, Done: px.getDone(),
		}
		reply := &InformReply{}

		if i == px.me {
			px.Inform(args, reply)
		} else {
			ok := common.Call(px.peers[i], "Paxos.Inform", args, reply)
			if ok {
				px.updatePeerDone(reply.PeerID, reply.Done)
			}
		}
	}
}
