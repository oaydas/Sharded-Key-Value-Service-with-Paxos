package paxosrsm

import (
	"time"

	"umich.edu/eecs491/proj4/paxos"
)

// IMPORTANT: Need to add additions for p4

type addOpReq struct {
	v     interface{}
	reply chan struct{}
}

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	reqCh chan interface{}
	done  chan struct{}
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.reqCh = make(chan interface{})
	rsm.impl.done = make(chan struct{})
	go rsm.rsmLoop()
	go rsm.deathWatcher()
}

// deathWatcher polls Status(-1) to detect when paxos has been killed.
// Seq -1 is always below minDone (which is >= 0), so a live paxos
// returns Forgotten. A dead paxos short-circuits on the closed term
// channel and returns Pending — impossible for seq -1 otherwise.
func (rsm *PaxosRSM) deathWatcher() {
	for {
		fate, _ := rsm.px.Status(-1)
		if fate == paxos.Pending {
			close(rsm.impl.done)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// rsmLoop is the single goroutine that owns appliedSeq.
// It serialises all AddOp work so ops are applied in order.
func (rsm *PaxosRSM) rsmLoop() {
	appliedSeq := -1

	for {
		var r addOpReq
		select {
		case <-rsm.impl.done:
			return
		case raw := <-rsm.impl.reqCh:
			r = raw.(addOpReq)
		}

		for {
			seq := rsm.px.Max() + 1
			if seq <= appliedSeq {
				seq = appliedSeq + 1
			}

			rsm.px.Start(seq, r.v)

			var decidedAtTarget interface{}
			for s := appliedSeq + 1; s <= seq; s++ {
				status, dv := rsm.px.Status(s)
				if status != paxos.Decided {
					rsm.px.Start(s, r.v)
					var ok bool
					dv, ok = rsm.waitForDecision(s)
					if !ok {
						return
					}
				}
				rsm.applyOp(dv)
				appliedSeq = s
				rsm.px.Done(s)
				if s == seq {
					decidedAtTarget = dv
				}
			}

			if rsm.equals(decidedAtTarget, r.v) {
				break
			}
		}

		rsm.px.Min()
		r.reply <- struct{}{}
	}
}

// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
func (rsm *PaxosRSM) AddOp(v interface{}) {
	ch := make(chan struct{}, 1)
	select {
	case <-rsm.impl.done:
		return
	case rsm.impl.reqCh <- addOpReq{v: v, reply: ch}:
	}
	select {
	case <-rsm.impl.done:
		return
	case <-ch:
	}
}

func (rsm *PaxosRSM) waitForDecision(seq int) (interface{}, bool) {
	to := 10 * time.Millisecond
	for {
		status, v := rsm.px.Status(seq)
		if status == paxos.Decided {
			return v, true
		}
		select {
		case <-rsm.impl.done:
			return nil, false
		case <-time.After(to):
		}
		if to < 500*time.Millisecond {
			to *= 2
		}
	}
}
