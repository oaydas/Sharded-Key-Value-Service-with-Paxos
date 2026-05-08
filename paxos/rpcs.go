package paxos

// In all data types that represent RPC arguments/reply, field names 
// must start with capital letters, otherwise RPC will break.

type Response string

const (
	OK     Response  = "OK"
	Reject Response  = "Reject"
)

// Prepare
//
// Proposer sends prepare message with a unique ballot number Peer
// responds either "Reject" (if already promised to a higher balot) or
// "OK"; if the latter, also supplies that peer's current highest
// ballot promised, and ballot #/value accepted so far.

type PrepareArgs struct {
	Seq    int //instance number
	N      int //ballot number
	PeerID int //index in peers of the peer sending the message
	Done   int //highest instance number that this peer is done with
}

type PrepareReply struct {
	Reply  Response
	N_p    int         //highest ballot number promised
	N_a    int         //highest ballot number accepted
	V_a    interface{} //value accepted, if any
	PeerID int         //index in peers of the peer sending the message
	Done   int         //highest instance number that this peer is done with
}

// Accept
//
// After a successful Prepare, proposer proposes the "best" V_a as
// this ballot's value. Each peer either Rejects it (if they have
// promised a higher ballot number) or OK confirming the accepted
// value

type AcceptArgs struct {
	Seq    int         //instance number
	N      int         //ballot number
	Value  interface{} //value being proposed
	PeerID int         //index in peers of the peer sending the message
	Done   int         //highest instance number that this peer is done with
}

type AcceptReply struct {
	Reply  Response
	N_p    int         //highest ballot number promised
	N_a    int         //highest ballot number accepted
	V_a    interface{} //value accepted, if any
	PeerID int         //index in peers of the peer sending the message
	Done   int         //highest instance number that this peer is done with
}

// Inform (that an instance is Decided)
//
// This is an optimization message. If a peer learns
// that a value is decided, it notifies the other peers
// They acknowledge this (but the ack is not explicitly
// necessary)

type InformArgs struct {
	Seq    int         //instance number
	Value  interface{} //value that has been decided
	PeerID int         //index in peers of the peer sending the message
	Done   int         //highest instance number that this peer is done with
}

type InformReply struct {
	Reply  Response
	PeerID int   //index in peers of the peer sending the message
	Done   int   //highest instance number that this peer is done with
}

