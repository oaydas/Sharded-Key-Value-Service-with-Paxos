package common

//
// RPC types needed by both shardmaster and shardkv
// These have to be defined here to avoid circular dependencies
//


// 
// Assign a single shard to a shardkv group.
//
// Call this once for each shard you have to move; it is easiest
// to move one shard at a time.
//
// The shardkv server that serves this RPC will in turn call
// ShardKV.Pull from one of the servers in the group of Servers
//
type AssignArgs struct {
	ConfigNum int
	Shard     int
	Servers   []string
}

type AssignReply struct {
}
