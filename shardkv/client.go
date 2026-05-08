package shardkv

import (
	"sync"
	"time"

	"umich.edu/eecs491/proj4/common"
	"umich.edu/eecs491/proj4/shardmaster"
)

type Clerk struct {
	mu     sync.Mutex
	sm     *shardmaster.Clerk
	config shardmaster.Config
	clientID int64
	lastOpID int64
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	ck.config = ck.sm.Query(-1)
	ck.clientID = common.Nrand()
	ck.lastOpID = -1
	return ck
}

//
// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	shard := common.Key2Shard(key)
	ck.lastOpID++

	for {
		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				args := GetArgs{key, ck.clientID, ck.lastOpID}
				var reply GetReply
				ok := common.Call(srv, "ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//
// send a Put or Append request.
// keep retrying forever until success.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	shard := common.Key2Shard(key)
	ck.lastOpID++

	for {
		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				args := PutAppendArgs{key, value, op, ck.clientID, ck.lastOpID}
				var reply PutAppendReply
				ok := common.Call(srv, "ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
}
