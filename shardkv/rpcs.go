package shardkv

const (
	OK            = "OK"
	ErrWrongGroup = "ErrWrongGroup" // Key not in a shard owned by this group
	ErrNoKey      = "ErrNoKey"      // Key would be owned, but does not exist
	ErrOld        = "OldRequest"    // Out-of-date shard pull request
)

type Err string

// Client requesting a key modification
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string  // "Put" or "Append"
	ClientID int64   // Unique client identifier
	OpID     int64   // Serial # of this operation
}

type PutAppendReply struct {
	Err Err
}

// Client asking for the value of a key
type GetArgs struct {
	Key      string
	ClientID int64   // Unique client identifier
	OpID     int64   // Serial # of this operation
}

type GetReply struct {
	Err   Err
	Value string
}

// One shardkv server pulling a shard from another
type PullArgs struct {
	ConfigNum int  // Which configuration number is this?
	Shard     int  // Shard number to pull
}

type PullReply struct {
	Err		  Err
	KVStore   map[string]string // key -> value map
	OpIDCache map[int64]int64   // cache of completed operations
}
