# Sharded Fault-Tolerant Key/Value Store

A distributed key/value storage system that partitions ("shards") keys across multiple replica groups, with each replica group internally replicated using Paxos for fault tolerance. A central, also fault-tolerant, **shard master** coordinates which replica group owns which shard and orchestrates reconfiguration as groups join and leave.

This architecture is patterned at a high level on production systems such as Flat Datacenter Storage, BigTable, Spanner, FAWN, Apache HBase, and Rosebud, distilled down to its essential moving parts: a configuration service plus a set of replica groups.

## Architecture

```
                    ┌───────────────────────────┐
                    │       Shard Master        │
                    │  (Paxos-replicated)       │
                    │                           │
                    │  • Owns configuration log │
                    │  • Join / Leave / Move    │
                    │  • Query                  │
                    └────────────┬──────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              ▼                  ▼                  ▼
       ┌────────────┐     ┌────────────┐     ┌────────────┐
       │  Group G1  │     │  Group G2  │     │  Group G3  │
       │  (Paxos)   │     │  (Paxos)   │     │  (Paxos)   │
       │  shards    │     │  shards    │     │  shards    │
       │  {0,1,2}   │     │  {3,4,5}   │     │  {6,7,8,9} │
       └────────────┘     └────────────┘     └────────────┘
              ▲                  ▲                  ▲
              └──────────────────┼──────────────────┘
                                 │
                            ┌─────────┐
                            │ Clients │
                            └─────────┘
```

- **Shard Master** — a small, Paxos-replicated service that maintains the authoritative sequence of numbered configurations. Each configuration maps every shard to exactly one replica group.
- **Replica Groups** — each group is a set of servers running Paxos. A group is responsible for some subset of the shards and serves `Get`/`Put`/`Append` for the keys in those shards.
- **Clients** — consult the shard master to discover the current configuration, then route each request to the replica group that owns the key's shard.

Keys are mapped to shards by a fixed hash (`Key2Shard` in `common/utils.go`). There are deliberately many more shards than groups, so load can be redistributed at a fine granularity.

## Repository Layout

```
src/
├── common/         # Shared types and utilities (Key2Shard, RPC arg/reply types)
├── paxos/          # Paxos consensus library
│   └── paxos_impl.go
├── paxosrsm/       # Replicated state machine built on Paxos
│   └── server_impl.go
├── shardmaster/    # The configuration service (Part A)
│   ├── types.go
│   └── server_impl.go
└── shardkv/        # Sharded key/value servers (Part B)
    └── server_impl.go
```

Only the `*_impl.go` files are intended to be modified.

## Components

### Paxos and PaxosRSM

`paxos` provides single-decree Paxos over a log of slots. `paxosrsm` wraps it into a replicated state machine: callers submit operations via `AddOp`, the library drives them through the log, and decided operations are applied in order through an application-supplied `applyOp` callback.

To support this project, `paxosrsm` is initialized with **two** callbacks:

- `applyOp(op)` — invoked once per decided log entry, in log order.
- `equals(a, b)` — invoked to compare two log entries for equality. This is needed inside `AddOp` to detect when a slot Claude/the proposer was racing for was already decided with the *same* operation it was trying to propose (avoiding double application). It exists because the operations stored in the log are not natively comparable with `==` (they typically contain maps or slices).

### Shard Master (`shardmaster`)

The shard master maintains an append-only sequence of numbered configurations. Configuration 0 is the initial state: no replica groups exist, and all shards are assigned to the sentinel group `GID 0`. Each subsequent configuration is produced in response to an administrative RPC.

#### RPCs

| RPC      | Arguments                              | Effect |
|----------|----------------------------------------|--------|
| `Join`   | `GID` (non-zero, not currently present), `[]server` | New configuration adds the group, rebalances shards as evenly as possible while moving as few shards as possible. |
| `Leave`  | `GID` (currently present)              | New configuration removes the group; its shards are redistributed across the survivors, again moving as few as possible. |
| `Move`   | `shard`, `GID`                         | New configuration in which the specified shard is assigned to the specified group. Primarily a testing/tuning hook. |
| `Query`  | configuration number (or `-1`)         | Returns that configuration. `-1` (or any number larger than the latest) returns the latest known configuration and reflects every prior completed `Join`/`Leave`/`Move`. |

A group is allowed to `Join`, `Leave`, and `Join` again — these are not duplicate requests.

#### Reconfiguration Protocol

When a `Join`, `Leave`, or `Move` produces a new configuration in which shard `S` moves from group `Gold` to group `Gnew`, the shard master:

1. Sends a `ShardKV.AssignShard` RPC to `Gnew`, telling it that it is now responsible for `S`.
2. `Gnew` responds to that assignment by issuing a `ShardKV.PullShard` RPC against `Gold` to fetch the shard's key/value contents (and the relevant duplicate-detection state).
3. Once the assignment completes, the shard master proceeds to the next shard.

Shard movements within a single configuration change are **serialized** — one shard at a time. The `Join`/`Leave`/`Move` RPC does not return to the caller until the reconfiguration it triggered is fully complete, so once the call returns, every group is ready to serve requests on its new shards.

By design, the only state the shard master maintains is the configuration log itself — duplicate request detection for client-facing shard master RPCs is not implemented (a production system would need this; here, repeated `Join`/`Leave`/`Move` from network retransmission can produce extra log entries, which is acceptable for this design).

### Sharded Key/Value Server (`shardkv`)

Each shardkv server is a member of exactly one replica group. Within a group, servers run Paxos via `paxosrsm` and apply decided operations to local state in log order. The log contains **two** kinds of entries:

- **Client operations** — `Get`, `Put`, `Append`.
- **Shard movements** — `AssignShard` and the completion of a `PullShard`.

Logging shard movements as Paxos entries is what makes correct linearizable behavior possible during reconfiguration: every replica in a group sees the same interleaving of client ops and shard ownership changes.

#### Client interface

Clients use `Clerk.Get(key)`, `Clerk.Put(key, value)`, and `Clerk.Append(key, value)`. The store provides **single-copy / linearizable semantics**: a `Get` always returns the value written by the most recent completed `Put`/`Append` on the same key. The Clerk is responsible for:

- Looking up the current configuration from the shard master (caching it, refreshing on error).
- Hashing the key to a shard, looking up the owning group, and sending the request to a member of that group.
- Retrying on `ErrWrongGroup` (after refreshing its configuration) and on network failure (against the next server in the group).

Each request carries a stable client-supplied request ID, which is **preserved across retries** so the server can deduplicate.

#### Server-side correctness rules

- A server replies `ErrWrongGroup` when it receives a request for a key whose shard the server does not currently own. `ErrWrongGroup` responses are **not** cached in the duplicate-detection table — the client will legitimately retry the same request ID against a different group.
- At any moment, **at most one** replica group considers itself the owner of any given shard. The Paxos-logged handoff ensures there is no window in which both `Gold` and `Gnew` will serve the same shard: `Gold` stops serving shard `S` before `Gnew` starts, and both transitions are recorded in their respective Paxos logs.
- Duplicate-detection state travels **with** the shard during `PullShard`. When `Gnew` ingests a shard from `Gold`, it merges in the request-ID cache entries belonging to keys in that shard — replacing wholesale would discard unrelated state for shards `Gnew` already owns.
- Duplicate-detection state is freed as request IDs become old enough that the client could not legitimately retry them.
- Old shards that a server no longer owns are left in place (not deleted) after a configuration change. This is a deliberate simplification.

#### Liveness assumptions

The implementation is correct as long as, in each replica group, a majority of servers are alive and can communicate with each other, with a majority of the shard master, and with a majority of every other replica group. Minority partitions, slow servers, and transient outages within a group are all tolerated.

## RPC Surface

The wire types are defined in:

- `common/types.go` — types shared by shard master and shardkv (notably `AssignShardArgs`/`Reply`, configuration structs).
- `shardmaster/types.go` — `Join`, `Leave`, `Move`, `Query` argument and reply types.

The set of RPCs is fixed: no new RPCs are added and no argument or reply types are modified.

## Concurrency

All shared server state is protected by Go-native synchronization. Synchronization is restricted to **channels and `sync.WaitGroup`** — no `sync.Mutex`, no atomics. Goroutines coordinate through channels, and any long-running background work (e.g., a server's configuration-poll loop, or Paxos's per-instance proposer goroutines) is shut down cleanly on server exit.

## Building and Testing

The project uses Go modules. From the repository root:

```bash
# Build everything
go build ./...

# Run a single package's tests
cd src/shardmaster && go test
cd src/shardkv     && go test

# Race detector — recommended; some bugs only show up under -race
go test -race ./...

# Run a specific test
go test -run TestBasic
go test -run TestConcurrent -race

# Verbose output
go test -v
```

Several tests are nondeterministic (they rely on timing, message reordering, and simulated partitions). It is worth running the suite multiple times — passing once is not sufficient evidence of correctness.

## Implementation Notes

A few details that are easy to get wrong and worth keeping in mind:

- **Maps are reference types in Go.** When deriving a new `Config` from a previous one, allocate a fresh map with `make` and copy entries individually. Use `maps.Equal` to compare maps for value-equality; `==` will not do what you want. The `equals` callback you hand to `paxosrsm` must do deep comparison on any map- or slice-valued fields of an operation.
- **Self-sufficient log entries.** Every Paxos log entry must contain everything a replica needs to apply it. A replica that wakes up and discovers a decided entry it didn't propose must not need to issue an RPC to figure out what the entry means. In particular, a `PullShard`-completion entry must carry the shard data inline, not a pointer to "go ask the other group."
- **Order matters during handoff.** Think carefully about the sequence: `Gold` logs "stop serving S" → `Gnew` pulls S → `Gnew` logs "start serving S". Any other order opens a window where neither group, or both groups, will respond.
- **`ErrWrongGroup` and dedup.** Returning `ErrWrongGroup` must not pollute the dedup cache, because the client will retry the same request ID elsewhere and that elsewhere must apply it.
- **First configuration.** Configuration 0 has no groups and assigns every shard to GID 0. The first `Join` produces configuration 1.

## Limitations

This is intentionally a minimal design and is not production-grade:

- No persistent storage — all state (key/values and Paxos logs) lives in memory.
- More messages per Paxos agreement than strictly necessary.
- The set of peers in a Paxos group is fixed at startup; there is no membership change protocol for the Paxos layer itself.
- Shard handoff is serialized and blocks concurrent client access to the shard being moved.
- The data model is flat key/value with string values — no ranges, transactions, secondary indexes, or schemas.
