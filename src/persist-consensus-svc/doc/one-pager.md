# Group Commit Consensus for Persist

## The Problem

Persist's consensus layer is backed by Postgres (or CockroachDB). Every persist shard performs a compare-and-set (CAS)
against Postgres at the source tick rate. Today at ~1 tick/s with 8,000 shards, that's 8,000 writes/s to a SQL
database — scaling linearly with shard count. Pushing tick rates faster (100ms, 10ms) would multiply this by 10-100x.
The SQL database becomes the bottleneck, limiting both how many objects we can maintain and how fresh we can make them.

```
  Today: O(shards) writes to Postgres per tick

  ┌──────────────┐
  │ environmentd │
  │              │─── CAS shard_0 ──┐
  │   persist    │                  │     ┌──────────┐
  │              │─── CAS shard_1 ──┼────▶│ Postgres │  8,000 writes/s
  │              │─── CAS shard_2 ──┤     │  / CRDB  │  at 1s tick rate
  │              │       ...        │     └──────────┘
  │              │─── CAS shard_N ──┘
  └──────────────┘
```

## The Approach

By moving consensus into a service we own, we can group commit — batching independent CAS operations across shards
into a single durable write. Instead of N shards making N independent writes to a SQL database, the service collects
them all and flushes once per batch window.

We back the service with object storage (S3 Express One Zone), which gives us two things: durable storage for the
write-ahead log, and distributed compare-and-set via conditional PUTs (`If-None-Match: *`). This means the service
doesn't need its own consensus protocol.

Because the consensus data per shard is so small, the entire working set for an environment can easily fit in memory.
This allows the service to serve all reads operations (`head`, `scan`, `list_keys` in the Consensus API) directly from
RAM.

```
  Group Commit: O(1/batch_window) writes to S3

  ┌──────────────┐        ┌────────────────────────────────┐
  │ environmentd │        │     persist-consensus-svc      │
  │              │──┐     │                                │
  │   persist    │  │     │  ┌──────────────────────────┐  │     ┌─────────┐
  │              │──┼─────┼─▶│ Actor (single-threaded)  │──┼────▶│   S3    │
  │              │──┤gRPC │  │                          │  │     │ Express │
  │              │  │     │  │  Batch all CAS ops       │  │     │One Zone │
  │              │──┘     │  │  Flush every 20ms        │  │     └─────────┘
  └──────────────┘        │  └──────────────────────────┘  │     ~50 PUTs/s
                          └────────────────────────────────┘    regardless of
                                                                shard count
```

## What We Built

### 1. A Consensus Service (`persist-consensus-svc`)

A standalone gRPC service that implements persist's `Consensus` trait — the same `head`, `compare_and_set`, `scan`,
`truncate`, and `list_keys` interface that Postgres implements today.

Internally, it runs a single-threaded actor that:

- Accepts CAS operations from all shards via a channel
- Evaluates each CAS against in-memory committed state (first writer for a shard wins per batch, all others wait)
- On a 20ms flush timer, serializes the entire batch into a single protobuf WAL entry and writes it to S3 Express One
  Zone with a conditional PUT (`If-None-Match: *`)
- On S3 success, resolves all waiting callers: winners get `Committed`, losers get `ExpectationMismatch`
- All callers — winners and losers — experience the same latency, making the system predictable

Reads (`head`, `scan`, `list_keys`) are served immediately from in-memory state with no S3 round trip.

```
    Write Path (one flush cycle)

    gRPC handlers                    Actor                         S3
    ─────────────                    ─────                         ──
    CAS(shard_A, seq=5) ──┐
    CAS(shard_B, seq=3) ──┼──▶ mpsc ──▶ ┌──────────────────┐
    CAS(shard_A, seq=5) ──┤             │ Evaluate CAS:     │
    CAS(shard_C, seq=1) ──┘             │  shard_A: accept  │
         ▲                              │  shard_B: accept  │
         │                              │  shard_A: reject  │
         │                              │  shard_C: accept  │
         │                              └────────┬─────────┘
         │                                       │
         │                              ── 20ms tick ──
         │                                       │
         │                              ┌────────▼─────────┐
         │                              │ Serialize batch:  │
         │                              │  3 writes         │──▶ PUT wal/00042
         │                              │  (one per shard)  │    If-None-Match: *
         │                              └────────┬─────────┘
         │                                       │
         │                              ◀── S3 200 OK ───────────────────
         │                                       │
         │◀─────── resolve oneshots ─────────────┘
         │         shard_A caller 1: Committed
         │         shard_B caller:   Committed
         │         shard_A caller 2: ExpectationMismatch
         │         shard_C caller:   Committed
```

### 2. An RPC Consensus Client (`RpcConsensus`)

A new `Consensus` trait implementation that translates persist's consensus API into gRPC calls. From persist's
perspective, this is just another backend — selected by passing `--persist-consensus-url='rpc://host:port'` to
environmentd. No changes to persist's write/read paths, compaction, or any other machinery.

```
    Consensus Trait — pluggable backends

    ┌────────────────────────────────────────────────┐
    │              persist-client                     │
    │                                                │
    │   dyn Consensus ─┬─▶ PostgresConsensus (today) │
    │                  ├─▶ CockroachConsensus         │
    │                  ├─▶ MemConsensus (tests)       │
    │                  └─▶ RpcConsensus (NEW)         │
    │                        │                       │
    └────────────────────────┼───────────────────────┘
                             │ gRPC
                             ▼
                      persist-consensus-svc
```

## How Durability Works

At its core, what we've built is a log on object storage. The service appends batched entries to a write-ahead log
stored as sequentially-numbered S3 objects, with periodic snapshots for fast recovery. S3 Express One Zone (directory
buckets) is the sole durable store. There are two object types:

**WAL entries** (`wal/000000000001`, `wal/000000000002`, ...): Written every flush. Each is a protobuf containing all
CAS writes and truncates in that batch. The conditional PUT (`If-None-Match: *`) guarantees exactly-once writes, as we
can definitively know whether the request succeeded (HTTP 200) or already was written (HTTP 412). These translate into
the existing `persist` Consensus return codes.

**Snapshots** (`snapshot`): Written every N WAL entries (default 100). A full serialization of all shard state. By
performing snapshots at a fixed interval, we can predictably bound recovery time.

**Recovery** avoids LIST operations, as directory buckets return unordered results. Instead: we load the snapshot, then
linearly probe WAL entries starting from snapshot+1 until a 404 signals the end.

```
    S3 Bucket Layout

    consensus/
    ├── snapshot                    ◀── full state through batch 400
    ├── wal/000000000398
    ├── wal/000000000399
    ├── wal/000000000400            ◀── snapshot covers through here
    ├── wal/000000000401            ◀── recovery replays from here
    ├── wal/000000000402
    └── wal/000000000403            ◀── latest batch


    Recovery Sequence

    1. GET snapshot ──────────▶ 200: state through batch 400
    2. GET wal/000000000401 ──▶ 200: replay
    3. GET wal/000000000402 ──▶ 200: replay
    4. GET wal/000000000403 ──▶ 200: replay
    5. GET wal/000000000404 ──▶ 404: done, resume serving


    Ambiguous Write Handling

    PUT wal/42 ──▶ timeout ──▶ retry PUT wal/42 (same If-None-Match: *)
                                 │
                                 ├── 200: original failed, retry won
                                 └── 412: original landed, we're good
                                          (exactly one object either way)
```

## The Cost Story

| Tick Rate | Postgres Writes/s | S3 PUTs/s (Group Commit) |
|-----------|-------------------|--------------------------|
| 1s        | 8,000             | ~50                      |
| 100ms     | 80,000            | ~50                      |

S3 Express One Zone: single-digit millisecond PUT latency, ~$0.0025 per 1,000 PUTs. At 50 PUTs/s, that's ~$11/month for
consensus — down from a dedicated Postgres/CRDB instance.

```
    Writes/s vs. Shard Count

    Postgres (1s tick)          Group Commit (20ms flush)
    ──────────────────          ─────────────────────────
     8000 │         ╱            8000 │
          │       ╱                   │
          │     ╱                     │
          │   ╱                       │
          │ ╱                         │
       50 │╱                       50 │━━━━━━━━━━━━━━━━━━
          └──────────────             └──────────────
          0    4k    8k shards        0    4k    8k shards
```

## What Changes in Materialize

The changes to Materialize itself are minimal and non-invasive:

- **New crate**: `src/persist-consensus-svc` — the service binary (~1,200 lines)
- **New proto**: `consensus_service.proto` — gRPC service definition + WAL format
- **New file**: `src/persist/src/rpc.rs` — `RpcConsensus` client (~150 lines)
- **Modified**: `src/persist/src/cfg.rs` — route `rpc://` URLs to `RpcConsensus`
- **No changes** to persist-client internals, compaction, blob storage, or any read/write paths

The service is deployment-agnostic: it can run as a standalone process or be embedded within environmentd. Clients only
need `rpc://host:port`.

## Key Design Properties

- **Isolated by S3 prefix**: Each service instance writes to its own prefix — e.g. `consensus/<env-id>/<cluster-id>/`.
  You run one consensus service per cluster, and clusters share nothing. This is a natural fit for per-cluster scaling
  and multi-tenancy.
- **Single-threaded actor**: No locks, no races. The simplest possible concurrency model for a correctness-critical
  component.
- **S3 conditional writes provide fencing**: `If-None-Match: *` guarantees exactly-once WAL entries without implementing
  our own consensus protocol. This also enables a future path to multi-instance (active-standby) if needed.
- **Opaque data**: The service stores `(shard_id, seqno, bytes)` tuples and never decodes the bytes. Same contract as
  Postgres today.
- **Infinite retry on S3 failure**: WAL writes retry indefinitely with exponential backoff. Only `Ok` and
  `AlreadyExists` (412) are definite results — transient failures never propagate to clients.

## What We Did Not Build

This service does not change anything about `persist` semantics or how information flows through Materialize. Despite
having a "group commit" stage, this work does not bring us any closer to introducing something like atomic cross-shard
writes, which require a higher-level reimagining of coordination within Materialize.

This is purely a swap of the consensus implementation: from Postgres/CockroachDB to an in-memory serving layer backed
by an object storage WAL.

## Wait a second...

_Maintaining an object storage-backed log sounds a lot like what persist does already. Did you just rewrite persist?_

This is not untrue. You could very likely replace the backend of `persist-consensus-svc` with a `persist` shard, itself
backed by Cockroach/Postgres. It's turtles all the way down.