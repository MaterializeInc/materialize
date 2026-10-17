Persist is responsible for durably storing partial time-varying collections (pTVC) in Materialize. This document defines the *contract* that persist offers to its callers and the contract it places on its dependencies.

Persist operates as a set of *shards* each representing a partial time-varying collection (pTVC). A pTVC represents a set of `((key, value), time, diff)` over a time range: `[since, upper)`[^frontiers]. A read at an `as_of` within the time range sees all changes at times up to and including `as_of`, while changes that occur before `since` may have their times advanced to `since`, so they can be consolidated (updates to the same `(key, value)` are merged by summing their `diff`s) to a single state of: `(key, value, diff)`.

To provide the persist contract persist needs somewhere to put bytes (*blob* store), and a place to agree on one ordering of writes (*consensus* log), which holds each shard's *state*: the metadata that points at its data in blob.

## Contract to Callers

Through the persist API, callers write to a shard, read from it, and supply the encoding of data. This section states what persist guarantees for each.

**P1. Append.** The only way to advance `upper` is to *append* to a shard via a `compare_and_append` operation, which appends a *batch* of updates and must be atomic, linearizable and idempotent. That is: only a single writer can advance `upper` from a given current `upper`, while losers see an error (`UpperMismatch`).

<details>
<summary>Implications and Details</summary>

- Atomicity comes from consensus. A batch's data is written to blob first then a single compare-and-set on the shard's state in consensus adds the whole batch.
- Idempotency holds within one call: if the consensus write's outcome is unknown, persist retries it with the same token, so the update lands at most once. A caller that retries its own `compare_and_append`, e.g. after a timeout, must make a new call with a new token. If the first attempt landed, the retry fails with `UpperMismatch`.
- On `UpperMismatch` the caller learns the current `upper` and decides whether to retry.
- An append covers the times `[upper, new_upper)`, where `upper` is the shard's current `upper` and `new_upper` is where the append advances it to. Every update in the batch must have a time in that range, which is checked as the batch is built, and returns `InvalidUsage` for an update outside the batch's bounds.
</details>

**P2. Definiteness.** The critical guarantee of Persist is that a shard is *definite*. That is, all reads at the same logical time (`as_of`) must see the *same* state as long as that time remains in the range of `[since, upper)`.

<details>
<summary>Implications and Details</summary>

- If two readers at the same time do not get identical data, then there would be non determinism in results in computation in Materialize.
- Definiteness holds because writes only add updates at or beyond `upper`: once a time is below `upper`, the updates at that time never change.
- If persist is not *durable* then definiteness breaks across a restart. That is, a reader after the restart could see a different state at the same `as_of` than a reader before it. Hence a `compare_and_append` only succeeds once its data is durably stored.
- Note: There is no requirement that `upper > since`, if `since >= upper` then there is no readable timestamp (a new shard starts with `since == upper`). Reading at or beyond `upper` waits until `upper` advances. Reading below `since` then fails with a `Since` error that names the earliest readable `as_of`.
</details>

**P3. Reads.** A reader can read the shard's contents at an `as_of` via `snapshot`, and all later updates via a `listen` call. However, the returned updates are not guaranteed to be consolidated, so a client must perform consolidation by itself as the same `(k,v,t)` may appear more than once[^snapshot-and-fetch].


**P4. Handles.** *Handles* are what ensure that persist does not advance state that clients need to consume. `SinceHandles` hold back the advancement of `since`: A `SinceHandle` is durable and survives process restarts. By contrast a `ReadHandle` holds back `since` and stops the data it is actively reading from being deleted[^layout]. Hence `since` is the minimum over all handles that hold it back.

<details>
<summary>Implications and Details</summary>

- Persist supports many `SinceHandle`s but in current practice almost everything funnels through a *single* `SinceHandle` owned by the storage controller.
- Persist provides a durable representation of pTVCs, but internally reorganizes the data (compaction and garbage collection[^gc]) to be more efficient. A `ReadHandle` is leased (15 minutes by default, configurable per deployment) and must heartbeat, which it does every quarter of the lease, otherwise another process's state update will expire it and it loses its hold. Reorganization still happens while a `ReadHandle` is active, but its lease keeps garbage collection from deleting the data it is reading. A `ReadHandle` is not durable.
- Note: a `WriteHandle` holds nothing back and grants no exclusive right to write: any number of writers may exist, and `compare_and_append` alone decides which one advances `upper`. A `WriteHandle` is also leased (1 hour) but has no heartbeat, however every append (including an empty one) renews it. Expiry of the lease is harmless as the writer's next append will re-register it.
- Holding a handle has cost as it holds back `since`. Holding back `since` stops compaction from consolidating past it, and a `ReadHandle` that is held stops garbage collection from deleting old data, so the shard grows without bound. A `ReadHandle` that misses its heartbeats loses its hold. By contrast, a `SinceHandle` never expires so its owner must keep advancing (known as downgrading) it or `since` never advances.
</details>

**P5. PubSub.** Persist provides a *PubSub* interface to subscribers of shards to receive changes without needing to poll persist, which is a latency optimization. Critically the PubSub delivery mechanism is **not** assumed to be reliable or ordered and a subscriber can always fetch latest state.
<details>
<summary>Implications and Details</summary>

- This is a two-sided mechanism: after every state change (e.g., a `compare_and_append`), persist pushes the change via `PubSubSender`, while readers consume via `PubSubReceiver`. A pushed change that follows directly on the reader's cached copy of the shard's state is applied to it. Otherwise it is dropped, and the reader catches up on its next poll of consensus.
- PubSub does not need ordering or reliable delivery because correctness comes from consensus: a dropped or late change only costs latency.
- Without PubSub, latency degrades to the polling interval.
</details>

**P6. Encoding.** Persist stores keys and values in an *encoding* defined by the caller. Keys (`K`) and values (`V`) implement `Codec`, while times (`T`) and diffs (`D`) implement `Codec64` (a fixed 8-byte encoding). The codecs of the first handle to open the shard are recorded in the shard's state, and a later handle with different codecs fails to open with `CodecMismatch`.

<details>
<summary>Implications and Details</summary>

- Persist consolidates on the stored encoding of `(k, v)`, not on decoded values. That encoding is not guaranteed to be one-to-one, so two updates that decode to the same `(k, v)` may not be merged. This is another reason a reader must consolidate by itself.
- Compaction may rewrite old data into the current format, but data that has not been compacted stays as it was written, so the `Codec` must be able to decode anything an older `encode` wrote.
- The shard also records a *schema* describing the structure of `K` and `V`, which can change after data exists via `compare_and_evolve_schema`, but only in backward-compatible ways (e.g., adding a nullable column at the end). Incompatible changes are refused, and a reader with an incompatible schema panics rather than return mis-shaped data.
- `CodecMismatch` is a programming error.
</details>

**P7. Versioning.** Persist state is tagged with a code *version*. A binary can read state tagged with its own or an older version, and halts on state tagged with a newer one. The tag moves forward only through an explicit `upgrade_version` call, which the storage controller makes when it opens a shard in read-write mode.

<details>
<summary>Implications and Details</summary>

- Versioning is what makes 0dt upgrades safe. A new binary in read-only mode reads the shard without moving the tag, so the old binary keeps running. Once the new binary opens the shard read-write, the tag moves to the new version, and an old binary that reads the shard halts.
</details>

**P8. Failures.** Persist retries every failure of blob or consensus internally, forever, so an outage of either makes persist calls *hang*. The errors a caller does see are about its own usage: `UpperMismatch`, `Since` and `InvalidUsage`. States persist cannot recover from halt the process instead, e.g., state from a newer version, or a blob that state references but blob storage reports missing.

### Non-goals

The following are not part of the contract. Callers that need them build them on top of persist or get them elsewhere.

- **Atomic writes across shards.** `compare_and_append` is atomic within one shard. `txn-wal` builds atomic writes across shards on top of persist.
- **Point lookups.** Reads are a `snapshot` and `listen` over a whole shard; there is no "get the value for key k"[^pruning].
- **Changing history.** Every write lands at or beyond `upper`. A caller can retract an earlier update by writing its negation at a new time, but cannot insert at an old time.
- **Backup and disaster recovery.** Durability comes from blob and consensus; persist has no backup or restore primitive of its own. `persistcli admin restore-blob` is a repair tool that can undelete blobs that consensus still references, using the blob store's versioning.

[^frontiers]: `upper` is the frontier before which all writes are complete; `since` is the frontier before which history may have been consolidated.

[^gc]: *Compaction* merges batches into fewer, consolidated ones; *garbage collection* deletes data that no state references any more.

[^snapshot-and-fetch]: `snapshot_and_fetch` is the exception: it returns a consolidated snapshot.

[^pruning]: A reader can skip parts using their stats. This is *pruning* not a lookup.

[^layout]: Compaction can write the data in a new, consolidated layout, but the old version stays in blob storage until no `ReadHandle` lease references it.

[^multipart]: Blobs over 8 MB are uploaded in 8 MB parts (`DEFAULT_MULTIPART_THRESHOLD`, `DEFAULT_MULTIPART_CHUNK_SIZE`).

[^retry-set]: A retried `set` may write the same bytes to the same key again, so the store must accept an identical overwrite.

[^linearizable]: It is interesting to ponder whether we could tolerate a non linearizable blob storage layer since blobs once written are immutable and only visible after they are referenced in consensus.

## Contract of Dependencies

### Blob

Blob storage is an object store where the actual shard data is stored and managed with a `set`, `get` and `delete` API. Persist requires the following of the blob storage layer.

**B1. Durability.** Writes to blob must be *durable*: once a `set` returns, the blob must survive process crashes, restarts and the loss of a node in the backing blob store.

<details>
<summary>Implications and Details</summary>

- Persist writes a blob before the consensus change that references it, so once `compare_and_append` commits, that data exists only in blob. If the blob is lost afterwards, readers find state pointing at a missing blob and the process halts.
- Persist keeps no second copy and does no repair, so durability is on the blob store (e.g., S3's replication).
</details>

**B2. Atomic writes.** Writes must be *atomic whole-blob writes*. Specifically persist `set`s each blob once using a writer-unique key and never updates it in place[^retry-set], and a `get` must see either the complete value or nothing.

<details>
<summary>Implications and Details</summary>

- Note: because of the order of operations of a write in persist (first to blob and then to consensus) consumers will not read writes to blob storage that error, as they are not made visible through the ordered log (consensus).
- If the blob storage exposes a partially-written blob or a truncated or mixed blob, subsequent reads would be corrupted.
</details>

**B3. Linearizable reads.** Reads must be *linearizable*[^linearizable], that is a `get` issued after a `set` of the same key completes must see that write and not report the key as missing.

<details>
<summary>Implications and Details</summary>

- If a `get` reports an existing blob as missing then a reader sees live data as missing, which halts the process or surfaces as a read error.
</details>

**B4. Multipart.** Large blobs[^multipart] are written into blob storage as a multipart upload, with the parts written in parallel. Multipart writes must also be atomic, that is the blob is not visible until the upload completes. Persist never aborts an incomplete upload, hence the bucket must reap incomplete uploads after some age (e.g., an S3 lifecycle rule).

<details>
<summary>Implications and Details</summary>

- An upload is complete once `complete_multipart_upload` succeeds, and `set` only returns after that. hence consensus only ever references a complete blob. if an upload fails or is interrupted then the parts already written are orphaned, and nothing in persist deletes them.
- Persist starts an upload with an explicit `content-length: 0` header, which gcs needs in its s3 interoperability mode. to read a multipart blob back, the store must either report the part count (`x-amz-mp-parts-count`), or report the blob's total size in `content-range` and support byte-range reads. if it does neither then `get` fails instead of returning a truncated blob, and persist retries it forever.
- If a store does not support multipart then it cannot store any blob over 8 MB. blobs routinely exceed that and the consequence would be write failure leading to ingestion and compaction stall.
- Note: persist targets blobs of at most 128 MB (`blob_target_size`), which is about 16 8MB parts.
</details>


**B5. Delete.** Persist deletes a blob via `delete` once no state references it (garbage collection). A `delete` must be idempotent, that is deleting a key that does not exist succeeds (and reports that nothing was deleted) instead of erroring.

<details>
<summary>Implications and Details</summary>

- More than one process may garbage collect the same blob, so a second `delete` of a key is expected.
- If `delete` errors on a missing key then garbage collection errors out and stalls, and the bucket grows without bound.
</details>

**B6. Listing.** The blob storage system must support a `list_keys` API that returns every key under a key prefix along with its size. Persist does not use this to decide what to delete, but it is needed for storage usage accounting and for admin tools (e.g., `persistcli inspect`).

<details>
<summary>Implications and Details</summary>

- If not all blobs can be listed at once, pagination must be supported (e.g., S3 returns at most 1,000 keys per request)
- If not all blobs are listed then storage usage is under-counted and leaked blobs are missed.

</details>

**B7. Versioning.** Object versioning is required to support restore of a blob through the `persistcli admin restore-blob` command.

<details>
<summary>Implications and Details</summary>

- Note: restore is *not required* to be supported, so we can accept blob storage that doesn't provide this.

</details>

**B8. S3 compatibility.** Persist connects to blob storage through the AWS S3 client (`aws-sdk-s3`), so the store must be *S3 compatible*. When it opens the blob store it checks the connection by issuing a `get` of a `HEALTH_CHECK` key. Hence a misconfigured endpoint, credentials or TLS setup fails when persist starts.

<details>
<summary>Implications and Details</summary>

- S3 compatible means the store implements `PutObject`, `GetObject` (including by `partNumber`), `HeadObject`, `DeleteObject`, `ListObjectsV2`, `CreateMultipartUpload`, `UploadPart` and `CompleteMultipartUpload`, and accepts AWS Signature Version 4 signed requests.
- If a custom endpoint is configured (i.e., any store that is not AWS S3) then persist uses *path-style* addressing, so the store must support it.
- The store's TLS certificate must validate against the system's root certificates, unless the endpoint is plain HTTP.
- Credentials and region come from the process environment, an assumed role, or an explicit access key.
- The client sends a CRC32 checksum header (`x-amz-checksum-crc32`) on every `PutObject` and `UploadPart`, so the store must accept it. Some older S3 compatible stores reject these headers.
- Each call has a connect timeout (7 seconds), a timeout to the first byte of the response (10 seconds), a per-attempt timeout (90 seconds) and an overall timeout (180 seconds), all configurable. A call that times out is an error, and persist retries it forever.
- Persist *hedges* slow reads: if a `get` has not completed after 2 seconds, persist issues a rate-limited second `get` of the same key and takes whichever succeeds first. The second `get` uses its own connection pool, so that a connection that died and stalled the first `get` does not stall the second. Hence the store must serve concurrent reads of the same key from separate connections.
</details>

#### Performance Expectations

Rough latency targets for `get` and `set`, based on production today: p50 = 40 ms, p99 = 200 ms.

Load is bursty as hydration can push read throughput to 10x their average or more.

#### Non-goals
- As blobs are never modified there is no need to support a read-modify-write, conditional-put or append operation; it is asked never to expose a partial blob.
- Keys are chosen when a blob is written and never change, so there is no need to support rename, copy or move.

### Consensus
Consensus is a single table in a SQL database where each shard's state is stored as an ordered log, and managed with a `head` (read the latest version), `compare_and_set`, `scan` and `truncate` API. Persist requires the following of the consensus layer.

```sql
CREATE TABLE IF NOT EXISTS consensus (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY(shard, sequence_number)
)
```

**C1. Durability.** Writes to consensus must be *durable*: once a `compare_and_set` commits, the new state must survive process crashes, restarts and the loss of a node in the backing database. Consensus stores each version of a shard's state tagged with a *sequence number* (`seqno`), which increases by one with every state change.

<details>
<summary>Implications and Details</summary>

- If a committed `compare_and_set` is lost, the shard rolls back to an older version. An `upper` that callers saw advance moves back, so later writes can land at times already read, which breaks definiteness (P2).
- The older version may also reference blobs that garbage collection has since deleted, so readers of the rolled-back state halt on a missing blob.
- Persist keeps no copy of consensus state elsewhere, so the consensus database must provide durability.
</details>

**C2. Linearizable compare-and-set.** To complete a `compare_and_append` operation after data has been written to blob storage, the persist API performs a compare and set into consensus: it writes the new state at `seqno + 1` only if the shard's latest `seqno` is still the one it read. This ensures exactly one writer can advance a shard's state from a given `seqno`. To do so, consensus must support a linearizable compare-and-set operation: for each shard key, `compare_and_set(new)` must commit if and only if `new.seqno == head.seqno + 1`.

<details>
<summary>Implications and Details</summary>

- Under the covers this requires serializable for CRDB and read committed for Postgres support.
- Implementing with read committed requires [key share locks](https://materialize.com/blog/building-a-linearizable-log-on-read-committed) within the database. This is because between a writer's read and its insert, another writer can append `seqno + 1` and a truncation of old `seqno`s can delete it, so the insert succeeds and leaves a gap. The `FOR KEY SHARE` lock blocks that truncation until the insert completes.
- If two writers both win `head + 1`, or a writer commits a non-contiguous `seqno`, the shard's history diverges.
</details>

**C3. Scan and truncate.** Persist reads a shard's state back via `scan`, which must return every version from a given `seqno` onwards, in order and with no gaps. It reclaims space via `truncate`, which deletes every version below a given `seqno` but must never delete the head (the latest version).

<details>
<summary>Implications and Details</summary>

- If a `scan` skips or reorders versions, or a `truncate` deletes the head, then persist cannot rebuild the shard's state, which is data loss.
- If a `truncate` deletes too little, consensus rows leak, but correctness holds.
</details>

**C4. Schema.** When persist opens consensus it creates its own schema and a single `consensus` table keyed by `(shard, sequence_number)`, so the role it connects as must be allowed to create both.

<details>
<summary>Implications and Details</summary>

- If the role cannot create the schema or table, opening consensus fails.
- On CockroachDB, persist also shortens the table's garbage collection TTL to 10 minutes and turns off automatic statistics. Without the short TTL, deleted rows pile up and scans slow down. If the role cannot make these changes, persist logs a warning and continues.
</details>

**C5. Connections.** Persist connects to consensus through a connection pool (50 connections by default). Statements have no timeout, on the server or the client. Persist only bounds getting a connection and detecting a dead one, so a slow query on a healthy connection runs as long as the database takes.

<details>
<summary>Implications and Details</summary>

- If the pool is too small for the load, a call waits up to 60 seconds for a connection and then errors, which persist retries forever. Hence the symptom is a stalled request.
- Changing the pool size requires a process restart.
- On CockroachDB every connection must run at serializable, and persist panics otherwise.
</details>

**C6. TLS.** TLS to consensus is configured entirely by the connection URL (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`), so the connection depends on the operator rather than persist.

<details>
<summary>Implications and Details</summary>

- Only `sslmode=verify-full` checks the server's hostname. `verify-ca` checks the certificate but not the hostname, `require` verifies the certificate only if `sslrootcert` is set, and `disable` and `prefer` (the default) verify nothing.
- A client certificate needs both `sslcert` and `sslkey`; if only one is set, opening consensus fails.
- Persist detects dead connections with TCP keepalives (idle 10 seconds, interval 5 seconds, 5 retries), a 30 second TCP user timeout and a 5 second connect timeout. Hence the database, and any proxy in between, must not drop keepalives.
- Pooled connections are recycled after at least 5 minutes so load gradually rebalances across database nodes.
</details>

#### Performance Expectations

Every state change to a shard is a `compare_and_set`, so its latency bounds how fast a shard's `upper` and `since` can advance. Rough latency targets, based on production today: p50 = 10 ms, p99 = 100 ms.

In cloud a consensus database is shared across environments (one per region).

#### Non-goals
- **No cross-key transactions** as every consensus data operation is single-key (single shard) and single-statement. No multi-key or statement transactions are used; the only exceptions are the schema setup when opening consensus and an admin-only listing of shards.
- The underlying consensus store does **not require** *secondary indexes, triggers, stored procedures, or foreign keys*: one table, one primary key.


## References

Design:
- [Formalism](/doc/developer/platform/formalism.md)
- [Persist design](/doc/developer/design/20220330_persist.md)
- [State PubSub](/doc/developer/design/20230418_persist_state_pubsub.md)
- [Persist schemas](/doc/developer/design/20250707_persist_schemas.md)
- [Hedged blob gets](/doc/developer/design/20260806_hedged_blob_gets.md)

Code:
- [`location.rs`](/src/persist/src/location.rs): the `Blob` and `Consensus` traits.
- [`s3.rs`](/src/persist/src/s3.rs): the S3 blob implementation (multipart, timeouts, health check).
- [`postgres.rs`](/src/persist/src/postgres.rs): the consensus implementation for both CockroachDB and Postgres (schema, compare-and-set queries, truncate, isolation).
- [`postgres-client`](/src/postgres-client/src/lib.rs): the consensus connection pool and keepalives.
- [`tls-util`](/src/tls-util/src/lib.rs): consensus TLS (`sslmode` handling, client certificates).
- [`cfg.rs`](/src/persist-client/src/cfg.rs): the defaults this doc quotes (leases, connection pool, timeouts).
