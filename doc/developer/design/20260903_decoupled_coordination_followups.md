# Decoupled coordination: architectural follow-ups

This is an audit backlog, not an expansion of Milestone 3. The two-adapter
startup/frontier repair takes priority. Implementation of the opportunities
below needs separate scope agreement. Findings are based on source inspection,
not measured performance.

The direction is incremental observation and publication by the component that
owns a fact. Reuse existing identities, catalog changes and Persist listeners.
Do not introduce a designated maintenance adapter, another consensus protocol,
or a general sampling framework.

## Recurring full reads

### Retired-row cleanup

The call to `collect_retired_rows` in `src/adapter/src/coord.rs` runs after
read-protection publication. `src/adapter/src/coord/retired_rows.rs` prevents overlapping local
tasks, but each new task scans current storage statuses, sessions and subscriptions.
The status cursor is in `src/storage-client/src/collection_writer.rs`.

Retain observations indexed by incarnation and exact shard identity. Incorporate
both row changes and retirement certificates, then examine affected owners.
Triggering only on retirement is insufficient: late predecessor writes can arrive
afterward. Preserve exact-upper conditional retractions, successor rows and the
txn-WAL path for session/subscription cleanup. Conflicts must refresh observations,
not replay old negatives. This is a strong next bounded optimization candidate.

### Caught-up replica-status analysis

`src/adapter/src/coord/caught_up.rs::analyze_replica_looping` starts a fresh
`snapshot_and_stream` during each enabled readiness check. It scans retained
replica-status history before selecting the relevant deployment and time window.
This is separate from the retained frontier-reference reader.

A task-owned listener could maintain qualifying events and their expiry times.
Preserve the selected observation frontier, deployment filtering, multiplicities,
retractions and expiration without new input. Release the reader when readiness
checks end. Do not silently replace the current selected-cut semantics with
unrestricted latest state. This is another bounded incremental-read candidate.

### Arrangement-size history samples

The `arrangement_sizes_snapshot` task in
`src/adapter/src/coord/message_handler.rs` reads complete size and hydration relations at one oracle timestamp on each configured
sampling interval. Publication checks freshness again.

Retained inputs could assemble samples at a common completed frontier. Consider
the replica-owned introspection opportunity below before adding another adapter
cache. Keep restart freshness and the publication check. History still needs
periodic samples of unchanged values: incremental inputs do not imply
changed-only history output.

### Native storage-metadata resolution

The execution loop in `src/clusterd/src/catalog_follower.rs` follows catalog
changes incrementally but rebuilds its wanted set and calls
`storage_metadata::resolve` each reconciliation turn. That function reconstructs
definitions, reads selected plans, resolves schemas and obtains physical uppers
for wanted dependencies and outputs.

Cache immutable resolution by GlobalId, shard, schema version and selected plan.
Invalidate from existing catalog effects and retry missing prerequisites
separately. Preserve exact table versions, MV writer selection and catalog-prefix
consistency. Physical uppers remain dynamic observations. This is repeated
metadata reconstruction, not a full data-shard snapshot.

### Native compaction alias scans

`src/clusterd/src/catalog_follower/compaction.rs::reconcile` recomputes shard
bounds each successful reconciliation turn. It scans all collection-to-shard
mappings, although only wanted shards receive work.

An incremental shard-to-alias permission index could restrict recomputation to
affected shards. Include every alias of each wanted shard, including aliases
outside the replica's wanted set. Missing bounds prohibit advancement. Keep
pending retries for failed or rate-limited downgrades, even without new catalog
changes. This is not an all-shard cleanup sweep.

## Direct-owner opportunities

### Remaining unified compute introspection

`src/adapter/src/coord/introspection.rs` subscribes to replicas and relays batches
into storage. Reinstallation deletes prior rows by replica ID. Error counts,
hydration times and arrangement sizes remain examples of indirect publication.

Replica execution can publish these facts under existing deployment/incarnation
identities, with separate public reductions. Preserve predecessor suppression,
unknown-after-replacement behavior, cumulative/history contracts and freshness.
Frontier reduction rules do not automatically apply to other metrics.

### Storage statistics and webhook contributions

`src/storage-controller/src/lib.rs` receives replica statistics, and
`src/storage-controller/src/statistics.rs` periodically publishes controller maps.
Replica-produced statistics are candidates for direct execution publication.
Webhook contributions remain adapter-owned and need aggregation, not relocation
to replicas or a designated adapter.

Preserve cumulative counters across restart and retirement, inactive-stat
retention and webhook accounting. The relay boundary is clear. The smallest
complete conversion still needs investigation.

### Catalog-derived projections and lifecycle history

The design retains originator-only projection publication and its scoped M3
inconsistency allowance. Suitable derived facts could become raw-catalog-backed
relations. Transactionally sourced lifecycle events are a separate possible
follow-up. Neither is approval to add projection-recovery protocols, watermarks
or another policy owner during this milestone.

## Separate correctness investigation

`src/adapter/src/coord.rs::prune_storage_usage_events_on_startup` and adjacent
startup history pruning derive negatives from snapshots and submit ordinary
builtin updates. Their comments rely on startup exclusivity. Concurrent adapters
make process-local startup exclusivity insufficient as a general argument.

Audit the real multiwriter retirement/append ordering before treating these as
harmless one-shot reads. This is a concrete concern, not a reproduced failure.
Do not fold its repair into frontier ownership without scope agreement.

## Snapshots that are not evidence of a recurring rescan bug

- Catalog following retains a listener. `cached_snapshot` clones typed in-memory
  state rather than rereading the Persist shard.
- Txn-WAL initializes a subscription and consumes it incrementally. A per-shard
  `data_snapshot` token is not a WAL-wide scan. Keep the approved GC-validation
  repair separate and do not add an all-shard sweep.
- Statistics restoration and collection-manager retention preparation run at
  task startup, not every maintenance tick.
- Bootstrap configuration reconciliation needs a consistent desired-minus-current
  base. Correct conflict handling matters more than avoiding this one-time read.
- Controller orphan cleanup deliberately lists services before its catalog
  snapshot. A stale cached inventory would violate that ordering.
- User queries and recovery snapshots require their own semantic justification.
  A method named `snapshot` alone is not a finding.

The current frontier repair already retains differential-writer observations,
samples physical storage progress cooperatively, moves replica reports to their
owner, and retains caught-up frontier observations. Do not count those paths as
untouched findings in subsequent audits.
