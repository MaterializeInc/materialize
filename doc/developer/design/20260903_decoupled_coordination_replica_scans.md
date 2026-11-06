# Replica consensus scans: consumers versus recovery

**Separate consumers do not require separate consensus polling.** The traces
confirm multiple same-process consumers making empty scans at the same cached
sequence number. Keep their distinct data/progress contracts, but share recovery
where another live owner already covers the shard. Do not remove recovery or
replace it with oracle calls: an oracle supplies a required timestamp, not the
catalog contents or dataflow completion through that timestamp.

No production behavior changes in this investigation. Temporary instrumentation
is preserved as reproducible patches, not enabled in production source.

## Measured attribution

Fresh debug deployments, one adapter/environmentd, four single-worker replicas,
an indexed one-row table, PostgreSQL 18 SERIALIZABLE consensus and file blobs.
Each warms for five minutes, then records two five-minute idle windows. Native
read-protection publication is 10s. Upstream is matched-settings `77ae0a3034`,
not current main. Native is `2d17eeffb7`. Both have the same common scan-origin
instrumentation. Builds and measurements ran sequentially.

The event is emitted after the actual consensus scan, including its shard,
requested sequence number, returned row count and bytes. Every exposed scan
counter reconciles with these events in both windows. No steady-state scans are
unclassified. Pubsubd exposes no scan counter and emitted no scan events.
These scans read Persist metadata, not WAL rows or the timestamp oracle.

Catalog replica scans/s, window 1 / window 2:

| Actual caller | Native | Upstream |
| --- | ---: | ---: |
| Row-listen fallback | 1.525 / 1.884 | 0.550 / 0.580 |
| Existing writer-upper fallback | 0.310 / 0.653 | 0 / 0 |
| Added storage-metadata upper observers | 0.476 / 0.722 | 0 / 0 |
| Follower's catalog-progress helper | 0.007 / 0.010 | 0 / 0 |
| Metadata CAS recovery | 0.686 / 0.902 | 0.010 / 0 |
| Append-upper recovery | 0.050 / 0.067 | 0 / 0 |
| Pre-rollup state refresh | 0.656 / 0.549 | 0.473 / 0.346 |
| Rollup history read | 0.656 / 0.549 | 0.473 / 0.346 |
| Live-state maintenance reads | 0.310 / 0.286 | 0.293 / 0.250 |
| Cold machine acquisition | 0.063 / 0.077 | 0.050 / 0.067 |
| Explicit catalog upper / transaction entry | 0.117 / 0.107 | 0 / 0 |
| **Catalog replica total** | **4.855 / 5.806** | **1.848 / 1.589** |

Total deployment scans are **7.748 / 8.447** native versus **7.047 / 6.965**
upstream. Native adapter plus controller issue fewer scans than upstream
environmentd. The net excess is therefore smaller than the catalog-replica
excess. The earlier roughly 2.8/s deployment difference is not a constant.
These short diagnostic windows establish attribution, not a precise prediction
of savings from deleting a particular observer.

### Why the catalog replica differs

It runs many persisted catalog projections and indexes. In the observed native
publication inventory, 90 collection IDs map to 73 progress shards, all eligible
for policy proposals. Installation and scheduling have finished, but publication
still consumes their uppers. Main already has many of these builtin MVs. Their
existence is not a branch addition.

The branch adds metadata observers alongside their runtime consumers, changes
the introspection topology, and performs catalog-governed protection and
compaction work in replicas. The measured gap includes both those new observers
and additional scans from existing row readers, MV upper waits and append-CAS
recovery. It is not all a second catalog listen.

Two readers of `mz_subscriptions_raw` account for 302 native scans across the
windows. They read the physical data shard. The follower's transactional-table
progress observer follows the **WAL shard**, not that data shard. Removing the
WAL observer would not remove those physical listens or their polling.

Most native metadata-CAS recovery scans come from `compare_and_append` on MV
output shards. They are failed conditional writes, not freshness sampling. The
trace identifies the retrying operation, not the winning concurrent operation.
Reducing conflicting metadata writes is a separate route to fewer retries.

Rollup and live-state maintenance account for about 98% of catalog-replica scan
bytes in these windows. Fewer empty fallback calls will not remove that payload.
Cold acquisitions are short-lived physical readers/fetchers on hydration-history
shards, not periodic reconstruction of all follower metadata.

## Which observers are needed?

| Consumer | Required observation | Reuse opportunity and boundary |
| --- | --- | --- |
| Catalog follower | Ordered committed rows, including fencing | Its row listen currently drains only to a known upper. The upper helper is its normal recovery driver, not another row consumer. The metadata watch on this same catalog shard duplicates that recovery. |
| Transactional inputs | Logical WAL progress | Reuse the process-local `TxnsRead` once initialized. Its continuous subscription runs independently of Schedule. A physical table upper is not a substitute. |
| Ordinary storage inputs | Progress for installation, scheduling and policy | A local active source listen can supply recovery. Before Schedule it is not yet listening. Its heartbeat is much slower. A reader on another process does not update this process's cache. |
| MV outputs | Actual durable output upper for reconstruction and recovery protection | Runtime mint already watches this upper. Reuse recovery where that owner is local and live, but retain pre-install/restart coverage. |
| MV read-back | Rows and consumed-data progress | Keep it. Its progress can lag the durable output upper during replay. It cannot replace the mint upper observation. |
| Policy publication | Complete logical-input evidence and committed authority | Keep catalog validation, protection and alias checks. Physical dataflow imports can omit optimized-away logical inputs. Indexes with no replicas still need policy coverage. |

`PersistClientCache` shares state within an OS process. Each upper observer and
listen currently starts its own `Machine::wait_for_upper_past` retry loop.
PubSub updates the shared state, and another owner's sequence-number advance can
suppress a scan. An **empty scan does not advance the sequence number**, so the
next waiter can scan again. Healthy connectivity does not prevent these calls
and does not prove every notification arrived.

A concrete witness in the first native run: `mz_databases` receives five empty
scans from writer-upper, row-listen and metadata-upper consumers within 108ms,
all from sequence number 565 and waiting at the same frontier. The event log
contains their individual callers, not merely a count of wakeups.

There is no linearizability justification for these duplicate background polls.
However, deleting every helper is not equivalent to sharing recovery. Catalog
listeners and suspended sources retain independent lease heartbeats, which can
repair stale state through CAS recovery. At defaults that interval is 225s.
Relying on this would lose normal listen retry cadence, not prove permanent
deadlock. A continuously driven catalog row listener could own recovery instead,
but its partially consumed reads must survive maintenance turns. Canceling and
restarting `sync_inner`/`fetch_next` is not a safe replacement.

Missing progress must remain distinct from an empty, completed frontier. Generic
compute frontier reports are not interchangeable with raw durable uppers: they
can incorporate `as_of`, lag replay or reflect teardown. Observation sharing
must not invent completion or restore retired executions.

## Recommended next repairs

1. **Skip an already-satisfied pre-rollup refresh.** In
   `internal/maintenance.rs::perform_in_background`, the requirement is
   `machine.seqno() >= rollup_seqno`. Passing that target to
   `fetch_and_update_state(Some(...))` asks for strictly newer state at equality.
   Native records 197 / 165 such scans on the catalog replica, all empty.
   `need_rollup` selects the committed seqno, and command application updates
   shared state before returning maintenance. Check the satisfied minimum before
   fetching. Keep the assertion and the separate history read needed to encode
   the rollup. This is an upstream opportunity too, not a new branch protocol.
2. **Share background recovery, not data-consumption frontiers.** Start with the
   definite catalog-shard duplicate and initialized local WAL ownership.
   For the wider duplication among dataflows, place coalescing at Persist's
   shared shard-state boundary rather than adding follower/runtime handover
   protocols. Preserve recovery under lost notifications, new demand, shutdown,
   suspension and remote-worker placement. Do not coalesce explicit fresh
   certification calls as if they were background retries.
3. **Keep conditional-write recovery and complete history reads.** Most append
   CAS recovery scans return new metadata. Removing them would retry against
   stale state. Rollup encoding needs the historical diff range, while GC needs
   live-state history, not just current upper. Reusing a complete retained diff
   range could avoid reads later, but requires a measured memory tradeoff and
   reconstruction fallback. Neither is permission for an all-shard sweep.

Removing only the catalog helper is low value for this workload. Consolidating
its ownership can simplify the architecture, but it is not the bulk scan fix.
No observer-removal speedup or lost-notification behavior after removal is claimed:
this investigation did not ship or benchmark such a change.

## Same pattern elsewhere

The source survey found additional scoped candidates:

- `QueryClient::collection_reader` lazily starts a third full WAL reader beside the
  two already started by StorageCollections and storage-controller. Inject an
  existing reader rather than replaying the WAL again. Keep metadata-only query
  access independent of local controller inventory and preserve read authority.
- Query progress waits can duplicate registered storage/WAL recovery. Reuse
  covered owners, retaining coverage for newly visible, not-yet-registered shards.
- `wait_for_compute_frontier` subscribes to local change notifications before
  inspecting state, but also polls that state every second. That timer cannot
  repair missing remote reports. Verify topology notification coverage and remove
  the cache-only poll, not transport recovery.
- Protected prewarming's separate client-protection projection freshly syncs
  beside the serving catalog's progress driver. Its background target can reuse
  that observation. This projection is not active on ordinary joined adapterd.
- Readiness already has a retained reference reader and a registered storage
  watcher. Reusing cached progress needs an explicit freshness decision: stale
  active-reference rows can make a candidate appear caught up. Keep the oracle,
  registration and retirement checks.

Controller/adapter catalog helpers have the same bounded-listen split described
above. Frontier output reconciliation already reuses registered storage progress.
Differential writers' retained row subscriptions are needed to incorporate peer
updates and rebuild conditional appends. They are not extra idle upper pollers.
See the [follow-up inventory](20260903_decoupled_coordination_followups.md).

## Evidence, verification and limits

[Recorded comparison](20260903_decoupled_coordination_replica_scans.json) includes
binary hashes, exact settings, counter reconciliation and per-shard attribution.
Raw evidence is under `~/.local/share/aj/m3-evidence/replica-scan-necessity/`:

- `v2/scan-audit-1793974261/`: fully tagged native run.
- `main/scan-audit-main-1793979420/`: matched upstream run.
- `scan-audit-1793972151/`: first native run and duplicate-scan witness.
- `consumer-audit.md` and `observer-pattern-survey.md`: source ownership details.
  The latter includes the correction about heartbeat recovery.
- `main/build-first-failure.log`, `main/build-retry.log`, `main/disk-guard.json`:
  initial linker failure with a truncated log, successful cold rebuild, and the
  30-GiB disk reserve. The shared target directory disappeared between attempts.

Reproduction tools are in `misc/benchmarks/native-idle/`. Apply
`scan-audit.patch` plus `controller-metrics.patch` to the native base, or
`scan-audit-main.patch` to the upstream base. Build with the supported wrappers.
Use `observe.py` with the recorded settings, then `analyze_scans.py RUN` and
`analyze.py RUN`. Preserve unresolved classifications and scrape uncertainty.
The reducer tests cover failed/empty scans, missing counters, boundary uncertainty,
unmatched counts and resets. Black, Ruff and those tests pass.

All measured fixtures passed and cleaned up. Broad CI remains non-green and was
not rerun. There is no production correctness or performance-fix claim here.
