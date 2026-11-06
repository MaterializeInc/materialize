# Native coordination: observed progress

Consensus scans fell **93%**, from **122–123/s to 7.9–8.4/s** in the fresh idle
fixture. Explicit upper fetches fell **99%**, from **115–117/s to 0.71–0.77/s**.
The final debug-build join/restart/removal witness passed while its survivor kept
writing. All owned runtimes are stopped.

This removes redundant freshness polling, not the remaining compute or storage
cost of the architecture. Total CPU and memory changed little. Consensus payload
bytes, writes and blob activity did not materially improve.

## Implementation

- Replica progress and periodic catalog following retain existing Persist watches.
  Empty and immutable plan-cache hits stay local. Reconstruction tries observed
  uppers, then refreshes only a conflicting installation batch.
- Controller reconciliation follows committed inventory. It freshly certifies
  uncertain absence after listing processes, so an unseen peer CREATE cannot cause
  deletion. Controller configuration uses Persist's API, releasing the existing
  barrier that allows its PubSub client to connect.
- Frontier output consumes retained progress. Exact positive binding observations
  can use the applied catalog prefix. Absence or mismatch requires fresh
  certification. Registration and read authority remain separate.
- Retirement cleanup protects a chosen observed cut, snapshots that cut and
  conditionally retracts its rows. Peer advancement rejects the candidate rather
  than retargeting its negatives. Differential writers reuse retained observations
  for ordinary attempts, but still refresh at initial acquisition and after
  conflict backoff.
- Retention validates complete logical inputs against durable observed progress.
  Finite progress remains usable under the candidate policy after an unobserved
  closure, matching upstream's cached-frontier policy changes. Cold inputs still
  acquire state. Timestamp floors reuse applied prefixes, and catalog conflicts
  consume the boundary already returned by the failed append.

Operation-time certification, transaction-open validation, oracle allocation,
expected-upper CAS, fencing and physical-readability checks remain. There is no
new leader, consensus protocol or longer publication interval.

## Comparable measurements

Same fixture as the [cached-frontier report](20260903_decoupled_coordination_cached_frontiers.md):
one adapter, four one-worker replicas, indexed one-row `idle_probe`, PostgreSQL 18
SERIALIZABLE consensus, file blobs, debug builds, info logs and 10s protection
publication. Each fresh environment warms for five minutes, followed by two
five-minute idle windows. No builds overlap the measurements.

Upstream is the preserved matched-settings `77ae0a3034` environmentd build from
the [idle comparison](20260903_decoupled_coordination_idle.md), not current main.
Values below are window 1 / window 2.

| Metric | Native before | Native after | Matched upstream |
| --- | ---: | ---: | ---: |
| Consensus scans/s | 122.20 / 123.04 | 7.89 / 8.39 | 5.74 / 4.98 |
| Explicit upper fetches/s | 115.50 / 117.11 | 0.71 / 0.77 | 0 / 0 |
| Scan payload, decimal kB/s | 248.89 / 235.17 | 249.43 / 242.52 | 144.14 / 120.90 |
| Consensus CAS/s | 120.71 / 121.96 | 118.24 / 120.95 | 83.21 / 82.89 |
| Blob gets/s | 14.20 / 14.65 | 14.25 / 14.28 | 2.13 / 2.16 |
| Blob sets/s | 9.76 / 9.67 | 9.57 / 9.48 | 4.67 / 4.45 |
| Materialize CPU cores | 1.291 / 1.279 | 1.256 / 1.249 | 0.597 / 0.594 |
| PostgreSQL CPU cores | 0.038 / 0.038 | 0.027 / 0.028 | 0.019 / 0.018 |
| Materialize PSS, GiB | 2.87 / 3.00 | 2.79 / 2.90 | 1.28 / 1.31 |

The removed reads were largely empty refreshes. Fewer calls did not mean less
metadata payload. The roughly 3% CPU/PSS differences are not evidence of a robust
resource improvement from two short windows. Native still uses about twice
upstream's CPU and PSS in this fixture.

| Process | Before scans/s | After scans/s | After CPU cores |
| --- | ---: | ---: | ---: |
| adapterd | 25.44 / 25.97 | 1.76 / 1.91 | 0.260 / 0.257 |
| controllerd | 5.07 / 5.05 | 0.050 / 0.063 | 0.025 / 0.025 |
| Catalog replica | 80.38 / 80.66 | 5.39 / 5.69 | 0.758 / 0.716 |
| mz_system replica | 3.39 / 3.46 | 0.23 / 0.28 | 0.060 / 0.063 |
| mz_probe replica | 3.42 / 3.37 | 0.21 / 0.21 | 0.059 / 0.061 |
| quickstart replica | 4.50 / 4.53 | 0.26 / 0.25 | 0.065 / 0.096 |

Pubsubd issued no consensus scans and used about 0.029 cores. For comparison,
upstream environmentd used 0.261 / 0.259 cores, including its in-process relay.

## Remaining reads

The final controller was connected throughout both windows and received about
4.2 state-diff messages/s. Its previous 0.83 fallback wakeups/s fell to 0.027–0.037/s.
This verifies notification delivery, not merely lower scan counts.

The remaining explicit fetches are predominantly publication/transaction
certification: each replica records about 0.10/s, while the adapter records
0.30–0.37/s. Standard fallback and contention account for much of the remaining
scan traffic. The catalog replica records 3.71–4.18 fallback wakeups/s and
0.84–1.03 CAS mismatches/s. Adapter fallback wakeups are 0.76–0.95/s.
These counters are not disjoint, and a wakeup can avoid I/O after another local
handle advances state. They must not be added into an exact scan decomposition.

The retained boundaries are:

- Cold acquisition and Persist's existing notification-loss recovery.
- Conditional-write catch-up and rebuilding after conflicts.
- Transaction and SQL certification, including current registration authority.
- Fresh absence certification after observing a possibly newer binding or service.
- Physical-since observation and protection for non-reusable read grants.
- Targeted refresh when observed output progress cannot satisfy reconstruction.

Additional replica-local progress observations and publication owners remain
part of native coordination. This is not a claim that every read above upstream
has been eliminated, or that every scan has an exact measured Rust call-site tag.
The source inventory and process/operation counters are preserved in the evidence.

## Verification and limits

Targeted tests cover immutable hits, independent caches with lost notifications,
retention policy changes/completion/restart, exact binding replacement, catalog
fencing, unseen peer CREATEs during cleanup, read acquisition, frontier output,
fixed-cut retractions, differential contention and shutdown. Combined all-target
Clippy passed. Mutation checks detected unconditional plan refresh and disabled
progress recovery.

The final live witness checked actual global IDs and shards, finite raw/current/
public frontiers, peer join, identity-checked restart, catch-up and removal while
the survivor kept writing. Controller notifications were observed directly.
The final launcher exited cleanly and process/container checks found no survivors.

The before idle windows passed, but their post-window diagnostic failed on harness
privileges. The prior report retains that failure and later corrected checks.
An intermediate launch here failed during PostgreSQL fixture startup, before
Materialize launched. The exact retry passed. The fixture now probes TCP rather
than PostgreSQL's temporary initialization socket, and the final run used that fix.
A repeated follower suite was stopped before completion to avoid disk exhaustion.
Earlier follower tests passed, and final live checks cover subsequent changes.

No new CI result is claimed. Last broad CI evidence remains non-green. Arbitrary
release compatibility and external-sink handover are not established here.

## Follow-ups

Prioritize the separate critical-reader birth-ordering investigation before
replacing physical-since checks with catalog permissions. It is a source-level
risk, not a reproduced failure. Catalog replica CPU remains the largest hotspot.
The earlier frontier-reduction diagnosis and incremental retired-row cleanup are
separate optimization candidates. See the
[follow-up inventory](20260903_decoupled_coordination_followups.md).

## Evidence

[Measurements and provenance](20260903_decoupled_coordination_observed_progress.json)
record both windows, exact binary hashes and the measurement-only patch. Production
changes are committed through `cf8b8ba0`. Temporary controller profiling routes
are absent from production source.

Raw evidence: `~/.local/share/aj/m3-evidence/consensus-read-removal/`.
Final run: `connected/observed-progress-1793966552/`. Its `analysis.json`,
`fallback-counters.json`, `frontier-lifecycle.log` and profiles retain the detail.
The root contains check logs, source audits, intermediate runs and cleanup records.
Pinned comparison binaries and evidence remain. Superseded build variants and
obsolete stopped test containers/images were removed, leaving about 26 GiB free.
