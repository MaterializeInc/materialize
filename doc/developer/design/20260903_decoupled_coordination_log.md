# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Assess remaining architecture
load separately. Do not change Persist or the 60s publication default.

## Next concrete action

Migrate `test/bounded-memory/mzcompose.py` to inventory-first launch. Three
scenarios drop/recreate the external replica inside testdrive scripts. Give these
explicit script boundaries so the runner can launch with refreshed IDs before
queries need the replacement. Preserve workload sizes, memory limits and assertions.

Upsert rehydration now passes all four original variants with four workers,
retained launch options across restart, row/statistics checks, updates and deletes.
Independent review found no defects. The first run exhausted the scratch runner's
ten-minute budget during the final restart. The complete rerun passed with a
twenty-minute overall budget and unchanged resource caps/assertion deadlines.
Evidence: `native-driver-scratch/upsert-launcher-evidence/aj-native-cluster-e79068f4/`.
No OOM occurred and fixtures were removed. This is mounted default-debug candidate
verification, not rebuilt-image CI.

The separate upsert load-test launcher remains unconverted. Its scenario list
combines two cases into one malformed tuple. Approval to split that tuple was
requested before expanding scope. Other launcher work can proceed meanwhile.

Upsert failpoint recovery now retains native ownership through healthy, failing
and recovered launches. All three original failpoints pass on capped scratch,
including expected source errors, retained rows and continued ingestion after
recovery. Four workers and each replica's identity survive restart. Evidence:
`native-driver-scratch/upsert-launcher-evidence/aj-native-cluster-1e5bd8fe/`.
These are mounted default-debug candidate checks, not rebuilt-image CI. No OOM
occurred and fixtures were removed.

Memory-limiter replica recreation now refreshes native identities. All three
original scenarios pass with retained default-debug binaries on capped scratch:
both million-row hydration checks and deliberate exit 167 at the low limit.
The heap bound is unchanged. Distinct identities, no OOM and fixture cleanup are
verified in `native-driver-scratch/cluster-launcher-evidence/aj-native-cluster-71ded598/`.
Its existing default-suite exclusion (database-issues#9502) remains unchanged.

Cluster death detection and replica metrics now launch with native ownership.
Both actual workflows pass on capped scratch with retained default-debug binaries.
Death detection establishes query readiness and checks only post-fault logs.
Metrics preserves zero-before-connection assertions with disabled control proxies,
then checks workloads, metrics and keepalives after enabling them. Review's startup
registration race is fixed without retrying unexpected nonzero values. Evidence:
`native-driver-scratch/cluster-launcher-evidence/`. An initial metrics runner failed
only while inspecting an unused testdrive container. The corrected runner passes
with unchanged workflow assertions, no OOM and fixture cleanup verified.

Replica-isolation is committed and passes its complete split/unified disruption
matrices with two processes per replica. Tracing passes after establishing native
query readiness before its unchanged configuration-propagation deadline. Kafka
source resumption preserves its four workers, restart, offsets and continued
ingestion. Source/sink-error tests no longer launch an unused replica, with active
workloads/assertions statically verified unchanged. Evidence is under
`native-driver-scratch/{isolation-verified,tracing-evidence}/` and
`native-driver-scratch/aj-native-kafka-resumption-55360511/`. These are targeted
mounted-candidate outcomes, not rebuilt-image CI.

Production ingress removal still needs the remaining external launchers. Native
multi-process nonleader coverage now passes in both Timely topologies. The launcher inventory
is in `native-external-launcher-audit.md` and `native-replica-execution-boundary.md`.

Owned worker `i-064fa517ec1d8bbdb` is available for this next unit, with a one-day
expiry recorded in `native-driver-scratch/worker.json`. Connect through its
instance-specific `native-driver-scratch/remote.py`. No access rules changed.
Keep the compiled candidate and evidence before releasing the worker.

Do not run heavy suites or builds on the local VM. Polling reserves did not prevent
an overload that required a restart. Scratch test containers must share the
`aj-native-tests.slice` hard budget: 40 GiB memory, no swap, eight CPUs. The runner
verifies kernel limits, container membership and aggregate OOM counters. Keep the
2 GiB RAM and 30 GiB disk reserves as additional guards, not the primary limit.
See `native-driver-scratch/scratch-hard-budget-effective.txt` and
`isolation_verify_capped.py`. Local interrupted fixtures were removed with logs
preserved in `native-driver-scratch/local-restart-cleanup/`.


## Verified outcome

Controllerd owns processes, replica followers own maintained execution, and
adapters retain policy, transactions, serving protection and transient queries.
Legacy combined/read-only serving, private migration execution, alternate
protection catalogs and exclusive age-based process cleanup are removed.

Selected native debug checks cover startup/restart, protection, queries,
compatible full writers, promotion and explicit retirement, warmed replicas,
source/sink handover, temporary-object ownership, pruning and table preservation.
Temporary objects retain session visibility and execution ownership. Promotion
preserves them. Disconnect, confirmed replacement and retirement reclaim them.
Format 97 requires fresh experimental state, with no ownership backfill.

Subscription IDs are execution-qualified across public observations, compute
logging and physical-dependency joins. Two-adapter default-dev coverage verifies
colliding local IDs, per-replica and merged progress, independent cleanup,
promotion survival and explicit retirement. Cold native SLT startup and targeted
catalog SQL checks pass with the existing test-harness stack budget on both
hosting contexts. Production stacks, Persist and publication settings are unchanged.
Commits: `8c9e18f0d7` (subscriptions), `917c98e010` (SLT harness).
Evidence and review dispositions: `native-subscription-frontier-assessment.md`.
These checks used mounted candidate binaries, not rebuilt-image CI.

Native programmable renderer scripts preserve dynamic plans and within-owner
mixed exports. Their default-build tests and PostgreSQL/MinIO composition pass
in both Timely topologies. External benchmark launch/restart checks also pass in
both topologies for native and historical v26.30 environments, preserving table
contents and continued writes. Native checks use retained default-debug binaries.
This covers actual launcher/executor methods, not the full benchmark workflow,
multi-process ingress or rebuilt-image CI. Fixtures are removed. Evidence:
`native-driver-scratch/verified/` and `native-driver-scratch/benchmark-verified/`.

The pinned 26.46/26.47 prototype pair shares implementation and plan encoding,
not arbitrary release compatibility. Its lifecycle runs precede the isolated
pgwire stack fix. That fix separately passes initial and resumed subscription
FETCH through both protocols with production worker stacks. The kept workflow
reproduces the old overflow. These are local outcomes, not broad CI acceptance.

## Live blockers and decisions

- Catalog-free production replica ingress remains. The programmable native driver
  replacement is verified. External launchers still need native context and
  refreshed IDs on replica recreation. Preserve historical-image-only paths.
  The dormant final-preflight hook also needs its boundary decision. See
  `native-external-launcher-audit.md` and `final-preflight-boundary-research.md`.
- Forced-migration cases and some platform/stress workflows remain unconverted.
  Preserve source/sink workloads, predecessors and compatible-image coverage.
  Pre-native-to-native upgrades are unsupported, not reset or converted.
- Full upsert stress exceeded the local deployment memory cap. Worker cleanup is
  repaired, but full-stress acceptance remains blocked. Do not shrink the workload
  or raise caps blindly. The full workload still needs a measured retry, not a
  smaller substitute.
- Data-ingest/parallel stress, randomized zippy/restore and bumped-version platform
  runtime acceptance remain incomplete. Selected same-build table preservation
  passed. An earlier index-publication stall and a 673-second source-status query
  remain unexplained. Passing retries do not resolve them. Startup/readiness and
  other unresolved cases remain in the
  [follow-up inventory](20260903_decoupled_coordination_followups.md).

Broad CI is not green. Last reviewed broad evidence is
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image gate. Registry visibility, merge skew, Persist JSON-order
checking and stale hydration expectations remain unresolved. Full bin/fmt fails
on unchanged long lines in `src/catalog/src/catalog.rs` and
`src/catalog/tests/read-write/oracle.rs`. Changed-file checks pass. The PR stays
draft.

## Essential evidence and local state

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`.

- Cursor fix, negative control and review:
  `native-compatible-current/subscription-cursors-verification.md`.
  Latest fixed binaries and manifests: `native-compatible-current/subscription-stack-fixed/`.
- Pinned compatible lifecycle, basic coexistence and inactive-predecessor coverage:
  `native-compatible-current/verification.md`, `basic-verification.md` and
  `offline-candidate-verification.md` in that directory.
- Pruning, selected platform preservation and the blocked stress case:
  `native-compatible-current/forward-pruning-verification.md`,
  `platform-tables-verification.md` and `upsert-verification.md` there.
- Ownership/removal evidence: `native-follower-only-verification.md`,
  `native-temporary-owner-verification.md`, `native-retirement-verification.md`
  and `native-cdc-runtime-verification.md`.
- Detailed earlier evidence pointers are preserved in
  `native-handoff-before-subscription-stack-final.md` and the
  [archive](20260903_decoupled_coordination_log_archive.md). The
  [publication comparison](20260903_decoupled_coordination_publication_interval.md)
  and `../logical-component-load/` are the performance baseline, not a claim about
  cleanup's performance impact.

All owned fixtures are removed. Obsolete 26.47 demo binaries were reclaimed,
with their manifests and evidence retained. The stack-fixed frontier-bug baseline
and upstream baselines remain. The identity-fixed production candidate is preserved
locally with matching source and binary manifests. Keep heavy work sequential, enforce 30 GiB disk and 2 GiB
available-memory reserves, and use one low-priority Cargo job. Preserve baseline
artifacts,
third-party caches, the persist-traffic worktree and unrelated
`tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`.
