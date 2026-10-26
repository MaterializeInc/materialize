# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3

Implementer session: `2026-09-14-13-05-31-256`. Bookmark:
`decoupled-coordination`, based on pinned `d9a2c5dc40`. M2 remains closed,
including the 60s default. M3 slice 1 is incomplete. No adapter uses remote
pubsub yet, and controllerd/adapterd have not been implemented.

Local commits, not yet pushed:

- `50a0d4dd`: standalone pubsubd and unified-image packaging. Clippy, 11 RPC
  boundary tests, manifest lint and packaged daemon smoke pass.
- `bbe00aba`: opt-in retained local supervision, distinct from explicit deletion.
  Four lifecycle tests, existing restart regressions and caller Clippy pass.
- `4a2a374c`: replica launch-spec extraction, with Clippy and restart regressions.
- `440e7126`: named adapter declarations and deployment-qualified execution
  identity, default declaration only at fresh initialization, schema v96.
  Aljoscha approved no M2-to-M3 conversion. Frozen v95 is unchanged.
- `ab9122e0`: joined catalog reconstruction without shared initialization,
  including writable joins. Unsupported conversion is rejected before fencing
  or admission. Active/prewarm/build/cache cases and legacy nonmutation pass.

All 393 catalog unit tests pass (2 skipped). The join matrix runs as independent
cases, preserving assertions and the standard per-test timeout. The active-join
regression fails on shared-definition mutation without the fix and passes when
restored. Production checks and all-target Clippy pass for catalog, adapter,
clusterd, environmentd, catalog-debug, testdrive and sqllogictest. Broad combined
workspace/integration verification is still open.

Latest verified slices:

- `kwuvpvnl`: catalog integration fixture corrections: generated snapshots include the
  default declaration/allocator, and unsupported-version rejection has its
  precise error assertion. Nonmutation assertions remain unchanged. All 68 tests
  across the five integration targets pass.
- `7585df45`: `TxnsHandle::adopt`: local schema-bearing handle adoption, extracted from shared
  registration. It neither registers nor advances the WAL. Caller retains
  readiness and write-authority responsibilities. Static review, Clippy and all
  33 txn-WAL unit tests pass through `bin/cargo-test`, including the new two-writer
  regression. Direct nextest omits the standard stack setting and overflows in
  two existing tests.
  Adapter startup is not wired to adoption yet.

Next ownership boundary: `bootstrap_tables` resets shared builtin tables and
writer completion/follower refresh both publish derived builtin diffs. Skipping
reset alone still duplicates facts. Architectural review confirms the shared publication gap. Aljoscha has been
asked to workshop transactional publication progress versus storing derived
metadata with catalog definitions before either is implemented. The tradeoff
is recorded in `builtin-publication-decision.md`.
Session ownership and finite table adoption are separate boundaries. Initialization
must remain finite, without embedding a coordinator or shared maintenance in
controllerd. Existing WAL registration/application can recover without a new
initialization marker.

Evidence lives in `~/.local/share/aj/m3-evidence/`: `catalog-join-split-full-unit.log`,
`catalog-join-{review,negative-revision,negative,restored}.*`,
`txn-local-adoption-review.md`, `builtin-publication-ownership.md`,
`table-initialization-recovery.md`, `initialization-join-boundary.md`,
`process-provisioning-boundary.md` and `adapter-declaration-boundary.md`.
`foundation-handoff-detail.md` preserves the detailed verification pointers.
Scratch test logs live in `~/m3-evidence/` and must be copied off-worker.

Use `bin/cargo-test` for the standard stack setting, forwarding `--lib` after
`--`. Metadata and comparison Postgres URLs are respectively
`~/m3-evidence/native-metadata-url.txt` and `native-postgres-url.txt` on scratch.
The latter supplies `POSTGRES_URL`. Leave `m2-pause-test-postgres` untouched.

## Concrete starting points

- `src/catalog/src/durable.rs` separates join from initialization/promotion.
  `Catalog::open_committed` in `src/catalog/src/catalog.rs` reconstructs initialized
  same-version state. Neither is a complete deployment initializer. Own-build
  plans remain adapter work, not a reason to embed a coordinator in controllerd.
- `src/adapter/src/coord/read_protection.rs::adapter_execution_predecessor` uses
  the configured adapter identity. Joining must supply the selected declaration.
  `src/adapter/src/coord.rs::bootstrap_tables` still resets shared system-table contents.
  Ordinary adapter join/restart must not supersede or reset a peer.
- Reuse `src/cluster-controller/` policy with a standalone catalog/observation
  context. Explicit replica reconciliation and transactional resource admission
  must follow the move. Extract provisioning from `src/controller/src/clusters.rs`,
  not the whole compute/storage controller. Adapters need discovery, not backend
  lifecycle workers or permissions.
- Pubsub is embedded in `src/environmentd/src/environmentd/main.rs`.
  `src/persist-client/src/rpc.rs` provides the relay and remote clients. The
  generation-owned service in `src/orchestratord/` must become environment-scoped.
  `src/persist-client/src/internal/service.proto` currently disclaims cross-version
  compatibility. Extraction must establish the supported wire contract, not assume
  that Persist payload compatibility also covers the envelope.
- The process backend has deterministic Unix sockets, while adapter listeners use
  TCP. Reuse its endpoint/proxy boundary and publish a usable pgwire address.
  Backend Online is not SQL readiness. Check child adoption/teardown, resource
  enforcement and unchanged launch specs rather than adding another supervisor.
- For the multi-adapter slice, inspect `src/adapter/src/coord/appends.rs` conflict
  handling and `src/storage-controller/src/collection_mgmt.rs` empty advancement.
  Introspection `RowScope` distinguishes deployments, not executions, and webhook
  writers in `adapter_storage.rs` claim all NULL-replica rows. Repair ownership
  without duplicating shared collectors. Hydration history already has
  concurrency-safe OCC publication and does not need a wholesale rewrite.

Replica burst and graceful-reconfiguration state are already deployment-local.
Preserve that policy during extraction. Separate runtime proofs do not establish
all burst/reconfiguration overlaps with promotion, but this is not a new policy
redesign or an instruction to start a broader acceptance campaign.

## M2 baseline and exclusions

Closeout is recorded at `676346d6`. CI140484 at `01ef1f72` has 93 passed jobs
and three failed exclusions: upstream merge skew, existing-environment
source-to-view conversion, and Kafka RTR's explicit-DDL revision conflict.
CI140485 passes both original fixed-seed parallel shards at the normal
8-CPU/16-GiB CI budget with the 60s default. The identified Iceberg descriptor
expansion is corrected, with no OOM in that run. This is not a general memory
bound or scheduling guarantee, and the combined build is not wholly green.

Keep those proof scopes closed and regular regressions enabled. Passing unchanged
retries do not explain every earlier timeout or the missing-Persist-metric flake.
Evidence under `~/.local/share/aj/m2-evidence/scratch/` includes
`cadence60-default/ci140484/`, `cadence60-default/ci140485/`,
`cadence60-default/iceberg-causal-check/causal-conclusion.md`,
`cadence60-default/publication-pause/`, `combined-publication/` and
`table-lifecycle-verification/`. The closeout revision retains the detailed
handoff and further evidence pointers. Do not turn them into an M3 backlog.

The explicit-DDL admission correction belongs to Aljoscha's separate work. Do not
suppress structural revision checks or replay acknowledged DDL here. Raise an
actual M3 dependency before pulling that work in or changing the pinned base.
Existing-environment conversion, unsupported-version interoperation, managed
replica-targeted MV handover and query-relative public replica visibility remain
outside the milestone scope. The deferred orchestratord draft is preserved at
`m2-deferred-orchestratord-retirement` (`5bb35005`), not an approved M3 solution.
The archived log is historical context, not required reading or a task list.

## Worker

Use `i-0788036f0778687f1`, m8i.4xlarge, 16 vCPUs, 64 GiB RAM and 500 GiB disk.
Last verified expiry: 2026-10-28 19:01:27 UTC. Check status and extend expiry as
needed under the prompt's existing authorization. The traffic investigation's
`i-01023ce0e1db179f9` belongs to another session. Leave it untouched.

The Iceberg and SLT fixtures are stopped, with data and evidence retained.
Preserve useful evidence before replacing fixture state. The separate
`m2-pause-test-postgres` container is not part of that fixture. `mzrepro.slice`
applies a temporary aggregate 8-CPU/16-GiB workload budget, not the build budget or
a faithful CI-host reproduction. Use `bin/ci-builder run stable <command>` for
pinned tooling, repository entry points and the worker's existing caches.
