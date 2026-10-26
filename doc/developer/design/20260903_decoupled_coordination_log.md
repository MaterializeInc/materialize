# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3

Implementer session: `2026-09-14-13-05-31-256`. Bookmark:
`decoupled-coordination`, based on `d9a2c5dc40`. Keep that upstream base pinned.
M2 is closed within its approved scope, including the 60s publication default.
M3 slice 1 is in progress. A standalone `pubsubd` reuses the existing relay,
with unified-image packaging and pinned envelope compatibility tests. Candidate
`50a0d4dd` is not yet pushed to the working bookmark. Compilation, all-target
Clippy, 11 RPC boundary tests and workspace manifest lint pass. The packaged
daemon starts without a catalog or backend connection and serves liveness and
metrics. Evidence is preserved in `~/.local/share/aj/m3-evidence/`.
No adapter has been moved to remote pubsub yet, and the one-default-adapter slice
is not complete.

Follow the [M3 slices](20260903_decoupled_coordination.md#3-independent-adapters-and-process-management).
Begin with one default adapter: deployment-scoped controllerd, join-only adapterd,
and externally managed, environment-scoped pubsubd. Establish adapter identity and
separate initialization from join without moving shared database duties into
controllerd. The first working slice must preserve native maintained execution
and leave healthy children running across controller restart.

Then make shared state correct for independent adapters, expose SQL/HTTP-managed
lifecycle, and complete deployment/backend integration. The decisive demo creates
B through A, uses both, kills A, and restores it through controllerd while B keeps
serving. Also demonstrate dropping all adapters and recovering SQL through
controllerd's HTTP list/create/drop API, without reinitializing the environment.
This is not complete with two manually launched SQL processes.

Next, establish the default adapter declaration and explicit realization identity,
then integrate remote clients with the controller-managed adapter.
The worker-generated Cargo.lock delta is preserved and applied locally, with no
dependency version changes. Initialization/join boundary research is saved in
`~/.local/share/aj/m3-evidence/initialization-join-boundary.md`.
Catalog/table initialization must be finite and separate from own-build planning
and shared maintenance. The controller's local restart must not repeat a global
system-table reset or treat a handle-local bootstrap flag as durable readiness.

The local backend now supports retained supervision separately from explicit
service deletion (`bbe00aba`). Four boundary tests, two existing in-runtime
restart regressions, and all-target Clippy for the backend and both caller crates
pass on scratch. Focused ownership review found a cancellable PID-file write,
which is corrected. Existing callers retain coupled shutdown. Use
`bin/cargo-test` for environmentd tests: direct nextest omitted the repository
runner's standard stack setting and overflowed during catalog startup.

Process provisioning research is saved alongside it in
`process-provisioning-boundary.md`. PID-only adoption and controller-owned TCP
proxies still need correction before claiming controller restart continuity.
Extract the existing service-spec/backend boundary, not the whole storage/compute
controller. Adapter SQL endpoints must remain usable independently of controllerd.
The declaration/identity map is in `adapter-declaration-boundary.md`.
Reuse existing execution replacement and retirement, with exact adapter identity,
and create the default declaration only in fresh initialization. Do not infer
initialization from an empty adapter inventory or silently convert v95 state.

## Concrete starting points

- `src/catalog/src/durable.rs` separates join from initialization/promotion.
  `Catalog::open_committed` in `src/catalog/src/catalog.rs` reconstructs initialized
  same-version state. Neither is a complete deployment initializer. Own-build
  plans remain adapter work, not a reason to embed a coordinator in controllerd.
- `src/adapter/src/coord/read_protection.rs::adapter_execution_predecessor` and
  durable `ClientIncarnation` still treat the adapter as a deployment singleton.
  `src/adapter/src/coord.rs::bootstrap_tables` resets shared system-table contents.
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
