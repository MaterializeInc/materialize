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

The foundation includes standalone pubsubd, retained local supervision, replica
launch specifications, named adapter declarations and catalog join without shared
initialization. Runtime catalog projection publication is originator-only, and
bootstrap snapshots are distinguishable from originating transaction rows.
These components have focused verification, not combined M3 acceptance.

## Resume here

- Classify CI140661 at `ec80584b`: SLT 1–4 and Testdrive 4/6 remain unresolved,
  separate from excluded upgrade conversion and merge skew. It does not cover
  the configuration reconciliation in `a1f90e7e`. Corrected CI140586 fixtures still
  need workload reruns. Cluster-thread assertions do not propagate to callers,
  an unresolved harness gap. Do not count checkout failures or unrerun corrections
  as passing.
- Configuration reconciliation before readiness is committed in `a1f90e7e`.
  Focused startup, restart and prewarming checks, formatting and Clippy pass.
  Complete multi-adapter promotion/concurrency verification remains open.
- Finish removing shared join/restart resets and positive snapshot replay in
  `src/adapter/src/coord.rs::bootstrap_tables`, preserving legitimate originating
  bootstrap diffs. `TxnsHandle::adopt` exists and is tested but is not wired into
  adapter startup. Local adoption does not register or advance the WAL, or establish
  readiness/write authority.
- Session/subscription rows are execution-qualified and public views filter live
  incarnations. Existing SQL and bootstrap-progress tests pass, but retired-row
  cleanup and runtime concurrency coverage remain missing.

Evidence is under `~/.local/share/aj/m3-evidence/`. Start with `ci140661/` and
`ci140586/current-failure-classification.md` for CI, `config-bootstrap-local-verification.log`
and `config-bootstrap-cas-verification.log` for readiness, and
`session-publication-boundary.md` for session cleanup. `foundation-handoff-detail.md`
holds completed foundation verification pointers. Broad combined verification is
still open.

## Boundaries to preserve

The [M3 projection limitation](20260903_decoupled_coordination.md#shared-maintenance-and-introspection)
permits unordered originator-only publication and persistent inconsistency after
publication failure only for those derived projections. Followers still enact
catalog effects. A colleague owns conversion of these projections to catalog-backed
views. Do not take over that work or add a publication watermark or recovery protocol.

Configuration relations must describe the active deployment before readiness.
Any active adapter may transactionally reconcile them, but prewarming and retired
deployments cannot overwrite them. Internal planning/provisioning retain their
deployment's configuration. Existing bounded bootstrap retries are approved for
the expected small adapter population. This is separate from projection
inconsistency and session cleanup.

Initialization and table adoption must remain finite, without a coordinator or
shared maintenance in controllerd. Existing WAL registration/application can
recover without a new initialization marker. See `table-initialization-recovery.md`
and `initialization-join-boundary.md` in the evidence directory. Continue process
extraction under the [M3 slices](20260903_decoupled_coordination.md#3-independent-adapters-and-process-management),
preserving existing replica burst/reconfiguration policy rather than redesigning
it or expanding its acceptance campaign.

M2 closeout and its evidence pointers are at `676346d6`. Keep those proof scopes
closed and regular regressions enabled, not a new M3 backlog. The explicit-DDL
admission correction belongs to Aljoscha's separate work. Do not suppress
structural revision checks or replay acknowledged DDL. Raise an actual M3
dependency before pulling that work in or changing the pinned base.

There is no approved M2-to-M3 conversion. Existing-environment conversion,
unsupported-version interoperation, managed replica-targeted MV handover and
query-relative public replica visibility remain excluded. The deferred draft at
`m2-deferred-orchestratord-retirement` is not an approved M3 solution.

## Worker

Use `i-0788036f0778687f1`, m8i.4xlarge, 16 vCPUs, 64 GiB RAM and 500 GiB disk.
Last verified expiry: 2026-10-28 19:01:27 UTC. Check status and extend expiry as
needed under the prompt's existing authorization. The traffic investigation's
`i-01023ce0e1db179f9` belongs to another session. Leave it untouched.

Use `bin/ci-builder run stable <command>` and `bin/cargo-test` for pinned tooling
and standard stack settings, forwarding `--lib` after `--`. Scratch logs live in
`~/m3-evidence/` and must be copied off-worker. Metadata and comparison Postgres
URLs are `native-metadata-url.txt` and `native-postgres-url.txt` in that directory.
The latter supplies `POSTGRES_URL`.

Iceberg and SLT fixtures are stopped, with data and evidence retained. Preserve
useful evidence before replacing fixture state. Leave `m2-pause-test-postgres`
untouched. `mzrepro.slice` is an aggregate 8-CPU/16-GiB workload budget, not the
build budget or a faithful CI-host reproduction.
