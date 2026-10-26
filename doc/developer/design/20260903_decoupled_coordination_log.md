# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3

Implementer session: `2026-09-14-13-05-31-256`. Bookmark:
`decoupled-coordination`, based on pinned `d9a2c5dc40`. M2 remains closed,
including the 60s default. M3 slice 1 is incomplete. External pubsub works in
opt-in environmentd mode, but controllerd/adapterd have not been implemented.

The foundation includes standalone pubsubd, retained local supervision, replica
launch specifications, named adapter declarations and catalog join without shared
initialization. Runtime catalog projection publication is originator-only, and
bootstrap snapshots are distinguishable from originating transaction rows.
These components have focused verification, not combined M3 acceptance.

## Resume here

- Native status ownership is implemented and locally verified. One private current
  relation is independent of unchanged history schemas and retention. Replica
  receive-time fencing and attempt-qualified retirement reject late predecessors.
  Catalog placement supplies paused health. Only successful committers publish
  native catalog-driven Paused/Dropped history. Followers retain local effects,
  join/restart emits no lifecycle history, and legacy reconstruction is unchanged.
  Strict Clippy, formatting, real binaries, 33 focused tests, native SQL lifecycle
  smoke and 534 assertions in seven existing catalog SLTs pass. Broad integration
  and physical retired-row cleanup remain open. See `current-health-simple-verified.tar.gz`,
  `current-health-final-catalog-verified.tar.gz`, `current-health-fence-followup.md`
  and `lifecycle-originator-status-review.md` with its disposition.
- Continue the finite table-initialization boundary, then controllerd/adapterd.
  `finite-table-initializer-next-boundary.md` is research, not implementation.
  `c03c2a0e` establishes finite storage metadata, not txn-WAL readiness or complete
  controller startup. `TxnsHandle::adopt` is tested but not wired into startup.
  Adoption alone does not register/advance the WAL or establish write authority.
- External pubsub is verified in `6798d042`, including protected SQL, outage and
  reconnect. Shared bootstrap snapshots are fresh-only. Active configuration
  reconciliation and durable-only credential publication have focused verification,
  but combined multi-adapter promotion/concurrency remains open. Session/subscription
  rows are execution-qualified and filtered, with physical cleanup still missing.
- Latest recorded broad integration is CI140687 at `b2ea75aa`: 90 checks passed,
  testdrive 6 failed on six missing Starting history rows after replica recreation.
  The original 37-file shard and focused status-history run pass on scratch under
  the aggregate 8-CPU/16-GiB slice. Replica-owned publication closes the demonstrated
  pre-subscription loss window, but this was not trace-proven as the CI cause.
  Rerun broad integration for the status slice rather than claiming that failure
  resolved. The preceding cumulative slowdown remains unexplained.
  Console/prod failed against production cloud services, not this branch's server.
  See `ci140687/failures.md` and `ci140687/starting-history-owner.md`.
- Native recovery/promotion, parallel, retention and corrected realization probes
  passed CI140661. Upgrade conversion and merge skew stay excluded. Cluster-thread
  assertions do not propagate to callers, an unresolved harness gap. Do not count
  checkout failures or unrerun corrections as passing.

Evidence is under `~/.local/share/aj/m3-evidence/`. Completed foundation pointers
are in `foundation-handoff-detail.md` and `pre-status-closeout-handoff.md`.
`session-publication-boundary.md` describes session cleanup. Broad combined M3
verification remains open. Scratch retains generated golden edits already copied
and preserved here. Check/preserve them before another push. Its standard SLT
runner uses the ignored `target/debug -> ../target-xcompile/debug` alias.

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

Shared `mz_role_auth` publishes only durable credentials. Process-local password
overrides still authenticate. Webhooks are environment-owned without creator
affinity. Their common advertised host, including port, is fixed across adapters
and deployments for M3. Without public ingress, keep the existing URL placeholder
and use discovered adapter HTTP addresses directly. Do not add a local proxy or
controllerd forwarding. Host changes are deferred, and eventual URL/view conversion
belongs with the colleague's work.

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
