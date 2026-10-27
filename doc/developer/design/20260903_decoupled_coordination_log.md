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

- Independent endpoint/launch review found three issues, now corrected locally:
  launch argument credentials in INFO logs, a transient assignment-file deletion
  during queued lifecycle cleanup, and oversized Kubernetes port names. Tests
  now cover secret omission, concurrent discovery and long listener names.
  Backend port names use bounded ordinals with lookup by configured listener name.
  These fixes are formatted but uncompiled. See `adapter-endpoint-review.md`.
- Adapter launch specifications now live in
  `src/environmentd/src/controllerd/provisioning.rs`: stable deployment-qualified
  identity, declared CPU/memory allocation, single-process scale, listener policy
  preserved with backend-assigned TCP addresses, and external pubsub. Tests cover
  listener policy, resources and rename-stable arguments. This new module is
  formatted but uncompiled. It does not yet implement controllerd startup.
- Source snapshot `bae88fe9` contains the joined-serving, retained-TCP, launch
  specification and metadata-freshness work. It is a draft verification snapshot,
  not verified M3 acceptance. Scratch SSO remains expired. CI140882 finished, so
  the next draft-PR run can verify this combined source without canceling prior
  work. All background tasks and subagents have finished.
- The draft contains join-only adapterd and retained direct-TCP endpoint support.
  Aljoscha approved hint-first local allocation: try the default port, otherwise
  discover a free one, and retain it across controller restarts. The backend uses
  `ServicePort.requires_tcp`, atomic assignment files beside PID metadata and
  existing process adoption. No proxy or new catalog records. Independent worker
  implemented backend code/tests. Parent inspected the diff. Formatting passes,
  but backend builds/tests have not run. See `controllerd-local-endpoints-decision.md`.
- Join-only adapterd passed combined catalog/environmentd/sqllogictest all-target
  Clippy and the admission/certification regressions. Removing the admitted-member
  guard made its regression fail at the expected assertion, then restoration
  passed. Matching environmentd, adapterd and clusterd product builds passed at
  `e599a09c`. Evidence: `adapterd-positive-checks.log`,
  `adapterd-admission-negative.log`, worker `adapterd-admission-restored.log`,
  `adapterd-environmentd-build.log`, `adapterd-product-build.log`.
  Public serve enforces process restrictions even for embedded callers. Joiners
  leave shared configuration rows unchanged. No promotion task runs in adapterd.
- Real takeover smoke has NOT passed end-to-end. Run `46105ed7` exited during
  table-writer initialization on CatalogOutOfSync while replicas published updates.
  Local finite metadata initialization now refreshes/retries at its natural catalog
  boundary, with strengthened test_multi_subscriber_catalog. That fix is uncompiled.
  Exact smoke repeat `accc2af1` successfully joined adapterd, preserved replica PIDs
  and shared configuration, queried an existing MV and resumed WAL writes. It then
  timed out on a newly created MV with pending_plans={User(513)}. Development
  environmentd and adapterd use different executable build IDs for written-plan
  namespaces. This is the leading fixture hypothesis, not yet verified against
  actual binaries. Do not weaken the witness or namespace safety to pass it.
  Existing --version output now includes written-plan-build for provisioner use.
  Adapterd also accepts only --process=0, as injected by the local backend.
  These latest CLI changes are uncompiled. Fixture: `adapterd-join-smoke.py`.
  Both failed fixtures cleaned up successfully. Worker run directories preserve logs.
- Next check after access returns: verify actual binary plan namespaces and the
  strengthened catalog regression, including negative control. Build/check the
  combined backend changes and run its new lifetime tests. Then wire controllerd
  finite initialization and launch using the actual adapter planning namespace.
  Do not sync a mixed in-progress snapshot. All subagents have finished.
- Next main outcome: one controllerd-managed default adapterd with external pubsub.
  `controllerd-extraction-map.md` identifies existing launch/backend seams and the
  coordinator-bound managed-policy context to move. Preserve the existing replica
  policy and deployment-local readiness. Controllerd must not contain a coordinator
  or shared maintenance. Independent adapters, SQL/HTTP lifecycle and complete
  backend/deployment integration remain required.
- Finite table completion and local adoption are committed in `f2de04ad`.
  Writable native startup initializes storage metadata and completes certified WAL
  registration before constructing local controllers. It adopts routes/tidy and
  uses the existing committer to complete later bootstrap WAL time without duplicate
  registration. It recovers committed CREATE/ALTER whose originating implications
  never ran. Legacy/read-only startup is unchanged.
  Strict adapter all-target Clippy, formatting and two boundary regressions pass.
  Both regressions fail for their expected bug when their respective fixes are
  removed, then pass after restoration. Rebuilt environmentd/clusterd pass SQL
  startup, CREATE/write/ALTER, two restarts with preserved rows and resumed writes,
  a maintained query and DROP with external pubsub. Concurrent DROP/Forget still
  needs multi-adapter end-to-end coverage. Evidence:
  `finite-table-final-verified.log`, `finite-table-product-smoke.tar.gz`,
  `finite-table-alter-recovery-negative.log`, `finite-table-wiring-review.md`.
- Runtime registration certification is in `05a4c836`, with its writable-mode
  guard and fresh-prefix regression in `d23adca2`. Certification fetches current
  mappings after each oracle allocation, including cached no-op fences and retries.
  Preserve CREATE completion, ALTER protection and write ordering. See
  `table-registration-certification-decision.md` and
  `finite-table-drop-fence-boundary.md`. Conflict-exhaustion halts and append
  membership assertions remain adjacent M3 gaps, not repaired by finite startup.
- Native current-health ownership, catalog-derived paused placement and unknown
  fallback are implemented. Successful committers alone publish native
  Paused/Dropped history. Completed sinks recover current running from terminal
  Persist output without fabricated event time or historical replay. Conditional
  Persist retirement removes predecessor status/session rows while preserving
  successor rows. The strengthened constant-sink/session restart witness and the
  full original Cluster 4 shard pass locally under the retained 8-CPU/16-GiB budget
  and unchanged deadlines. The witness correction is `98df7ff1`: SQL session UUIDs
  come from `mz_sessions` via `pg_backend_pid()`, not process-scoped `mz_session_id()`.
  Evidence: `ci140847/session-fixture/{exact-repeat,fixed,full-fixed}.log`,
  `retirement-eligibility/`, `retired-rows/`, `session-cleanup-ownership-review.md`.
- CI140876 on `0324369e` finished. Its Linux Clippy failure was reproduced:
  two assertion unwraps in the certification test. Verified repair `b9755c93` is
  published. CI140882 finished: Clippy/Cargo tests passed, Cluster 1, SLT 4
  and 0dt smoke failed. Adapterd/backend verification snapshot follows this repair. CI140876 also failed macOS Clippy, SLT 4 and 0dt smoke, not yet fully
  inspected. Merge skew remains excluded. Logs: `ci140876/build.json`, `clippy.log`.
  Prior CI140847 Testdrive 4 passes unavailable-health expectations. Cluster 4's
  session-ID fixture is corrected and its full shard passed locally. Cluster 1's
  MV-frontier failure and two SLT 4 status EXPLAIN mismatches remain unresolved.
  Upgrade conversion remains excluded. Prior logs: `ci140847/README.txt`.
- External pubsub, fresh-only shared seeding, active configuration reconciliation,
  durable-only credential publication and execution-qualified session/subscription
  ownership have focused verification. Combined independent-adapter concurrency,
  promotion and retirement remain open. Native recovery/promotion, parallel and
  retention regressions remain enabled. The cluster-thread assertion propagation
  gap remains unresolved. Do not count checkout failures or unrerun corrections.
- Worker `i-0788036f0778687f1` is clean on `e599a09c`, with no task left running.
  Scratch SSO expired. Aljoscha must run `bin/scratch login`; request already sent.
  Do not replace the worker or block all implementation on that access issue.
  Test codegen exhausted disk and exact retry could not start. Audited inactive
  dependency incremental caches reclaimed about 15 GiB, preserving changed-crate
  caches, compiled products and fixtures. Tests and product builds then passed.
  Last free space was about 9 GiB during product linking. Evidence:
  `adapterd-inactive-cache-audit.txt`, `adapterd-inactive-cache-reclamation.log`.
  Use combined `-p mz-catalog -p mz-adapter --lib` for focused catalog/adapter tests.
  Local formatting: `RUSTUP_TOOLCHAIN=1.98.1 bin/fmt`.

Evidence is under `~/.local/share/aj/m3-evidence/`. Completed foundation pointers
are in `foundation-handoff-detail.md` and `pre-status-closeout-handoff.md`.
`session-publication-boundary.md` describes session cleanup. Broad combined M3
verification remains open. The standard SLT runner uses the ignored
`target/debug -> ../target-xcompile/debug` alias.

Scratch access improves iteration but must not stall it for extended SSO outages.
Use local verification within available disk/memory or draft-PR CI as a fallback.

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
