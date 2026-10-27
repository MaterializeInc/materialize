# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3 independent adapters

Sole implementer: `2026-10-27-11-01-36-080`. Bookmark: `decoupled-coordination`,
based on pinned `d9a2c5dc40`. M2 is closed, including the 60s publication default.
The controller-managed default-adapter startup/restart demonstration passes.
Independent adapters, SQL/HTTP lifecycle, replica-policy integration,
zero-adapter recovery, interrupted initialization and deployment/backend
acceptance remain open. Broad CI is not green.

## Resume here

Continue the controller-managed two-adapter demonstration, then SQL/HTTP lifecycle.
An intermediate fixture may create declarations with the existing catalog-admin
tool, but does not replace SQL/HTTP acceptance. The first two-adapter attempt hit
Postgres's default 100-connection limit before adding the declaration. Match the
repository fixture's 5,000-connection setting and rerun without product tuning.

Keep cluster-controller policy in adapterd, including declaration realization,
burst scaling, reconfiguration and ON REFRESH scheduling. Controllerd only enacts
committed process inventory, alongside its initialization and administrative API
responsibilities. Separate policy enablement from `manages_replica_processes` in
adapter task startup and declaration reconciliation. Preserve deployment-local
observations and catalog precondition checks under concurrent adapters, without
a designated policy adapter. With zero adapters, policy may pause while
controllerd maintains committed processes.

The controller HTTP API is an unauthenticated port. Authentication and access
control belong to the external system exposing it. Keep catalog validation and
deployment fencing, but add no in-process HTTP auth machinery. SQL adapter
management is superuser-only.

The recovery bookmark `recovery/concurrent-session-20261027` is not an
implementation baseline. Its policy/observation extraction must not be resumed
as a controller-owned policy migration. The older extraction reports are not
ownership guidance. Follow the current design.

## Verified outcomes and open checks

- CI140930 binaries at `e15ac489` passed the managed startup/restart witness in
  `run-0b7e58f7823243d5aac2742e69e3bf2a/`. Two controller restarts preserved the
  adapter and four replica PIDs, SQL endpoint, held connection and temporary view.
  Writes and maintained reads continued without controllerd. Restart applied
  changed replica-size credits. New MV creation/query and table DROP succeeded.
- CI140930 passed Clippy, native adapter-loss, SLT2 and the active/pending-generation
  configuration test. Cargo reported 4,326 passes and the CLI fixture failure
  corrected in `f98c1a23`. Verify that fix in the next combined run. Refresh CI
  status before publishing and preserve any in-flight verification.
- CI140921's pushdown snapshot failure was not reproduced in four focused runs
  with its exact binary and service environment. See `slt2/`. This does not close
  the parallel-shard failure. Earlier MV-frontier, status EXPLAIN and 0dt failures
  remain unclosed unless demonstrated by a newer run. The cluster-thread assertion
  propagation gap also remains open. Do not weaken assertions or deadlines.

## Boundaries and remaining risks

Replacement builtin-table migration recovery belongs with the affected upgrade
path. A transient replacement set can disappear after migration commit, and a
non-MIN shard upper does not prove population. Do not admit incomplete backfill
or infer upgrade support from the fresh-start demo. See
`replacement-backfill-review.md`.

Finite table initialization and adoption have focused and rebuilt restart
coverage. Runtime registration certifies current mappings after oracle
allocation, including retries. Preserve CREATE completion, ALTER protection and
definitive write ordering. Concurrent DROP/Forget still needs multi-adapter
coverage. Conflict-exhaustion halts and append-membership assertions remain open
integration risks. See `finite-table-final-verified.log`,
`table-registration-certification-decision.md` and
`finite-table-drop-fence-boundary.md`.

The [projection limitation](20260903_decoupled_coordination.md#shared-maintenance-and-introspection)
permits originator-only publication and persistent inconsistency only for the
specified derived projections and lifecycle history. Followers still enact
catalog effects. A colleague owns catalog-backed conversion. Do not add a
publication watermark or recovery protocol.

Configuration relations describe the active deployment before readiness.
Prewarming and retired deployments cannot overwrite them. Planning/provisioning
use deployment-local configuration. Joined adapter startup does not publish
shared configuration. Keep shared-infrastructure initialization and configuration
reconciliation finite, without SQL coordination or shared maintenance.

Shared `mz_role_auth` publishes durable credentials only. Process-local overrides
still authenticate. Webhooks are environment-owned, without creator affinity.
Their common advertised authority, including port, is fixed for M3. Without
shared ingress, retain the URL placeholder and use discovered adapter HTTP
addresses directly. No controller proxy or public-host redesign.

Preserve replica-owned current health, catalog-derived paused placement and
unknown fallback. Completed sinks retain ordinary terminal-output recovery.
Retired-row cleanup must preserve successors. Existing focused and Cluster 4
coverage is in `ci140847/session-fixture/`, `retired-rows/` and
`session-cleanup-ownership-review.md`.

M2 closeout evidence is at `676346d6`. Keep regular regressions enabled, not a new
M2 campaign. Explicit-DDL admission belongs to Aljoscha's separate work. There is
no approved M2-to-M3 or existing-environment conversion. Unsupported-version
interoperation, managed replica-targeted MV handover and query-relative public
replica visibility remain excluded. The deferred
`m2-deferred-orchestratord-retirement` work is not an approved M3 solution.

## Evidence and verification resources

Evidence is under `~/.local/share/aj/m3-evidence/`. Completed foundation pointers
are in `foundation-handoff-detail.md` and `pre-status-closeout-handoff.md`.
The standard SLT runner uses the ignored
`target/debug -> ../target-xcompile/debug` alias.

Worker: `i-0788036f0778687f1`, m8i.4xlarge, 16 vCPUs, 64 GiB RAM, 500 GiB disk.
Last verified expiry: 2026-10-28 19:01:27 UTC. Check status, source revision and
live tasks before reuse, and extend expiry under the prompt's authorization.
Scratch SSO needs renewal. Do not replace the worker or stall implementation:
local checks within available resources and draft-PR CI are approved fallbacks.
Keep `i-01023ce0e1db179f9` and `m2-pause-test-postgres` untouched.

Use `bin/ci-builder run stable <command>` and `bin/cargo-test`, forwarding `--lib`
after `--`. Local formatting uses `RUSTUP_TOOLCHAIN=1.98.1 bin/fmt`. Watch disk
headroom during codegen/linking and preserve useful evidence before cleanup.
Worker logs live in `~/m3-evidence/` and must be copied off-worker. Its
`native-metadata-url.txt` and `native-postgres-url.txt` supply fixture URLs.
`mzrepro.slice` is the aggregate 8-CPU/16-GiB workload budget, not the build budget
or a faithful CI-host reproduction.
