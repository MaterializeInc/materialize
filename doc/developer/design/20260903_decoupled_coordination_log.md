# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3 SQL/HTTP lifecycle

Sole implementer: `2026-10-27-11-01-36-080`. Bookmark: `decoupled-coordination`,
based on pinned `d9a2c5dc40`. M2 is closed, including the 60s publication default.
The controller-managed two-adapter demonstration passes. SQL/HTTP lifecycle,
replica-policy integration, zero-adapter recovery, interrupted initialization and
deployment/backend acceptance remain open. Broad CI is not green.

## Resume here

Continue the local SQL/HTTP lifecycle work. Uncommitted changes implement
CREATE/DROP/RENAME ADAPTER parsing, planning, shared catalog mutations, response
handling and mandatory superuser checks, including when optional RBAC is disabled.
Configured size names are forwarded to adapters for shared size validation.
Combined adapter/environmentd/sqllogictest all-target checks pass before the latest
`mz_adapters` raw-catalog projection was added. Check that projection, rerun parser
tests after switching to the standard unreserved keyword, add catalog and
authorization boundary coverage, and run strict Clippy.

Controller HTTP administration and deployment-qualified endpoint/status publication
remain to implement. Use the same catalog ops as SQL. Durable content commit already
allocates and completes through the oracle in `durable/persist.rs`. Preserve that
completion boundary. HTTP is unauthenticated, with access control outside the
process. Do not restore controller-owned policy extraction from recovery history.

The catch-up repair is published as `d6f662f7` in CI141032. It applies and enacts
one transaction at a time against its matching projection, invalidating snapshots
before enactment. Independent review approved it. Formatting, strict catalog/adapter
lib/test Clippy, the multi-subscriber regression and the full catalog suite pass
(396 passed, two ignored). Evidence: `prefix-review.md`,
`prefix-reviewed-{test,clippy}.log`, `prefix-catalog-suite.log`.

Exact CI141032 binaries passed `two-managed-adapters-smoke.py` in
`run-3b18bc7440e544cf8e1fd1045d5a0576/`. It verifies paused-peer CREATE/ALTER/DROP
catch-up, cross-adapter DDL/writes, temporary-view isolation, replacement of only
the failed adapter, and two controller restarts preserving child PIDs, sessions
and maintained reads. Cleanup completed without errors. The witness source and
product image digests/hashes are retained in that directory. Binaries are in
`ci141032-product/`. The intermediate declaration uses the catalog-admin fixture,
not the required lifecycle API. Postgres used 5000 connections and a 768 MiB limit.

Policy task enablement is separate from process ownership in `bc10afcb`. Keep
policy deployment-local and catalog-validated across adapters, without a leader.
With zero adapters, policy may pause while controllerd maintains committed processes.

Published head: `d6f662f7`, CI141032. Preserve in-flight verification before another
push. CI140970 reached terminal failure before this publication. Its Cargo and
Clippy jobs passed. Earlier parallel SLT and upgrade failures remain unclosed.
The focused SLT2 runs in `slt2/` did not establish the failure as a baseline flake.

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
