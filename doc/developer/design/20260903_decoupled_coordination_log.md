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

Lifecycle core (`c5838f31`), local launcher (`4515a0df`) and deployment-qualified
observations (`6bb0bfe4`) are implemented. SQL CREATE/DROP/RENAME require superuser
with optional RBAC disabled. HTTP adapter management uses the same validated
catalog operations, without in-process authentication, on port 6882 by default.
Both independent reviews approved authority, fencing and ownership boundaries.

`AdapterRuntime` stores change-only observations by adapter and generation.
Declaration drop and generation retirement clean up its rows. It does not
invalidate planning or publish builtin-table rows. SQL exposes named endpoints,
backend status and separately observed readiness. Readiness uses existing
unauthenticated health routes, with target/revision checks on late responses.
Kubernetes time-only events do not invalidate readiness. The branch-owned v96
schema and fixture were updated in place, with matching hashes verified.

Strict combined all-target Clippy passes for adapter, environmentd, sqllogictest
and catalog-debug after review fixes. Parser tests pass (76 passed, one ignored).
Catalog lifecycle, runtime authority/roundtrip/planning, promotion, multi-subscriber
and upgrade checks pass. New builtin descriptor validation passes. The removal of
unused transaction helpers was subsequently compile-checked. HTTP and observation
unit tests compile but have not executed. Updated SLT/testdrive catalog listings
and the RBAC-disabled denial regression also await execution.

The launcher currently requires an external relay. Wire default pubsub provisioning
into controllerd from static configuration, including HTTP inventory and retained
process adoption. Preserve the shared relay across deployment retirement, with
only one configured provisioner during overlap. No catalog declaration or election
protocol. Update the [local recipe](../controllerd.md) as that path becomes usable.
Add `bin/controllerd --stop` with the design's local environment scope, retaining
data and declarations. Verify it after controller exit, repeated stop and restart.
Keep Ctrl-C controller-only. The stop path must not build or initialize anything.
Fourteen scoped launcher Python tests, Black and Ruff pass. Actual startup/adoption
through plain `bin/controllerd` remains unverified. Default development builds must
work for demos, with `--optimized` optional as in `bin/environmentd`.
Scratch SSO is still expired. User must run
`bin/scratch login`. The user authorized local cleanup. Removed only the Rust
incremental cache after checking no builds were active, leaving 71 GiB free.
Keep product builds and broad codegen on the approved worker.

Next: obtain the latest candidate's matching CI product and run
`sql-http-adapter-lifecycle-smoke.py`, including self-drop, zero-adapter restart
and HTTP recovery. The private script now also checks SQL named endpoints,
backend status/readiness, dropped-observation cleanup, LIKE collection filters and
collection inspection after a zero-adapter restart. It is syntax-checked, not
executed, and requires a product containing the pending collection endpoint.
Separately exercise the public launcher and its managed pubsub path.
`GET /api/collections` is committed as `7c6e3651` with LIKE filters,
qualified names, unnamed entries and committed `collection_compaction_bounds`.
It follows designer commit `16dcb3c6`: no Persist inspection or fallback to physical
since. The earlier physical-shard implementation was removed, including its Persist
API changes. Browser/curl examples and filter/transport tests are added. Formatting
and whitespace checks pass. Strict environmentd all-target Clippy passes for the
final catalog-frontier implementation. Its HTTP and filter tests compile but have
not executed. The lifecycle witness needs a matching product.

CI141151 on published `9062a348` built both architectures. Rust tests stopped at
compilation: the catalog debug/open test formatters omitted `adapter_runtimes`,
and a new runtime test used a forbidden Option unwrap. The exact scoped command
`cargo clippy -p mz-catalog --all-targets -j 2 -- -D warnings` reproduced all three
and passes after the fixes. Both formatters include the field rather than ignoring
it. Both exact snapshot failures were reproduced, regenerated through the tests
and accepted with cargo-insta. The original debug/open tests now pass. Runtime
publication and authority/cleanup tests also pass (two tests). The full CI container
command and HTTP/readiness unit tests remain unexecuted.

Scratch authentication was rechecked and remains expired. Local disk pressure
was resolved by the authorized cache cleanup. Targeted compilation and snapshot
checks can resume locally. Replica policy, interrupted initialization, deployment
coexistence and backend acceptance remain open. No M3 completion or
new-product runtime claim is justified.

Evidence: `runtime-review-resolution.md`, `runtime-reviewed-clippy.log`,
`runtime-builtin-tests.log`, `lifecycle-review-resolution.md` and the earlier
lifecycle/catalog/parser check logs under the private evidence directory.

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

CI141032 is terminal failed, with no executing or queued jobs. Its product supplied
the prior two-adapter witness, not the lifecycle/status candidate. Inspect the latest
bookmark and CI before another publication. Preserve executing verification. Earlier
parallel SLT and upgrade failures remain unclosed. The focused SLT2 runs in `slt2/`
did not establish the failure as a baseline flake.

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
