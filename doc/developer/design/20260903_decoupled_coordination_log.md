# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3 runtime acceptance

Sole implementer: `2026-10-27-11-01-36-080`. Bookmark: `decoupled-coordination`,
based on pinned `d9a2c5dc40`. M2 remains closed, including the 60s publication
default. Published implementation head is `6dbd009d`. M3 is not complete.

## Resume here

Default debug revision `888e822f` passed the real `bin/controllerd` witness:
managed relay initialization, maintained SQL, Ctrl-C and unchanged adoption,
relay-only executable replacement with retained endpoint/SQL child PIDs/session,
whole-environment stop after controller exit, and restart retaining SQL data.
Cleanup succeeded. The build through `bin/controllerd --build-only` took 42m33s.
Evidence is preserved off-worker in `public-launcher-888e822f.tgz`, including
`launcher-5adaafebc6e8413bac8b3763148bf52c/results.json` and the executed witness.
This does not cover the later default-reset helper.

The two-adapter SQL/HTTP lifecycle witness on the same debug revision failed
once, then passed unchanged. The failed run reached adapter replacement before
both adapters panicked at `src/txn-wal/src/txn_cache.rs:595`: a retained write was
older than the global minimum unapplied timestamp. Restarts repeated the panic.
Do not dismiss this as a deadline or weaken the assertion. A two-shard reduction reproduces it twice in an extracted std-only harness
(`/tmp/txn-cache-repro/repro.rs`), not a Cargo test. The cache file is identical
to pinned upstream. Per-shard cleanup leaves another shard behind the advancing
global minimum, violating global compaction validation without losing writes in
the reduction. Oracle review of the shared invariant/repair is running. No
production changes or assertion weakening have been made.
Both runs are preserved in `debug-lifecycle-888e822f.tgz`:
`run-93d7927e3ac54f4597f18d7b4a8f6feb` failed,
`run-04119edf999540daafc2b286ccba5fc7` passed.
The passing run covers protocol-named HTTP endpoints. It does not erase the
intermittent failure.

Aljoscha reported controller exit after `pkill adapterd`. The supplied exit241
indicates SIGTERM, not a panic. Full-command matching would also match
controllerd's `--adapterd-image` argument, but that explanation is unconfirmed.
The real launcher witness passed plain `/usr/bin/pkill adapterd`, after verifying
all matches belonged to its disposable environment. Controllerd survived, replaced
only its adapter, and retained replica PIDs and maintained data. Evidence and
cleanup success are preserved in `public-launcher-pkill-888e822f.tgz`, run
`launcher-7d1c815c44d4461f89c5afd7fdae504f`. The local report remains unexplained.

Next acceptance: concurrent adapter replica policy, interrupted initialization,
compatible old/new deployment coexistence and the executed user-facing upgrade
walkthrough in [the developer guide](../controllerd.md), then remaining backend
and broad regression gates. Agent34 identified the existing ON REFRESH
empty/one/empty regression in `test/testdrive/cluster-controller.td:1033–1117`:
run with both adapters, resize across endpoints, then repeat new transitions with
each adapter as sole survivor. Preserve default policy ticks and publication
cadence. Single-active-deployment status queries do not prove prewarming isolation.

## Implemented contracts and verification

Adapter policy remains deployment-local and catalog-validated, enabled separately
from provisioning ownership (`bc10afcb`). No designated policy leader. Controllerd
initializes deployments, enacts committed process inventory and serves the simple
unauthenticated administrative listener. It does not own replica policy.

SQL CREATE/DROP/RENAME require superuser even with optional RBAC disabled. HTTP
uses the same validated catalog operations. Deployment-qualified `AdapterRuntime`
observations expose named endpoints, backend status and separate readiness.
Drop/retirement clean them up. They do not invalidate planning or publish builtin
rows. Late readiness results check target/revision. Reviews approved authority,
fencing and ownership. Parser, catalog lifecycle/runtime/promotion/upgrade tests
and strict combined Clippy passed. Some HTTP unit tests and updated SLT/testdrive
regressions still await execution.

The CI141217 candidate `969056d5` passed all24 SQL/HTTP lifecycle checks, including
paused-peer catch-up, superuser enforcement, independent sessions, replacement,
controller restarts, collection filters, zero-adapter restart without recreation,
and HTTP recovery of maintained data. Its external-relay evidence is in
`run-4dc771671d034ee8a0a142f051818c1d/`. The debug rerun above adds named-endpoint
coverage but exposes the open transaction-cache failure.

`GET /api/collections` returns catalog-committed combined since, with SQL LIKE
id/name filters. No physical Persist inspection or fallback. The lifecycle
witness covers the endpoint. Strict environmentd Clippy passes.

Plain `bin/controllerd` provisions pubsub from static configuration, records it
in inventory, and passes its endpoint to adapters/replicas. Explicit pubsub URL
selects external mode without adoption or readiness gating. Only managed pubsub
opts into executable-change replacement and Kubernetes owner-reference exclusion.
Adapters/replicas and argument/settings changes do not trigger native replacement.
Qualified image sidecars preserve safe unchanged adoption and stable endpoints.
Unknown legacy identities remain adopted. Eleven process-library tests and six
Kubernetes library tests pass, plus strict Clippy. Runtime Kubernetes acceptance
remains open. Keep the standalone image publishable.

`--stop` is scoped, idempotent and data-preserving without rebuilding. Ctrl-C
stops only controllerd. PID/start identities and recorded backend locations keep
unrelated environments untouched. Real launcher stop/restart now passes.
`--reset`, explicitly requested after deletion of local state, accepts only the
default local database/state/blob targets. It stops recoverable associated
processes before dropping the three metadata schemas and local directory, rejects
live foreign-worktree database users, and preserves files on SQL failure.
Independent review findings are resolved. All29 launcher tests, Black and Ruff
pass, including real disposable process checks. SQL reset itself has not been
exercised end-to-end against the default database. No user database was reset.

The catch-up repair (`d6f662f7`) applies/enacts each transaction against its
matching projection. Review, strict Clippy, multi-subscriber regression and the
full catalog suite passed (396 passed, two ignored). Evidence:
`prefix-review.md`, `prefix-reviewed-{test,clippy}.log`, `prefix-catalog-suite.log`.

## CI and worker

CI141235 for `6dbd009d` is terminal failed. Failed jobs are x86 product
`01a182a6-884a-4a85-a0a7-335a8a2a494a` and merge skew. The current product log confirms
`pubsubd` is not public on Docker Hub. Merge-skew details remain uninspected. CI141217 built products but failed the
public Docker Hub visibility gate for `materialize/pubsubd`; a registry owner
must resolve it without bypassing the gate. Its pinned-base merge-skew job also
failed. Cargo/downstream integration did not run. Broad CI is not green.

Worker `i-0788036f0778687f1` is pinned at `888e822f`, with default debug binaries
and about17GiB free after building. Scratch access works. Last expiry check showed
five hours remaining. Task120 completed successfully. Check for new work before syncing source. Exact older CI product:
`materialize/materialized:mzbuild-FJXSGTKPCPESYSV4SG3CJ3LIKLBPF5GP`, digest
`sha256:6d4cb1d4f5065426e5c80fe7a813a663620cc123b9222baafd7bbebf7eca7f51`,
extracted at `/home/ubuntu/m3-evidence/ci141217-product`. It supplies the distinct
relay executable for replacement acceptance, not green CI.

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
Scratch access is restored. Keep local checks lightweight and use draft-PR CI
for broad verification.
Keep `i-01023ce0e1db179f9` and `m2-pause-test-postgres` untouched.

Use `bin/ci-builder run stable <command>` and `bin/cargo-test`, forwarding `--lib`
after `--`. Local formatting uses `RUSTUP_TOOLCHAIN=1.98.1 bin/fmt`. Watch disk
headroom during codegen/linking and preserve useful evidence before cleanup.
Worker logs live in `~/m3-evidence/` and must be copied off-worker. Its
`native-metadata-url.txt` and `native-postgres-url.txt` supply fixture URLs.
`mzrepro.slice` is the aggregate 8-CPU/16-GiB workload budget, not the build budget
or a faithful CI-host reproduction.
