# Decoupled coordination: current handoff

The [design](20260903_decoupled_coordination.md) owns contracts and scope.
The [prompt](20260903_decoupled_coordination_prompt.md) owns working rules.
The [PR](https://github.com/MaterializeInc/materialize/pull/38696) provides the
reviewer-facing summary. Replace this handoff as work advances, rather than
appending a work log. Detailed evidence stays outside these documents.

## Current work: M3 runtime acceptance

Sole implementer: `2026-10-27-11-01-36-080`. Bookmark: `decoupled-coordination`,
based on pinned `d9a2c5dc40`. M2 remains closed, including the 60s publication
default. Published implementation head is `34942b30` (metrics implementation `5b466d0c`). M3 is not complete.

## Resume here

M3 targets local-process deployments. Kubernetes backend/operator integration and
runtime acceptance are deferred, not milestone blockers. Preserve existing backend
work without expanding it. The executed local version-upgrade walkthrough remains
required, including prewarming, promotion, endpoint switching and retirement.

Replica resource sampling is restored at controllerd's process boundary in
`5b466d0c`. The existing 60s sampler is shared with legacy provisioning. Samples
retain capture time across delayed publication and write the existing,
deployment-qualified metrics history. Adapter retention and policy are unchanged.
Strict controller/environmentd all-target Clippy, 15 controller unit tests and
the default build pass. The public recovery witness also verifies fresh resource
observations timestamped strictly inside a 65s interval with no adapters.
Evidence: `replica-metrics-79a8b795.tgz`, run
`launcher-e0feb4c0c5184e39b8ec1527f740c6e3`, and
`metrics-verification-79a8b795.log`. Cleanup succeeded. Independent review's
sample-time finding was fixed before verification. Separate publisher startup
plumbing remains local to the two actors rather than adding a shared abstraction.

Aljoscha explicitly rejects privilege/configuration follower lag for serializable.
Preserve catalog freshness at every isolation level. Oracle review confirmed
request-scoped reuse is safe, but its suggested freshness relaxation is rejected.
The uncommitted performance repair carries a noncloneable StatementCatalog from
preparation into the same simple-query or HTTP statement execution. Later requests,
replans and SQL EXECUTE unrolling do not reuse it. Independent review43 found no
counterexample. Review44 found that local Bind validation would allow BindComplete followed by
DescribePortal to expose an obsolete description without Execute. That candidate
is rejected. Bind, Describe and extended Execute retain fresh certification.

The simple/HTTP regression passes on `af3d03f8`. Its earlier failures were HTTP
fixture authorization: the anonymous role needed creation and a SELECT grant.
Both failures were reproduced unchanged before repair. Assertions and production
RBAC were not weakened. The expanded regression also checks HTTP REVOKE and
retains independent Bind/Execute snapshot checks.

The rejected-Bind candidate's full pgwire suite passed 57/58 twice. COPY TO failed
because its configured MinIO endpoint localhost:40109 was absent. The unchanged
case passes after provisioning the existing cached repository MinIO image, with
the same copytos3 bucket setup as ci/test/cargo-test/mzcompose.py. Owned container:
`aj-m3-pgwire-minio`, ID9466434e4470, goal-labelled and loopback-only. Preserve the
other three PostgreSQL containers. One health probe initially hit a startup reset.
Repeating it succeeded without restarting MinIO.

Performance implementation is committed as `650352b5`. No Bind relaxation is
included. Strict all-target Clippy, expanded SQL/HTTP snapshot-count and revocation
regression, and the full 58-test pgwire suite pass on candidate `cc0f03b7`. The
public default build and cross-adapter first-attempt GRANT/REVOKE plus recovery
witness pass: `launcher-646539f1da5541339b8658b41452f6ee`. Optimized build completed
in 65m33s. Full output: `/tmp/m3-peek-final-clean-fixture.log` (task176, exit0).
Worker code matches the performance implementation. It predates only the separate
metrics formatting/test-attribute repair. That metric unit wrapper still needs a
rerun on the latest revision after access is restored.

Task178 attempted to start the optimized benchmark but did not reach the worker:
SSO token expired and refresh failed. Asked Aljoscha to run `bin/scratch login`.
No latency measurements exist yet. Do not bypass authentication. The worker remains
`i-0788036f0778687f1`, expiry2026-10-29T00:36:58Z. Archive the runtime evidence off
worker once access returns. Local test/build logs and witness source are retained.

The successful full suite required a fresh owned MinIO fixture. Two earlier
57/58 results were reproduced unchanged: first a missing service, then an object
left by the successful targeted COPY test. No test or production semantics changed.
Current owned fixture ID is
`051a06efcf43093703903481e6ed58429276a8fab6684ae4c43712f3c5fae9c8`.

Published lint repair `34942b30` is verified by CI141276's passing Lint/rustfmt
job. CI141276 is terminal failed on product registry visibility and merge skew.
CI141256's product log explicitly says pubsubd is not public on Docker Hub.

A reviewed private benchmark script is prepared at
`~/.local/share/aj/m3-evidence/fast-peek-benchmark.py` and copied to worker evidence.
Prebuild with the public optimized launcher before executing it. It requires an
unthrottled 32-CPU adapter allocation on the 16-vCPU VM, records actual cgroups,
and measures one-row indexed SELECT with simple/extended protocols, 1/8 clients,
and strict/ordinary serializable. Capture candidate then published baseline
`98de254e` on this same machine, with no concurrent compiler/tests during timings.
The backend is PostgreSQL18, not the user's CRDB benchmark. No latency result yet.
Cross-target triple debug/optimized caches were unused and removed after executable
and symlink checks, leaving 106GiB free. Default target caches and evidence remain.

Deferred beyond M3: Kubernetes research found missing orchestratord launch,
readiness and SQL routing, separated child service-account authority,
forwarded-file delivery and shared relay cleanup wiring. Existing backend resource
tests do not establish these.
The approved worker expiry was extended to 2026-10-28T22:00Z without changing
capacity. CI141248 for the retention fix is terminal failed with no live jobs.

Aljoscha's two-adapter boot crash from metrics-history retention is fixed and
published as `c82c0769`. A definitive UpperMismatch now logs and skips that
snapshot's retractions, matching status-history retention. Other errors retain
existing handling. No ownership change or stale-retraction retry. The real-Persist
regression covers a successful pass, competing append and competing collector.
It and strict storage-controller all-target Clippy pass. The default build passes.
Evidence: `metrics-retention-19531e45.tgz` (verified candidate source matches the
published fix).

Full policy/recovery runtime verification also passes: concurrent ON REFRESH,
non-forced warm resize with continued writes/reads, controller restart retaining
healthy processes/status, replica termination followed by new offline/online
history and maintained-read recovery, and survivor-only policy transitions.
Cleanup succeeded. Evidence: `policy-recovery-retention-19531e45.tgz`, successful
run `launcher-4e4c1ea912b443a7afd564bc60a8ca2f`. Earlier recovery witnesses used the
wrong replica identity flag (`--replica-id` rather than `--catalog-replica-id`);
the failure reproduced before correction. Their logs also exposed the real
metrics-retention conflict independently reported by Aljoscha.

This does not resolve the separate txn-cache GC assertion below. Its removal
still awaits approval. Promotion readiness/adapter restart UX also awaits the
previously requested decision. Do not infer either approval from the retention fix.

Default debug revision `888e822f` passed the real `bin/controllerd` witness:
managed relay initialization, maintained SQL, Ctrl-C and unchanged adoption,
relay-only executable replacement with retained endpoint/SQL child PIDs/session,
whole-environment stop after controller exit, and restart retaining SQL data.
Cleanup succeeded. The build through `bin/controllerd --build-only` took 42m33s.
Evidence is preserved off-worker in `public-launcher-888e822f.tgz`, including
`launcher-5adaafebc6e8413bac8b3763148bf52c/results.json` and the executed witness.
This does not cover the later default-reset helper.

The extended relay-outage witness also passed on `888e822f`. After stopping the
controller and terminating its relay, new inserts/deletes and maintained MV reads
completed through durable fallback while the relay remained absent. Restart
restored the relay endpoint without replacing SQL children. Evidence and cleanup
success: `public-relay-outage-888e822f.tgz`, run
`launcher-57029096f3b14e86b539be0732d8cd83`.

Default-reset runtime acceptance passed at `519dbdc3` after a clean sync and
successful default build. The witness verified ordinary reset with retained
children, then deleted its own state directory and verified process recovery,
metadata reset and fresh SQL again. Cleanup succeeded, and independent read-only
checks confirmed its default database and directory were removed. No preexisting
database or directory was reset. The metadata server was PostgreSQL18.4, not
CockroachDB. Evidence: `default-reset-519dbdc3.tgz`, run
`launcher-642c6649b2454a0084ede6523bdbe58c`.

The two-adapter SQL/HTTP lifecycle witness on the same debug revision failed
once, then passed unchanged. The failed run reached adapter replacement before
both adapters panicked at `src/txn-wal/src/txn_cache.rs:595`: a retained write was
older than the global minimum unapplied timestamp. Restarts repeated the panic.
Do not dismiss this as a deadline or weaken the assertion. A two-shard reduction reproduces it twice in an extracted std-only harness
(`/tmp/txn-cache-repro/repro.rs`), not a Cargo test. The cache file is identical
to pinned upstream. Per-shard cleanup leaves another shard behind the advancing
global minimum, violating global compaction validation without losing writes in
the reduction. Oracle confirmed the obsolete global-GC check is not a reader safety invariant.
Removal with cross-shard/two-writer/replay regressions awaits Aljoscha approval,
explicitly requested. No production changes or assertion weakening have been made.
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

The replica-observation repair passes unit and targeted runtime verification.
Native policy and catch-up use existing deployment-local QueryReady observations,
not the permanently-empty legacy backend status map. Controllerd publishes original
process transitions directly to the existing deployment-qualified Persist status
history. No new catalog record or observation transport, and no retention or
policy moves into controllerd. Independent review found initial-watch transition
loss. Desired inventory is now bound before polling watches. A compiler task-result
error reproduced and was fixed. Library checking and strict all-target Clippy pass, including task138 after
the final test-fixture correction.

The focused status test passes with names constructed through the real service
formatter. Its original fixture failure reproduced before correction. The broader
controller suite exposed a preexisting error-message expectation bug: collection
filter failures were compared with invalid-adapter-ID errors. That also reproduced
and was corrected without changing production validation. All14 controller unit
tests, strict all-target Clippy and the default public build pass at candidate
`d61d9c6b`. Evidence: `replica-status-verification-d61d9c6b.tgz`.

Two public-launcher policy witnesses passed on `d61d9c6b`, both with successful
cleanup. They cover concurrent ON REFRESH creation, resize, maintained reads,
retirement and new transitions with each tested survivor after the other process
exits. The extended run also completes a non-forced native warm resize while
writes and maintained reads continue, exercising QueryReady-based readiness.
Evidence: `public-policy-d61d9c6b.tgz`, run
`launcher-f47b4c0eb5174e8290f56c25ad9a9bdc`, and
`public-policy-warm-d61d9c6b.tgz`, run
`launcher-807cd1f038924be79bef151bc18e92a4`.
The worker has about57GiB free after unused cross-target incremental cache cleanup.
Built products, source, evidence and the active cache were preserved.

The original ON REFRESH witness failed twice. First: cache assertion. Exact
repeat: scheduled replica and MV start, but SQL status stays empty. Evidence:
`policy-failure-888e822f.tgz` and `policy-repeat-888e822f.tgz`, last run
`launcher-deee7f176eaf455691145bd93bfbc80d`. Both cleaned up successfully.
The passing witnesses above include dropped-process exit checks. They do not
resolve the separate intermittent transaction-cache assertion.

Replica resource metrics and backend-status session notices/Segment events are
also absent in split mode. This repair does not silently exclude them from M3.
Metrics belong with controller backend observations. Changing notices to query
connection semantics requires an explicit UX decision.

Deployment coexistence requires implementation, not just acceptance. Controller
startup has only fresh/active-restart paths. Pending admission and promotion APIs
already exist in durable catalog and environmentd preflight, but controllerd lacks
those lifecycle paths, adapter catch-up reporting, serving-mode transition and
retired-generation cleanup. Agent37 traced reuse boundaries. Keep policy/readiness
computation in adapters, reuse existing admission/fencing and external promotion,
and bring consequential serving-mode/cleanup choices to Aljoscha. The executed
local old/new upgrade walkthrough remains open, alongside
interrupted initialization and remaining broad regressions.

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
is deferred beyond M3. Keep the standalone image publishable.

`--stop` is scoped, idempotent and data-preserving without rebuilding. Ctrl-C
stops only controllerd. PID/start identities and recorded backend locations keep
unrelated environments untouched. Real launcher stop/restart now passes.
`--reset`, explicitly requested after deletion of local state, accepts only the
default local database/state/blob targets. It stops recoverable associated
processes before dropping the three metadata schemas and local directory, rejects
live foreign-worktree database users, and preserves files on SQL failure.
Independent review findings are resolved. All29 launcher tests, Black and Ruff
pass, including real disposable process checks. The real default-path reset and deleted-state recovery passed as described above.
No user database was reset.

The catch-up repair (`d6f662f7`) applies/enacts each transaction against its
matching projection. Review, strict Clippy, multi-subscriber regression and the
full catalog suite passed (396 passed, two ignored). Evidence:
`prefix-review.md`, `prefix-reviewed-{test,clippy}.log`, `prefix-catalog-suite.log`.

## CI and worker

CI141243 for `68825578` is terminal failed, with no executing jobs when the
retention fix was published. The new publication requires fresh CI inspection.
CI141235 for `6dbd009d` is terminal failed. Failed jobs are x86 product
`01a182a6-884a-4a85-a0a7-335a8a2a494a` and merge skew. The current product log confirms
`pubsubd` is not public on Docker Hub. Merge-skew details remain uninspected. CI141217 built products but failed the
public Docker Hub visibility gate for `materialize/pubsubd`; a registry owner
must resolve it without bypassing the gate. Its pinned-base merge-skew job also
failed. Cargo/downstream integration did not run. Broad CI is not green.

Worker `i-0788036f0778687f1` is now pinned at `19531e45`, with default debug binaries
and about57GiB free after cache cleanup. Scratch access works. Last expiry check showed
five hours remaining. Task150 completed successfully. No runtime/build work remains active. Check again before syncing. Exact older CI product:
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
