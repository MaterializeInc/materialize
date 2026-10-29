# Decoupled coordination: current handoff

Follow [the prompt](20260903_decoupled_coordination_prompt.md) and
[the design](20260903_decoupled_coordination.md). M2 is closed within its approved
scope. M3 remains active. Kubernetes/operator runtime is deferred beyond M3.
Preserve native execution and the 60s publication default.

## Current slice and next action

The logical storage-frontier correction is complete. It combines committed read
permission and exact shard binding from one fresh catalog prefix with durable
write progress. Physical since can remain pinned by ingestion resumption leases
and must not define the public logical read frontier. Certification stays bounded
and incremental, with round-robin selection to prevent hot-key starvation.

Verification passes: all44 storage-controller tests, targeted durable catalog
certification, strict storage-controller/catalog/adapter all-target Clippy, default
`bin/environmentd` and `bin/controllerd` builds, two-adapter restart and placement,
and the full `controller-frontiers.td` on default-debug binaries. The source
assertion that failed three times before the correction passes unchanged. The
obsolete assertion that treated `mz_frontiers` as a storage source now checks
`mz_storage_frontier_observations`, retaining both progress predicates. Targeted
MV replacement/alias coverage is retained. Review67's starvation and permission-only
coverage findings are resolved, including a real peer CAS conflict and twice-red
starvation regression. All runtime compositions and managed test processes stopped.

Evidence is archived under `startup-spin/logical-frontier-runtime-pass.json.gz`
and `logical-frontier-full-test-pass.json.gz`. Runtime binary source is ec0ca0def,
full test checkout3580c0fd differs only in test/docs. The full-test archive records
binary hashes and both revisions. Logs: `/tmp/m3-logical-frontier-runtime.log`,
`/tmp/m3-logical-frontier-debug-testdrive-final.log`, and
`/tmp/m3-logical-frontier-final-checks.log` (tests pass, subsequently corrected
Clippy casts). No worker job remains active.

**Next: compatible-version full writers and explicit retirement.** Aljoscha
approved both compatible versions accepting SQL, DDL and table writes. Promotion
switches active execution authority without implicitly retiring the old deployment.
Retirement is a separate deliberate administrative command, durably fencing the
selected deployment before reclaiming its protections/execution and processes.
Surviving deployments and shared pubsub must remain untouched. Keep one authorized
external-sink execution to avoid Kafka producer fencing. Brief candidate-adapter
restart is acceptable, but read-only prewarming is no longer the final M3 outcome.

Agent68/task321 identified the existing constraints: pending DDL is restricted,
same-generation admission requires equal SemVer precedence, and promotion removes
old membership/protection/execution. Schema/fingerprint and semantic compatibility
constraints still apply. Do not substitute same-version build-metadata variation
for real version overlap. Output write enablement is currently one-way
(`execution.rs::apply_catalog`), so simply preserving old sink executions would
leave competing Kafka producers. Reuse admission and explicit retirement rather
than introducing a new consensus protocol. The designer-owned design/prompt still
need alignment with this approved steering.

## Initialization recovery

Atomic provenance is absent for unmarked metadata,0 pending,1 complete. Only the
original build/generation's pending initialization can resume. Admission checks
precede fencing/migrations. Unmarked existing environments cannot be adopted.
Completed restarts reconcile only four configuration tables, never the full seed.

Review62 found that a delayed same-generation initializer could reseed after a peer
completed and launched children. A deterministic real-API regression reproduced
loss of the peer's completed-table change twice. The fix freshly checks pending
provenance AFTER each WAL snapshot, before its conditional append at B+1. If
completion follows certification, any child WAL write invalidates that append.
Retries resnapshot and recertify. Oracle63 verified this ordering against the actual
catalog/Persist/txn-WAL implementations. No new ownership record or lease is needed.

Current verification passes:
- All four recovery tests, including five durable interruption boundaries,
  unmarked refusal, completed-restart drift preservation and overlapping startup.
- All 21 durable-open integration tests.
- Strict catalog all-target Clippy and repository formatting.

Evidence: `/tmp/m3-init-overlap-before.log`, `before-repeat.log`,
`fixed-verification.log`, `fmt.log` (same `/tmp/m3-init-overlap-` prefix).
Candidate: `/tmp/m3-init-overlap-fixed-candidate.txt`. The two-before/one-after
regression is real table-content validation, not merely absence of a panic.

## Published work

[PR38696](https://github.com/MaterializeInc/materialize/pull/38696) remains draft.
Bookmark `decoupled-coordination`, contributor remote `origin`, publication is fast-forward. The frontier and txn-WAL slices below are published.
Initialization recovery follows them as a separate commit.

- `ba7537a3`: bootstrap retries peer WAL advancement, preserving retirement
  checks and snapshotting at the established timestamp.
- `20e9ca3f`: retained differential-writer observations, refresh after backoff,
  bounded retry work and responsive command/shutdown handling.
- `37a38130`: replica-owned and cooperative storage frontier observations,
  public reductions, retained upgrade-reference reads and authorized cleanup.
- `5d5e654c`: removes only txn-WAL global GC-tightness validation. Safety checks
  and per-shard lazy cleanup remain. The real two-writer regression failed twice
  before the fix. All 34 txn-WAL tests, strict Clippy and formatting pass, including
  explicit listener progress before and after writer restart. Review61 found no
  blocking issue. The formerly blocked completed-restart preservation test passes.

Txn evidence: `/tmp/m3-txn-lazy-cleanup-before.log`, `before-repeat.log`,
`/tmp/m3-txn-final-verification.log`, `/tmp/m3-txn-clippy-preservation.log`.
The test does not claim reader-process restart. No cleanup sweep was added.

## Frontier acceptance and remaining coverage

Default-debug integrated candidate f59cf4156377125941592ba1c8f591a9a96143be passed
actual two-adapter join/disconnect/replacement with peer writes, SQL/HTTP progress,
frontier preservation, two-replica reductions, targeted MV replacement aliases and
completed frontiers. Catalog-authorized DROP cleanup preserved live observations
and survived adapter restart. All 28 cleanup check cycles were below one second.
The original startup-spin symptom itself remains unreproduced.

All 43 storage-controller tests, 80 catalog-decoder tests, five caught-up tests,
real-Persist publisher/certification tests and strict integrated checks passed.
The exact frontier commit separately passed adapter/clusterd all-target checks.
The runtime candidate also contained pending initialization recovery.

Native prewarm requires identical builtin fingerprints. The frontier repair starts
a builtin epoch. Use compatible post-change versions for the upgrade walkthrough,
not cross-epoch native migration. Both raw-frontier migration guards remain.
Replica reports require current execution identity. Storage cleanup observes rows
before freshly certifying exact bindings, retains incremental state, and retries
CAS conflicts with new certification. Silence or certification error cannot authorize
retirement. Zero adapters may pause cleanup.

## Remaining M3 work

Implement controller deployment coexistence: pending admission, adapter readiness,
promotion/serving transition, overlapping compatible full writers and explicit
retirement/retired-generation cleanup. Reuse existing durable
admission/fencing and environmentd preflight. Keep policy/readiness in adapters
without a designated leader. The brief candidate-adapter restart at promotion is approved. Bring other
consequential UX decisions to Aljoscha before implementation. Execute the copyable local version-upgrade recipe
in `doc/developer/controllerd.md`, preserving warmed execution and shared pubsub.

Remaining acceptance also includes matched fast-peek measurements, diagnostic/notice
requirements and broad regressions. Replica process resource metrics are implemented.
Existing-environment conversion and async catalog-owner extraction remain excluded.
Derived projections retain the explicit originator-only M3 limitation.
[Architectural follow-ups](20260903_decoupled_coordination_followups.md), committed
in b422b581, preserve the audit backlog without authorizing unrelated implementation.

## CI

Broad CI is not green. Builds141410 and141419 (latest published head0cc15b05)
completed optimized Rust builds but failed the public-image gate because `pubsubd`
is not public on DockerHub. Registry visibility needs @team-testing, not a bypass.
Build141419 published materialized and testdrive images, used for the unchanged
frontier regression. Worker `current-candidate-images.json` records their tags.
They do not contain the pending logical-frontier correction. Upstream merge-skew
failures remain outside the authorized integration scope. Do not merge upstream,
enable nightly validation, bypass gates or mark the PR ready.

## Worker and evidence

Worker `i-0f5a60529f57d6ac7`,16vCPU/61GiB/500GiB, expires
**2026-10-30T18:13:29Z**. Scratch authentication works. Use toolchain1.98.1,
CARGO_BUILD_JOBS=2 and RUST_MIN_STACK=8388608 for tests. Clippy is installed.
Private PostgreSQL18 runs on loopback26257 with max_connections5000.
Connection file: `/home/ubuntu/m3-evidence/native-postgres-url.txt`. Never print it.

Private harnesses are in `~/.local/share/aj/m3-evidence/` locally and
`/home/ubuntu/m3-evidence/` on the worker. Run with `bin/pyactivate`.
Use SQL replica size `scale=1,workers=1`; adapter HTTP size1 is separate.
Each harness has its own metadata database, verified process cleanup and results.
Do not sync during verification or overlap heavy work with measurements.

Sanitized, validated gzip/JSON evidence is under
`~/.local/share/aj/m3-evidence/startup-spin/`:
`baseline-repeated-frontier-loss.json.gz`, `frontier-integrated-runs.json.gz`,
`targeted-mv-before-fix.json.gz`, `frontier-restart-pass.json.gz`,
`integrated-placement-restart-pass.json.gz`, `frontier-retirement-pass.json.gz`.
The last archive preserves results, SQL and all 28 cleanup samples from worker run
`launcher-9985b2776db94483ad38361b0a6d44cd`. Preserve evidence before worker expiry.
