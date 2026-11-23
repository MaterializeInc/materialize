# Decoupled coordination: implementer session prompt

```text
Implement Milestone 3 in doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and the current handoff in
doc/developer/design/20260903_decoupled_coordination_log.md, then inspect the
code, worktree and remote bookmark. Preserve other sessions' work. Current design
and steering supersede old proposals.

Current steering

M2 is closed within its approved scope, including the 60s publication default.
Keep its demonstrated outcomes closed and regular regressions enabled. Preserve
native maintained execution rather than restoring an adapter-side installer.

Follow the M3 slices in the design, starting with one controller-managed default
adapter and separate pubsub. Continue through independent adapters, SQL/HTTP-managed
lifecycle and local deployment coexistence, not just the binary extraction.
The handoff identifies the current slice, concrete gaps and next useful check.

Kubernetes backend and operator integration, including Kubernetes runtime
acceptance, are deferred beyond M3. Preserve existing backend code, but do not
pursue that integration or treat it as a milestone blocker. The local-process
demo and version-upgrade walkthrough remain required.

M3's local entry point is plain `bin/controllerd`, using the existing developer
launcher. The default development build must work for demos. `--optimized` is
optional, with the same behavior as `bin/environmentd`. Controllerd provisions
statically configured pubsubd through the existing backend and includes it in
inventory, without catalog records or a separate manual startup step. Preserve
the design's shared-relay lifetime and ownership across deployments. Verify the
local demo through this entry point and document it, not only the test harness.

Include `bin/controllerd --stop` for idempotent, data-preserving local environment
shutdown, even after controller exit and without rebuilding. Keep Ctrl-C as
controller-only shutdown. Follow the design's environment-scoping contract.

M3 also requires an executed, user-facing local version-upgrade walkthrough in
`doc/developer/controllerd.md`. Two different compatible versions must both serve
SQL, DDL and table writes before and after promotion. Promotion switches active
execution authority without retiring the previous deployment. Retirement is a
separate explicit HTTP administrative operation, fencing only the selected
deployment before releasing its protection and cleaning up its processes.
Preserve surviving deployments and shared pubsub. Each external sink retains one
authorized execution even while both versions serve writes. A brief candidate
adapter restart at promotion is acceptable, but preserve warmed replicas.
Singleton CDC readers follow catalog-selected ownership through replica followers
and transfer alongside sink authority. Candidate compute warms from shared Persist
source data. Allow temporary reader overlap and existing connector takeover/recovery,
but superseded readers must eventually stop reconnecting. Do not add stop
acknowledgements, leases or another coordination protocol. Verify continued
ingestion and eventual cessation of contention, not interruption-free handover.
Temporary Items keep their session UUID for visibility and gain the existing
adapter execution incarnation as ownership metadata in the same transaction.
Keep normal disconnect cleanup. Reclaim only catalog-confirmed replaced or retired
executions, preserving items on promotion. Do not make `mz_sessions` a correctness
dependency or add per-connection catalog records.
Experimental catalog-format changes may require fresh environments. Do not add
an in-branch migration or infer ownership for existing UUID-only temporary Items.
Read-only prewarming does not satisfy this outcome. Two package versions built
from the current source, sharing implementation and plan encoding, are the
accepted pair for M3. Report them as such, not as release-skew or migration
coverage. Keep schema/encoding/semantic compatibility checks. Document and
execute the commands, not only test coverage. Reuse catalog admission and Persist
protocols rather than adding a separate consensus or leadership mechanism.

Include `GET /api/collections`: pretty JSON with collection ID, name and the
catalog's committed combined since, not physical Persist since. Support SQL LIKE
patterns in the `id`/`name` URL filters. Follow the design's read-only inspection
contract and document browser/curl usage.

Work outcome-first: get the smallest real end-to-end slice of the main outcome
working early. Use that integration to expose architectural risks and determine
which supporting work is actually needed. Limit preparation to concrete blockers
of the next integration step, rather than completing supporting layers or
edge-case inventories in advance. Stop preparing once that step is unblocked.
Keep partial coverage explicit without weakening correctness or final acceptance.

Keep cluster-controller policy in adapterd, committing desired replica changes
through validated catalog transactions without a designated policy adapter.
Controllerd enacts committed process inventory and initializes deployments. It
owns neither replica-set policy nor shared database duties. Its HTTP API is an
unauthenticated listener with access control provided externally, not a new auth
subsystem. Catalog validation and deployment fencing still apply.

Fix multi-adapter ownership at the natural boundaries. Reuse safe collectors and
existing backends rather than building a general background
job framework or rewriting all introspection. The abandoned async catalog-owner
refactor remains outside scope.

Peer writes to the catalog, oracle and table WAL are ordinary in M3, not a sign
of an unexpected writer. A conditional write must converge under them. Prepare
before allocating its timestamp, keep prepared work across retries, back off,
and return to the owning event loop instead of retrying inline without bound.
Ordinary contention must not halt a process or fail startup. Fix the surveyed
sites at their existing boundaries without a general retry framework, and keep
the ordering guarantees each conditional write provides.

The M3 retry cut is now explicit: finish the in-progress MV allocation repair,
then defer further statement-by-statement scheduling conversions. Remaining
runtime and startup retry limitations must stay visible. This narrows the
blanket responsiveness requirement, not write safety, native-only removal or
final acceptance. Do not claim the remaining inline owners are cooperative.

Proceed with the approved replica metadata-only reconstruction boundary without
waiting on those deferred conversions. Preserve exact storage and execution
schemas, real SQL types/dependencies, selected plans and fencing. Do not insert
fake MIR or assume declared and inferred descriptions are equivalent. Bring back
any need for new durable metadata or a broader boundary before implementing it.

KafkaSources acceptance uses optimized builds with `--actions=10000
--max-execution-time=20m` and a 60-minute job timeout, preserving the agreed CPU
and memory limits. Run the pinned upstream base once with the same optimized
profile and scenario. Require completed final validation and verify backup/restore
coverage, running restore separately if absent. Report time to ready per generation
for both. Debug candidate readiness belongs to startup measurement, not the Zippy
acceptance gate. Complete the 60s four-index run and version-pair rebuild before
further readiness tuning. Before more operator profiling, report the lagging
source-statistics shard's since, the candidate dataflow's starting timestamp and
whether lag tracks environment age. If the candidate replays full history, find
what holds compaction back.

Startup and restart latency is an M3 outcome. Measure adapter restart, whole
environment restart and promotion to serving on the default debug build, then
remove waste: work over all objects where a few are needed, repeated catalog
open cost across processes, and fixed sleeps on the restart path. Report
before and after timings per phase. Do not skip validation or recovery to get
there.

Scope and working rules

Mechanisms and factoring within the design's contracts are yours to choose.
Bring consequential behavior changes, ownership/protocol changes or
disproportionate cost to Aljoscha and pause the affected work. Routine
implementation decisions need no renewed approval.

Compare disputed behavior with the baseline before introducing stronger
guarantees or new machinery to satisfy a test. Check affected owners when changing
a shared rule. Keep exclusions explicit and do not silently broaden the milestone.

Add durable records only for required information that cannot be derived.
Production test-only APIs need approval. Preserve freshness, structural
validation, protection and definitive submitted-write outcomes. Do not tune
Persist leases, statement deadlines or the 60s publication default, weaken safety
checks, or invent scheduling guarantees to turn an unexplained failure green.
Judge publication behavior at that default. Workflows that shorten the interval
are stress tests, not acceptance gates or optimization targets.
M3 completes on the pinned upstream base. Do not merge or rebase onto main.
The merge-skew CI check stays red and is accepted. Do not work on it. State
this in the PR description, next to any job marked unsupported.

Debugging and verification

Rerun an exact failing command and inspect its output before broader diagnostics.
Separate diagnosis from acceptance. Within the approved scratch budget, use
focused subsets, short captures and reversible A/B patches without renewed
approval. Choose a hypothesis and distinguishing measurement, retaining the
relevant cost or failure in a reduction. Keep ineffective experiments out of
landing history.

Use existing metrics and narrow diagnostics first. Verify effective runtime
configuration and resources, not just flags or queue labels. Attribute regressions
with matched baselines. Verify repairs against the original workload, concurrency
and resource budget. Keep SQL completion, protection progress and physical
reclamation distinct. Unrelated failures must not displace the current slice.

Follow repository instructions and skills. Use one dedicated x86_64 scratch
worker via bin/scratch and misc/scratch/decoupled-coordination.json:
16 vCPUs, 64 GiB RAM and 500 GiB disk. Builds there may use every vCPU and
default Cargo parallelism, with no build time limit, because builds and
runtimes never overlap. Keep runtime and measurement limits fixed so numbers
stay comparable.
Reuse the worker and build caches, restarting it or extending expiry proactively
without renewed approval. Record its ID and expiry and preserve useful evidence off-worker.
Ask before increasing instance size, count or disk. SSO renewal requires
Aljoscha to authenticate with bin/scratch login. Report that access blocker
without treating it as a reason to replace the worker or to stop working.

The shared editing VM (16 cores, 18 GiB RAM) is a working machine, not only an
editor. Use it whenever scratch is unavailable, and for quick checks when it is
faster: compiles, Clippy, targeted tests and the full bin/controllerd setup all
run here. Apply no fixed memory cap. Limit Cargo jobs from observed memory
pressure and leave a few GiB free for the editing sessions. Keep broad CI-style
suites, large workloads and long measurement runs on scratch.

Keep canonical edits here. Sync explicit Git revisions only to an idle, clean
worker: bin/scratch push force-checks out the supplied revision. Do not overwrite
remote work or change source under a running build/test. Use repository entry
points and build defaults, adjusting concurrency from observed resource pressure.
Keep unrelated heavy work off the worker during measurements.

Use focused checks for the inner loop and regular draft-PR CI for broad
integration and final combined verification. Preserve in-flight verification
when publishing fixes. Verify changed contracts at their boundaries, using
existing regressions where sufficient. Seek independent review when warranted.
Report failed, pending and unverified results explicitly.

Preserve contracts, not the test inventory. Remove obsolete or vacuous tests
rather than weakening assertions. Replace coverage only for meaningful gaps.
Batch shared fixture corrections instead of rediscovering them per test.

Documentation and history

The handoff is a resume pointer, not a work log. Keep only the current slice,
outcomes, live blockers, the next useful check and evidence pointers.
Replace current state and remove resolved work. Update it once per finished
outcome, not per step, experiment or verification run. Per-run receipts, hashes
and file inventories belong in the evidence directory, not the handoff.
Keep the PR description concise and reviewer-facing: motivation, approach, scope,
current verification and open blockers, with evidence links. Update that summary
in place, not with per-commit or per-run history. Detailed logs, test inventories
and intermediate measurements belong in linked evidence, outside the repository.
Design and prompt bodies are designer-owned unless documentation work is
explicitly assigned. Flag resolved steering for removal. Re-read this prompt
after compaction.

Commit coherent changes with jj and continue beyond intermediate commits.
You may commit and push to the bookmark above without asking. Push at least
daily so work does not live on one machine. Before finishing,
fold your own fixups and handoff updates into a straight implementation history
while preserving useful review boundaries. Check remote changes before pushing.
Ask before rewriting others' commits or commits others have built on.

Keep the PR as a draft. Do not enable nightly validation, merge, mark ready, or
push to upstream or other branches without asking.
```
