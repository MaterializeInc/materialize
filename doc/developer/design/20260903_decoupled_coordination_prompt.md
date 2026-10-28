# Decoupled coordination: implementer session prompt

```text
Implement Milestone 3 in doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and the current handoff in
doc/developer/design/20260903_decoupled_coordination_log.md, then inspect the
code, worktree and remote bookmark. Preserve other sessions' work. The archived
log is historical context, not required reading or a task list. Current design
and steering supersede old proposals.

Current steering

M2 is closed within its approved scope, including the 60s publication default.
Keep its demonstrated outcomes closed and regular regressions enabled. Preserve
native maintained execution rather than restoring an adapter-side installer.

Follow the M3 slices in the design, starting with one controller-managed default
adapter and separate pubsub. Continue through independent adapters, SQL/HTTP-managed
lifecycle and deployment/backend integration, not just the binary extraction.
The handoff identifies the current slice, concrete gaps and next useful check.

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
Keep the upstream base pinned unless Aljoscha authorizes integration.

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
16 vCPUs, 64 GiB RAM and 500 GiB disk. Keep the shared editing VM lightweight.
Reuse the worker and build caches, extending expiry proactively without renewed
approval. Record its ID and expiry and preserve useful evidence off-worker.
Ask before increasing instance size, count or disk. SSO renewal requires
Aljoscha to authenticate with bin/scratch login. Report that access blocker
without treating it as a reason to replace the worker.

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
Replace current state and remove resolved work. Do not append a
paragraph for each fix, experiment or verification run, or start another diary.
Keep the PR description concise and reviewer-facing: motivation, approach, scope,
current verification and open blockers, with evidence links. Update that summary
in place, not with per-commit or per-run history. Detailed logs, test inventories
and intermediate measurements belong in linked evidence. Do not append to the
archive.
Design and prompt bodies are designer-owned unless documentation work is
explicitly assigned. Flag resolved steering for removal. Re-read this prompt
after compaction.

Commit coherent changes with jj and continue beyond intermediate commits.
You may commit and push to the bookmark above without asking. Before finishing,
fold your own fixups and handoff updates into a straight implementation history
while preserving useful review boundaries. Check remote changes before pushing.
Ask before rewriting others' commits or commits others have built on.

Keep the PR as a draft. Do not enable nightly validation, merge, mark ready, or
push to upstream or other branches without asking.
```
