# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and the current handoff in
doc/developer/design/20260903_decoupled_coordination_log.md, then inspect the
code, worktree and remote bookmark. Preserve other sessions' work. The archived
log is historical context, not required reading or a task list. Current design
and steering supersede old proposals.

Current steering

Finish M2 with native ownership enabled, including compatible-version warm
handover, before starting M3. Do not restore an adapter-side installer.
The handoff identifies the current blocker, next comparison and design-decision
point. Follow that endpoint rather than accumulating locally useful fixes that
leave the same user-visible failure unresolved.

Keep demonstrated outcome scopes closed and regular regression coverage enabled.
Fix concrete integration failures without reopening an acceptance campaign or
making unrelated fixture cleanup a prerequisite. The design owns SQL behavior,
protection lifetime, deployment identity, compatibility and the M2 exclusions.
Those contracts remain binding without being repeated as implementation tasks here.

Scope and working rules

Mechanisms and factoring within the design's contracts are yours to choose.
Bring consequential behavior changes, ownership/protocol changes or
disproportionate cost to Aljoscha and pause the affected work. Routine
implementation decisions need no renewed approval.

Compare disputed behavior with the baseline before introducing stronger
guarantees or new machinery to satisfy a test. Check affected owners when
changing a shared rule. Keep deliberately excluded owners explicit in the
handoff rather than treating the whole correction as complete.

Add durable records only for required information that cannot be derived.
Production test-only APIs need approval. Preserve freshness, structural
validation, protection and definitive submitted-write outcomes. Do not tune
Persist leases, statement deadlines or the 1s publication default, weaken safety
checks, or invent scheduling guarantees to turn an unexplained failure green.

Debugging and verification

Separate diagnosis from acceptance. Within the approved scratch budget, use
smaller check subsets, shorter captures and reversible A/B patches without
renewed approval. Choose a hypothesis and distinguishing measurement. Confirm
that a reduced case retains the relevant cost or failure. An experimental
candidate need not be ready to land or have its benefit proved beforehand.
Keep ineffective experiments out of the landing history.

Use existing metrics, profiles and narrow diagnostics before adding
instrumentation. Verify effective runtime configuration and resources, not just
CLI flags or CI queue labels. Use a matched baseline when attributing regressions.
Keep fixture services available for inspection when useful, then use clean state
for comparisons. Reuse run recipes and collect related evidence together.
Batch independent reads, status checks and remote collection rather than taking
a model round trip for each. Keep large raw captures in artifacts.

Short experiments do not relax correctness or acceptance. Verify a promising
repair against the original workload, concurrency and resource budget.
Measure user-visible completion and protection/compaction progress separately.
Triage unrelated failures without letting them displace the current blocker.

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

The handoff is a resume pointer, not a work log. Keep only current outcomes,
live blockers, the next useful comparison, its decision point and evidence
pointers. Replace current state and remove resolved work. Do not append a
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
