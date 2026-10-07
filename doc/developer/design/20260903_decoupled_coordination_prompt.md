# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and the current handoff in
doc/developer/design/20260903_decoupled_coordination_log.md, then inspect the
code, worktree and remote bookmark. Preserve other sessions' work. The archived
log is historical context, not required reading or a task list. Consult it only
for a specific question. Current design and steering supersede old proposals.

Current steering

Milestone 2 includes native deployment coexistence and compatible-version warm
handover. Keep native ownership enabled, with no second adapter-side installer.
Finish M2 before starting M3's adapterd/controllerd split, SQL adapter lifecycle
and process inventory. That extension retains independent-client isolation and
requires multiple serving adapters, but does not expand M2 acceptance.

The next end-to-end outcome is same-version native prewarming and warm
promotion: a second deployment uses its own catalog-described replicas and
durable read-only/output-write authority, warms while the first serves, then
retains that execution through externally authorized promotion. Catalog
membership must not imply output-write authority. Preserve read protection,
catalog fencing and external-sink safety. No borrowed deployment identity or
controller-to-native cold bridge.

Use the existing concurrent-writer protocols for compatible Persist outputs,
including late writes from retired deployments. Correctness must hold
throughout, not just after convergence. No generic deployment fence on Persist
appends or all-output fencing barrier is required. Bring a concrete unsafe
interleaving before adding stronger fencing, not merely overlapping writers.

Catalog admission and retirement govern Persist compatibility, including fresh
shard initialization through read-like APIs. Reuse deployment membership and
needed version information, without spoofing the binary version. Bootstrap and
maintenance upgrades must honor that policy. After durable retirement, formats
may advance without waiting for zombies to die. Reject admission incompatible
with already-written state. Solve the initialization race through Persist
compatibility, not pre-publication initialization and orphan cleanup.

Use that outcome to establish the deployment model, then complete
catalog/Persist writer coexistence and handover between compatible versions.
Same-version success is an intermediate proof, not completion of M2. Do not
design a same-version-only shortcut that needs another ownership model for
version overlap.

M2 requires stable ReplicaIds for shared declarations and replicas deliberately
carried over across deployments, including their public IDs. Runtime identity
includes deployment. Independent creates and DROP/recreate get fresh IDs, not
name-based matches. Deployments can have different replica sets, including
private hydration bursts. Keep shared intent distinct from deployment-local
membership, hydration, burst state and reconfiguration progress. Align the
catalog model with these contracts, not just a stable declaration_id behind
changing public replica IDs. Table layout and factoring remain yours to choose.

For these milestones, keep mz_cluster_replicas shared and materializable,
filtered to the catalog's active deployment regardless of the querying adapter.
Routing, reconciliation and prewarming readiness use their own deployment's
inventory. Query-relative public catalog views and their maintained-query
semantics are future work, not a prerequisite or an implementation task here.

M2 excludes native warm handover with replica-targeted MVs on managed clusters.
Reject that combination before promotion, leaving the active deployment intact.
Preserve single-deployment behavior and handover for explicitly declared
replicas on unmanaged clusters. Do not retarget pins or cascade-drop shared
MVs when retiring private replicas. Broader MV pinning semantics are future
work, not the core replica identity and membership model.

Close the known serving-compatibility gaps at their owning boundaries:
- Establish the index's initial as_of, bound and logical/actual-input protection
  with its definition and selected plan in the DDL transaction. Plan changes
  update import protection atomically. SELECT and EXPLAIN share acquisition
  before installation. Preserve fixed timestamps, transaction effects and
  zero-replica EXPLAIN, without observation-dependent candidates or EXPLAIN
  workarounds. Protection follows current requirements, not a birth-time pin.
- Preserve statement-specific timeout scope at admission, retaining freshness,
  explicit cancellation and definitive completion for submitted writes. No
  universal deadline or SET-only workaround.
- Remove obsolete controller accounting. Keep meaningful public lag, frontier,
  hydration, cleanup and autonomous-metric observations at their native owners,
  without recreating controller state for parity or inventing missing values.

The outage, targeted DDL and bounded-throughput proofs are closed. Keep their
assertions and regular CI coverage, not another acceptance campaign. Fix
concrete integration failures alongside the work above, but do not make
unrelated fixture cleanup a prerequisite for progressing the deployment model.
Batch corrections to shared fixture setup instead of rediscovering the same
prerequisite per test.

Scope and working rules

The design owns the contracts. Mechanisms and factoring within them are yours
to choose. Build coherent production paths, not speculative scaffolding. Bring
consequential behavior changes or disproportionate cost to Aljoscha and pause
the affected work. Routine implementation decisions need no renewed approval.

Existing-environment conversion, pre-feature binary compatibility and arbitrary
concurrent serving adapters remain outside M2. M3 owns independent adapters and
process management. Add durable records only for required information that
cannot be derived. Production test-only APIs need approval. Do not tune grace
periods or leases, weaken required safety checks, or invent scheduling
guarantees to turn an unexplained failure green.

Follow repository instructions and skills. Use regular draft-PR CI as the
default integration loop, including mzcompose and performance workloads. Run
cheap local formatting/checks and targeted tests where useful. Verify changed
contracts at their boundaries and use existing regressions where they suffice.
Compare disputed behavior against the baseline. Seek independent review when
warranted. Report failed, pending and unverified results explicitly.

Preserve contracts, not the test inventory. Remove obsolete or vacuous tests
rather than preserving them with weaker or tautological assertions. Add
replacement coverage only for a meaningful gap in a current contract.

Keep detailed implementation and validation status in the PR description.
Maintain the current handoff in place with only live work, unresolved decisions
and the next useful step. Remove resolved or superseded items rather than append
a diary. Do not add CI results or tool status, or append to the archived log.
Design and prompt bodies are designer-owned unless documentation work is
explicitly assigned. Flag resolved steering for removal. Re-read this prompt
after compaction.

Commit coherent changes with jj and continue beyond intermediate commits. You
may commit and push to the bookmark above without asking. Before finishing,
fold your own fixups and handoff updates into a straight implementation history
while preserving useful review boundaries. Check remote changes before pushing.
Ask before rewriting others' commits or commits others have built on.

Keep the PR as a draft. Do not enable nightly validation, merge, mark ready, or
push to upstream or other branches without asking.
```
