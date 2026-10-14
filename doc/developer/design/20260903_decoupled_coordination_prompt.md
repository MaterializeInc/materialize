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

Read protection belongs to deployment-qualified adapter and replica realizations,
not expiring client registrations. Implement explicit execution replacement and
retirement on DROP, scale-down, deployment retirement and cancelled prewarming.
Silence retains protection, potentially indefinitely. A successor supersedes its
own realization's execution, not other catalog writers. Stale executions cannot
restore retired grants or overwrite their successor's protection. Remove obsolete
renewal/reclamation machinery, rather than tuning or rescheduling heartbeats.
M2 needs one deployment-owned adapter realization, not M3's full adapter lifecycle.

Kafka uses ordinary single-replica placement within the active deployment, not
oldest-client election or heartbeat expiry. Restart recovers on the selected
replica. Check current catalog authority on each producer open/reopen and retain
Kafka fencing, committed-progress recovery and sink-version checks. A delayed old
open may cause a transient producer restart. Do not add a lease or wait for old
process death to prevent that. Prewarming must not open producers or block the
active deployment's selection.

Preserve native prewarming and compatible-version warm handover: a second
deployment warms on its own catalog-described replicas while the first serves,
then retains that execution and protection through externally authorized promotion.
Retirement revokes only the old deployment's authority and protection. Catalog
membership does not grant output-write authority. No borrowed deployment identity
or controller-to-native cold bridge.

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

The outage, targeted DDL and bounded-throughput proofs are closed except where
the protection-lifetime contract changes. Replace expiry-dependent expectations
with explicit replacement/retirement and durable-hold coverage in existing fixtures.
Keep regular CI coverage, not another acceptance campaign. Removing renewal does
not establish SQL or frontier-advancement progress. Fix concrete integration
failures, but do not make unrelated fixture cleanup a prerequisite for this work.
Batch corrections to shared fixture setup instead of rediscovering the same
prerequisite per test.

Handle nonstructural frontier conflicts at the existing catalog transaction
boundary, retaining valid preparation and input holds. This includes moved
compaction bounds, not just peer grants. Apply committed diffs and their effects,
then recompute affected timestamp/protection decisions against that prefix.
Bound immediate retry work and yield with backoff without discarding valid
preparation. Keep structural validation and the 1s publication default. Do not add
another retry framework or a general dependency-tracking scheme.

Scope and working rules

The design owns the contracts. Mechanisms and factoring within them are yours
to choose. Build coherent production paths, not speculative scaffolding. Bring
consequential behavior changes or disproportionate cost to Aljoscha and pause
the affected work. Routine implementation decisions need no renewed approval.

Existing-environment conversion, pre-feature binary compatibility and arbitrary
concurrent serving adapters remain outside M2. M3 owns independent adapters and
process management. Add durable records only for required information that
cannot be derived. Production test-only APIs need approval. Persist reader leases
remain separate from catalog protection lifetime. Do not tune them, statement
deadlines or publication cadence, weaken safety checks, or invent scheduling
guarantees to turn an unexplained failure green.

Debugging and verification

Separate diagnosis from acceptance. On disposable scratch environments, use
smaller check subsets, shorter runs and reversible A/B patches to answer a
specific question. Choose a hypothesis and distinguishing measurement before
each run. Confirm that a reduced case retains the relevant cost or failure.
These routine experiments need no renewed approval within the approved resource
budget. Trying a candidate does not require proving its benefit beforehand or
making it ready to land. Keep ineffective experiments out of the landing history.

Use existing metrics, CPU/heap profiles and narrow diagnostics before adding
instrumentation. Check the effective runtime configuration, not just CLI flags.
Keep owned fixture services available for direct inspection when useful, then
use clean state for fresh comparisons. Reuse the run recipe and capture related
observations together rather than requiring a full run for each measurement.
Triage other failures without letting unrelated fixture issues displace the
main blocker.

Unchanged-workload requirements in the handoff apply to acceptance, not these
exploratory runs. Keep correctness checks, Persist leases, statement deadlines
and the 1s publication cadence intact. A reduced or differently resourced pass
does not close the original failure. Verify a promising repair against the
original workload, concurrency and resource budget before claiming resolution.

Follow repository instructions and skills. Use the dedicated scratch worker for
focused builds, reproductions and profiling. The shared editing VM stays lightweight.
Use bin/scratch with misc/scratch/decoupled-coordination.json: one x86_64 worker,
16 vCPUs, 64 GiB RAM and 500 GiB disk. Reuse its checkout and build caches, keep
the default 36-hour expiry, and record its ID and expiry with the run evidence.
Preserve useful results before expiry. Ask before increasing size, count or lifetime.
Run bounded work and analysis through an established remote session where practical
to avoid repeated authentication gates. Reconnection still requires valid access.

Keep canonical edits in this workspace. Sync an explicit Git commit to an idle,
clean worker checkout: bin/scratch push force-checks out the supplied revision.
Do not overwrite remote work or change source under a running build/test.
Use repository build defaults, adjusting concurrency from observed resource
pressure rather than arbitrary caps. Keep unrelated heavy workloads off the worker
during measurements. Use repository entry points and matching-architecture CI
artifacts where available, not a new debugging framework or acceptance suite.

Keep regular draft-PR CI for broad integration and final combined verification.
Verify changed contracts at their boundaries and use existing regressions where
they suffice. Compare disputed behavior against the baseline. Seek independent
review when warranted. Report failed, pending and unverified results explicitly.

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
