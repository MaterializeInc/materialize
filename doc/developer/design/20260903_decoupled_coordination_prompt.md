# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and doc/developer/design/20260903_decoupled_coordination_log.md,
then inspect the current code, worktree, and remote bookmark. Preserve existing
work and account for progress from other sessions. Use the latest handoff and
current code to identify the active milestone in the design's Implementation and
verification section. Choose the next coherent piece toward its observable outcome,
briefly explain that choice, then implement and verify it. Historical proposals
and next steps are context, not a cumulative task list.

Current steering

Check which of these review findings remain unresolved, then choose one coherent
change. Remove resolved steering from this prompt. These are implementation
priorities, not additional design requirements.

Milestone 1 remains active. Source/sink authorization and maintained compute
permission are connected. What remains is removing machinery that grew beyond
the design, recovery liveness, and performance evidence. Items 1 and 2 remove
code and should go first.

Items 1 and 2 are paused for the first-installation authority and infeasible
reconstruction decisions described in the latest handoff. A hard upper bound
does not by itself guarantee a readable installation timestamp.

1. Permission as an as_of constraint: the design now settles that committed
   permission is a hard upper constraint on installation `as_of`, applied in
   as-of selection. Remove the read-only wait loop in index authorization and
   the per-second re-delivery of all index bounds. Read-only bootstrap then
   installs within permission on every cluster without waiting on the writer,
   which also resolves the blocked-cluster prewarming question. Keep concurrent
   drops during bootstrap correct.
2. Index bound birth records: an index bound record at birth always says MIN and
   carries no information. Let an index's bound appear at first publication, and
   remove the lifecycle threading through item, system-mapping, introspection,
   and cluster mutation paths together with the whole-catalog `index_ids` scans
   in validation. Fold as-of authorization into ordinary publication so CREATE
   INDEX is one durable commit and `ship_new_dataflow` no longer commits from
   inside an implication batch. Keep monotonicity and no-regression.
3. Test-only production surface: `freeze`/`freeze_at`, `FrozenReadonly`,
   `Command::CatalogDumpSnapshot`, `dump_upper`, the `mz-catalog-upper` header,
   and `publish_interval = 0` as a pause switch exist for the testdrive
   consistency checker and test workflows. Decide once, with Aljoscha, what the
   checker actually needs, then remove the rest. Do not add further diagnostic
   surface to production traits or the coordinator command enum.
4. Publication scaling: per-tick work and transaction validation must scale with
   changed records, not with all collections or items. Then finish representative
   measurements using the existing workflow, including DDL latency, subscriber
   lag, and retained history, and record the numbers in the PR. Distinguish
   publication overhead from workload and observer costs. Do not make a general
   catalog redesign a prerequisite.
5. Add a targeted test for final sink execution as-of selection using aggregate
   readability rather than policy permission, including ungoverned collections.
6. History: the 09-09 commits, including `wip: Integrate maintained recovery
   protection`, are not yet one coherent story. Squash them before milestone 2
   work begins.
7. Ownership transition: prioritize a production maintained-lifecycle subscriber
   over more standalone APIs. The bound-only `CatalogSubscriber` is not that
   outcome and should either grow into it or take a narrower name. Address
   creator-local plans, prepare_state's reliance on locally installed collections,
   and writer-side responsibility for complete maintained requirements: the
   sequencer supplies an MV's requirement as its own op, and the transaction
   validates but does not derive it. These are transitional dependencies, not
   evidence that the current single-owner path fails.

Keep the draft PR description accurate about what is implemented and what remains,
with validation status in the PR rather than the design log.

Standing rules

- No production trait methods, coordinator commands, or system-variable semantics
  whose only consumer is a test harness. Bring the need to Aljoscha first.
- A durable record is added only when it carries information that cannot be
  derived from existing catalog state or durable progress. A record whose value
  is constant at birth is a signal to look again.
- Milestone 2 starts with MV compute installation from committed state and a real
  catalog subscriber. Client protection stays in milestone 3. Do not pull its
  machinery forward.

Prefer changes that remove a dependency on the originating adapter and demonstrate
that through a production path. Preparatory work is appropriate when it unblocks
that path. Temporary interfaces may be replaced as integration clarifies the
boundary. Do not add parallel machinery merely to preserve them or keep milestone
boundaries tidy. Keep maintained requirements object-owned and client protection
incarnation-scoped, without durable per-query or per-SQL-session bookkeeping.

Treat the design as the agreed boundaries, not a prescribed mechanism. Prefer
the smallest coherent solution that preserves the full capability. Incremental
progress is fine, but do not mistake an intermediate step for completion.
Implementation choices within the agreed boundaries do not require renewed
design approval. Catalog fields, leases, protocol messages, planning placement,
and process topology remain implementation choices.

Bring discoveries, consequential tradeoffs, and scope growth to me, Aljoscha.
Pause affected work when guidance is needed rather than silently narrowing
scope, adding machinery, or changing an agreed boundary. Bring disproportionate
implementation cost to me even when the implementation conforms to the design.

Follow repository instructions and skills. Verify at the changed boundaries,
and seek independent review when the risk warrants it.

Use the draft PR's CI as the default test loop instead of running test suites
locally. You may push changes and iterate on CI failures. Keep cheap local
formatting and checks, and run targeted local tests when useful, not as a
prerequisite for pushing. Follow the mz-debug-ci skill when investigating CI.
Report pending or failed checks explicitly rather than treating a push as
successful validation.

Locally, `bin/fmt`, `cargo check`, and the Rust parts of `bin/lint`
(check-cargo, check-formatting, check-python-docs) work. `bin/lint` also runs
checks whose tools are not installed here (npm, helm-docs, trufflehog, zizmor);
CI is the authority for those. Do not record local tooling gaps anywhere.

Focus on regular PR CI for now. Nightly intentionally does not run on this
draft PR, so do not treat its absence as a blocker or try to enable it. We will
start nightly validation once we have a working implementation.

Normally finish the session with one coherent change. Give it a clear commit
message and change description explaining the outcome, rationale, and validation
status, not the chronology of attempts. If blocked, report the blocker rather
than claiming completion.

Append only a minimal dated handoff to
doc/developer/design/20260903_decoupled_coordination_log.md: consequential findings
or decisions, unresolved questions, and the next useful step. Do not record
validation status there at all: CI results, pending checks, formatting or
compile checks, tool availability, and review outcomes are reconstructible from
the PR and are noise in the log. Distinguish proposals from decisions we
reviewed together. Do not rewrite earlier entries. Keep the main design focused
on design, and change its agreed boundaries only after discussing them with me.

You may commit and push progress to this bookmark without asking again.
Prefer jj. In-progress commits and pushes are allowed while iterating. Before
finishing, squash your session's intermediate commits, including fixes and log
updates, into one coherent commit.
You may rewrite and repush your own session's work-in-progress commits for
this purpose. Check for remote changes before pushing. Do not overwrite
others' work, and ask before rewriting other sessions' commits or commits
that others have built on. Report validation failures honestly.

Keep the existing PR as a draft. Do not merge, mark it ready, or push to
upstream or other branches without asking.
```

## Code navigation

- [Catalog implications](../../../src/adapter/src/coord/catalog_implications.rs)
  derive effects from committed changes. MV and metric-sink compute installation
  still have sequencer-side paths.
- [Compute protocol](../../../src/compute-client/src/protocol/command.rs) mixes
  lifecycle and query commands. [Transport](../../../src/service/src/transport.rs)
  replaces the active client on a new connection.
- [StorageCollections](../../../src/storage-client/src/storage_collections.rs)
  owns storage capability accounting and critical since handles. The fixed
  critical-reader identity and epoch fencing support handover, not independent
  owners aggregating their local holds.
