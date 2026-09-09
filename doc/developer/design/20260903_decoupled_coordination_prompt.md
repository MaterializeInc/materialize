# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and its implementation log, then inspect the current code,
worktree, and remote bookmark. Preserve existing work and account for progress
from other sessions. Use the latest handoff and current code to identify the
active milestone in the design's Implementation and verification section. Choose
the next coherent piece toward its observable outcome, briefly explain that
choice, then implement and verify it. Historical proposals and next steps are
context, not a cumulative task list.

Current steering

Check which of these review findings remain unresolved, then choose one coherent
change. Remove resolved steering from this prompt. These are implementation
priorities, not new design requirements or a reason to reopen agreed decisions.

- Maintained protection: milestone 1 remains active. The protected-MV path is an
  implementation checkpoint, not completion across maintained object types. Make
  source/sink recovery requirements constrain catalog authorization even when
  local controller accounting is absent, and bring maintained compute compaction
  under committed permission. Follow the milestone's requirement-derivation and
  recovery-semantics boundaries rather than prescribing new records for every type.
- Publication scaling: publish_read_protection emits one catalog op per record,
  while Transaction::get_op_updates scans accumulated pending updates after each
  op. This creates quadratic work on the coordinator loop. Address that path and
  measure publication cost at representative collection counts, including DDL
  latency and retained history. Do not infer scalability from the small recovery
  demonstration or make a general catalog redesign a prerequisite.
- Consistency-check coupling: check_catalog_state_quiesced changes the publication
  interval, and ddl_revision introduces a production conflict exception for that
  setting. Revisit whether the checker is forcing unnecessary production semantics.
  Preserve full catalog comparison, including protection records, without adding
  further special cases to make the harness pass.
- Ownership transition: after milestone 1, prioritize a real catalog subscriber
  over more standalone APIs. In that transition, address prepare_state's reliance
  on locally installed collections and make the writer-side responsibility for
  complete maintained requirements explicit. An MV's requirement is a separate
  sequencer op today. These are transitional dependencies, not evidence that the
  current single-owner path fails.

Keep the draft PR description accurate about what is implemented and what remains,
with validation status in the PR rather than the design log.

Prefer connecting existing pieces through the active milestone's production path
over adding further standalone APIs. Preparatory work is appropriate when it
unblocks that path. Let integration evidence refine intermediate interfaces
rather than adding machinery to preserve them.

Treat the design as the agreed boundaries, not a prescribed mechanism. Prefer
the smallest coherent solution that preserves the full capability. Incremental
progress is fine, but do not mistake an intermediate step for completion.
Implementation choices within the agreed boundaries do not require renewed
design approval.

Bring discoveries, consequential tradeoffs, and scope growth to me, Aljoscha.
Pause affected work when guidance is needed rather than silently narrowing
scope, adding machinery, or changing an agreed boundary.

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

Append only a minimal dated handoff to the design's log: consequential findings
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
