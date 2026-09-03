# Decoupled coordination: implementer session prompt

```text
Work on doc/developer/design/20260903_decoupled_coordination.md.

Working PR: https://github.com/MaterializeInc/materialize/pull/38696
Bookmark: decoupled-coordination
Remote: origin, pointing to the contributor fork, not upstream

Read the design and its implementation log, then inspect the current code,
worktree, and remote bookmark. Preserve existing work and account for progress
from other sessions. Choose the next coherent piece toward the full outcome,
briefly explain that choice, then implement and verify it.

Treat the design as the agreed boundaries, not a prescribed mechanism. Prefer
the smallest coherent solution that preserves the full capability. Incremental
progress is fine, but do not mistake an intermediate step for completion.

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

Normally finish the session with one coherent change. Give it a clear commit
message and change description explaining the outcome, rationale, and validation
status, not the chronology of attempts. If blocked, report the blocker rather
than claiming completion.

Append only a minimal dated handoff to the design's log: consequential findings
or decisions, validation status, unresolved questions, and the next useful step.
Link evidence rather than recording every CI attempt or duplicating the change
description. Distinguish proposals from decisions we reviewed together. Do not
rewrite earlier entries. Keep the main design focused on design, and change
its agreed boundaries only after discussing them with me.

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
