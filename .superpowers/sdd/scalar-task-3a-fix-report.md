# Task 3a fix report

**Status:** Complete. All changes applied, tested, and committed.

**Commit:** `a87d6e03b3` — `eqsat: harden flatten_assoc termination (operand cap + cycle regression test)`

**Cap constant:** `FLATTEN_MAX_OPERANDS = 4096` (above MAX_ENODES=600; well above any realistic predicate width).

**New tests:** `test_flatten_assoc_cycle_or_terminates` and `test_flatten_assoc_cycle_and_terminates` added in `rules.rs`. Both pass in ~5ms.

**Test totals:** 276 tests in mz-transform (all pass); 56 scalar rule tests (all pass, up from 54 before this change).

**Changes applied:**
1. `FLATTEN_MAX_OPERANDS` const + defensive operand-vector cap with NOTE comment in `flatten_assoc`.
2. Two deterministic cycle regression tests (OR and AND duals), including `assert_flat` helper.
3. Doc comment updated to note unconditional-soundness argument generalizes to all `is_associative` variadics.

**Concerns:** None. No prior tests weakened; `bin/fmt` and `cargo check -p mz-transform --tests` both clean.

**Report path:** `/home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer/.superpowers/sdd/scalar-task-3a-fix-report.md`
