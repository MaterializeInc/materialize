<!--
Sync Impact Report
===================
Version change: N/A → 1.0.0 (initial ratification)
Modified principles: N/A (initial)
Added sections: Core Principles (7), Development Standards, Quality Gates, Governance
Removed sections: None
Templates requiring updates:
  - .specify/templates/plan-template.md: ✅ no changes needed (Constitution Check
    section already references constitution file generically)
  - .specify/templates/spec-template.md: ✅ no changes needed (requirements and
    success criteria align with principles)
  - .specify/templates/tasks-template.md: ✅ no changes needed (test-first phasing
    and checkpoint structure align with principles)
Follow-up TODOs: None
-->

# Materialize Constitution

## Core Principles

### I. Correctness First

Materialize is a streaming database. Incorrect results are unacceptable.
Every feature MUST produce correct results under all supported input
conditions before optimizing for performance or ergonomics.

- All SQL semantics MUST match documented PostgreSQL compatibility
  targets or be explicitly documented as deviations.
- Edge cases (NULLs, empty inputs, retractions, error rows) MUST be
  handled, not silently dropped.
- Correctness regressions on `main` are treated as urgent to fix.

### II. Simplicity

Prefer simple solutions to complex ones. Complexity MUST be justified.

- If a function is hard to explain, break it into smaller functions.
- If a change requires extensive documentation to explain, simplify
  the change.
- Rely on Rust's type system to enforce invariants rather than
  runtime checks where possible.
- Follow YAGNI: do not build for hypothetical future requirements.

### III. Testing Discipline

Every change MUST be tested. Materialize uses a multi-tier testing
strategy: unit tests (cargo test), system tests (testdrive,
sqllogictest, pgtest), and long-running stability tests.

- New features MUST include system-level tests (testdrive or
  sqllogictest) covering happy paths and key edge cases.
- Bug fixes MUST include a regression test demonstrating the fix.
- Tests MUST be deterministic and reproducible.
- Test failures on `main` are treated as urgent to fix.

### IV. Backward Compatibility

Materialize follows a lifecycle (Internal Development, Private Preview,
Public Preview, GA). Changes MUST respect the stability guarantees of
each phase.

- Breaking syntax changes MUST go through migration procedures.
- New features MUST be gated behind feature flags (disabled by
  default) until they reach the appropriate lifecycle phase.
- Wire protocol and catalog changes MUST be forward-compatible or
  include explicit migration paths.

### V. Performance Awareness

Engineers MUST understand the scaling characteristics of the code they
write and make conscious decisions about performance trade-offs.

- Document time and space complexity for non-trivial algorithms.
- Quadratic or worse algorithms MUST include a comment justifying
  the acceptable input size range.
- Performance regressions on `main` are treated as urgent to fix.
- Benchmarks SHOULD be added for performance-sensitive paths.

### VI. Code Clarity

Write code for others, not only for yourself. Code MUST be
understandable by someone unfamiliar with the specific subsystem.

- Follow the project style guide (SQL capitalization conventions,
  Rust idioms, clippy lints).
- Each PR MUST be a single semantic change, reviewable as one unit.
- Architectural decisions SHOULD be documented so maintainers do
  not need the original author present to succeed.

### VII. Observability

Production systems MUST be debuggable without code changes.

- Structured logging and tracing spans MUST be present for
  significant operations (source ingestion, query execution,
  sink production).
- Error messages MUST guide the user toward resolution, not just
  report the failure.
- Internal metrics SHOULD be exposed for capacity planning and
  alerting.

## Development Standards

- **Language**: Rust for all core services. Python and Bash for
  build/test tooling.
- **Formatting**: `cargo fmt` and `clippy` MUST pass. Use `bin/fmt`
  and `bin/lint` for project-wide checks.
- **Dependencies**: New crate dependencies MUST be justified. Prefer
  existing ecosystem crates over vendoring.
- **Documentation**: User-facing features MUST have reference docs.
  Release notes MUST be updated in `doc/user/release-notes.md`.

## Quality Gates

Every PR MUST pass the following before merge:

1. **CI green**: All automated test suites pass.
2. **Code review**: At least one reviewer familiar with the affected
   subsystem approves the change.
3. **No correctness regressions**: Existing tests continue to pass
   with no behavioral changes unless intentional and documented.
4. **No performance regressions**: Feature benchmarks SHOULD be run
   for performance-sensitive changes.
5. **Documentation**: User-facing changes include updated docs and
   release notes.

## Governance

This constitution supersedes informal practices where conflicts arise.
Amendments require:

1. A PR modifying this file with a clear rationale.
2. Review and approval by at least one project maintainer.
3. Version bump following semantic versioning:
   - MAJOR: Principle removal or incompatible redefinition.
   - MINOR: New principle or material expansion.
   - PATCH: Wording clarification or typo fix.
4. Update of `LAST_AMENDED_DATE` to the merge date.

All PRs and code reviews SHOULD verify compliance with these
principles. When a principle is violated, the violation MUST be
explicitly justified in the PR description.

**Version**: 1.0.0 | **Ratified**: 2026-03-05 | **Last Amended**: 2026-03-05
