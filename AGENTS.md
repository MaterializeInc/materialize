# Materialize

## Repository layout

`console/` is the web Console. It was moved in from the standalone
`MaterializeInc/console` repository, which is now archived and stale.

## Dependency management

Use the `mz-dependencies` skill before changing dependencies. Never regenerate
the full Cargo.lock and never run bare `cargo update`.

## Merging

The repository allows only squash merges. Merge commits and rebase merges are
disabled in the GitHub settings. Every PR lands on `main` as one commit, with
the PR title as subject and the PR body as message. Intermediate commits on a
PR branch never reach `main`, so `git bisect` never visits them. Do not ask
authors to squash on merge or to keep every intermediate commit green.

## Guidance

* When designing specs or implementing features, preserve the full scope and
  capability of the solution. Do not substitute dynamic or generative
  approaches with hardcoded data, skip automation that a reference
  implementation provides, or simplify away the parts that make a feature
  robust and maintainable. If a reference codebase generates data from an
  authoritative source, our implementation should do the same, not ship a
  static snapshot. When you feel tempted to reduce scope or take a shortcut,
  flag it to the user and workshop an alternative together rather than silently
  downgrading the design.
* Before reporting success on a code change, run the cheap formatters and
  checkers. For Rust, these are `bin/fmt` and `cargo check`. Before committing,
  run the pre-commit checklist in the `mz-commit` skill.
* When debugging CI or lint failures, start by reproducing the exact failing
  command locally and reading its output. Do not run generic checks (clippy,
  fmt, grep) in a shotgun approach.
* We value simplicity and clear abstractions. Simplicity means fewer concepts
  and moving parts, not less capability. Take the most care over the boundaries
  between components: the interfaces that traits, modules, and crates expose.
* Avoid em-dashes for structuring sentences in any prose: code comments, specs,
  design docs, all of it. Restructure with full stops and commas instead. The
  same goes for semicolons, though one is fine where splitting would mangle the
  sentence.
* Apply this guidance to the code and designs you write. When existing code
  you are working on deviates from it, do not fix it in passing. Keep the
  change minimal, call out the deviation, and let the user decide what to do
  about it.
* Never put a side effect inside `debug_assert!` (or `debug_assert_eq!`, etc.).
  The macro body compiles out whenever debug-assertions are off, which includes
  `[profile.optimized]` (what `bin/environmentd --optimized` and mzcompose use)
  and `[profile.release]`. Only the dev profile and `[profile.ci]` turn them on.
  A side effect written there, for example
  `debug_assert!(map.insert(k, v).is_none())`, runs under `cargo test` but
  silently vanishes in optimized and release builds, leaving the logic it
  powered inert and profiler-blind. Bind the effect to a `let` outside the
  assert, then assert on the bound value. Clippy's `debug_assert_with_mut_call`
  catches `&mut self` receivers but not `&self` or free-function side effects.
* Never write vendor, customer, or account names into durable or user-facing
  surfaces: committed code, comments, column comments, docs, specs, commit
  messages, PR bodies, or test fixtures. Anything persisted to disk gets shared,
  indexed, and outlives the context, so a name in it is a leak. Anonymize to the
  technical pattern instead, for example "a wide unfiltered LEFT JOIN driving a
  freshness incident" rather than who hit it. Names are fine in ephemeral chat,
  not on disk.
* Never use `std::collections::HashMap`/`HashSet` directly, `clippy.toml`'s
  `disallowed-types` blocks it. Use `BTreeMap`/`BTreeSet` when iteration order
  matters, or `mz_ore::collections::HashMap` for keyed-only access with no
  iteration. Hash-order iteration is a real nondeterminism source, for example
  an unstable order reaching plan output or persisted state, not a style nit.
* Choosing whether a new feature flag defaults on or off in production is a
  product decision. When adding a flag, ask a human which default to use, and
  use the `mz-feature-flags` skill. When reviewing a PR, do not question the
  default it chose.

## Code comments

Spend comments on the non-obvious: concurrency and async hazards (races,
lease/handle expiry, values that must not be held across an await point),
ordering constraints ("X must happen before Y, else Z"), invariants whose
violation panics or corrupts data, restart/recovery semantics, the origin of
magic constants, and why the alternative a reader would otherwise assume was
not taken. Idiomatic code (match arms, iterator chains, getters, logging) needs
none.

A doc comment is the caller's contract: a one-sentence summary, then only the
invariants and semantics a caller must know. A self-evident public item needs
only that one line. Don't narrate the body in rustdoc ("Phase 1 ... Phase
2 ...", or enumerating a flag's branches). Reasoning about how or why the code
works goes in an inline `//` at the decision point, or in a module-level `//!`
when it is about how the pieces fit together. Document a struct field only when
its meaning is subtle.

Every fact gets exactly one owning comment: the decision point for reasoning,
the mechanism for mechanism docs, the public setter or constant for config
semantics. Everywhere else either points at the owner or says nothing. The
tell is the same clause appearing near-verbatim in a module doc, a struct doc,
and an inline comment. When that happens, pick the owner and cut the rest. In
particular: don't restate a callee's documented contract at its call sites,
don't narrate in one function's doc what a sibling's doc already owns, and
don't duplicate an assert message in a comment on the same line.

Budget doc-comment paragraphs by decisions: each paragraph should carry a
distinct decision or invariant, not re-argue one documented elsewhere or
enumerate the item's callers or fields. Skip speculative comments about what
a future implementation could do (one `TODO` at the owning seam is enough),
and skip comments explaining an absence when the surrounding docs already
imply it. No ASCII section-divider banners (`// ----- helpers -----`). Item
placement carries the structure. Performance claims in comments should state
a constraint or a measured number, not unverifiable color ("the compiler can
autovectorize this").

A comment must make sense from the code around it, not from previous changes.
Don't refer to earlier states of the code or to future PRs, unless the history
explains why the code behaves as it does and that knowledge needs recording.

Before keeping a sentence, ask whether a maintainer would get something wrong
without it. If deleting it changes nothing they would do or believe, delete it,
however true it is. Prefer deleting a sentence to rewording it: shortening the
words while keeping every fact is not editing.

A comment that asserts a constraint ("X is not possible, because Y") has to be
checked when it is written, by a compile probe, a test, or a pointer to what
enforces it. An unchecked constraint outlives its truth. It reads as fact,
nobody retests it, and later work gets designed around a limit that is no
longer there. Where the constraint cannot be checked, write what was observed
instead of what is impossible.

Avoid "X rather than Y" unless Y is what a reader would otherwise assume. Where
Y is not a live prior it is unconstrained and unfalsifiable, and it rots
silently, since the code can move to Y with the sentence still reading
correctly. Where Y is a real trap, assert the failure instead of the contrast
("blocking on this future deadlocks, because `drop` runs on the worker thread
that has to poll it"), which gives the claim truth conditions. A failure mode
that cannot be named means the comment records a preference and not a reason.

The same economy applies to tests: a test whose name and assert messages
state the property needs no doc comment. Keep test docs for non-obvious
setup, fixtures whose shape encodes the scenario, and multi-phase protocols.

Mark counterintuitive gotchas with `NOTE:` and future work with `TODO:`.
