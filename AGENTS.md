# Materialize

## Skills

Canonical agent skills in `.agents/skills/`. `.claude/skills` is compat symlink
for Claude Code. Check `mz-*` skill before tasks — encodes project conventions,
saves time.

Use the `mz-test` skill before running ANY tests, even mid-task — the canonical
commands aren't the obvious ones (e.g. `bin/sqllogictest --optimized`, not
`cargo build --bin sqllogictest`).

## Code navigation

For operation flow tracing, read first:

* `doc/developer/generated/flows.md` — maps operations (query lifecycle, source ingestion, MV creation, sink lifecycle, catalog DDL, timestamp selection, persist read/write, controller architecture) to `crate::module` paths in execution order.
* `doc/developer/generated/<crate>/_crate.md` — per-crate overview: modules, key types, dependencies.
* `doc/developer/generated/<crate>/<module>.md` — per-file docs.

> **READ-ONLY: `doc/developer/generated/` is generated, not authored.**
> The entire `doc/developer/generated/` tree is maintained exclusively by the
> recurring documentation agent, which runs the `update-docs` skill/command.
> That agent is the *only* session permitted to create, edit, or delete files
> under this directory.
>
> In any other session: **treat `doc/developer/generated/` as read-only.** Use
> it for navigation and context, but never edit, create, delete, or regenerate
> files there — not even to "fix" something you noticed, and not as part of an
> unrelated change. These files carry `source`/`revision` front-matter that the
> recurring agent manages; hand edits desync that bookkeeping. If a generated
> doc is wrong or stale, report it in your response rather than editing it, and
> leave the correction to the `update-docs` agent. Do not stage or commit any
> path under `doc/developer/generated/` unless you are explicitly running the
> `update-docs` workflow.

## Dependency management

### Workspace dependencies

All third-party versions declared in `[workspace.dependencies]` in root
`Cargo.toml`. Members use `dep.workspace = true` or
`dep = { workspace = true, optional = true }`.

* **Add new dep**: add to `[workspace.dependencies]` in root `Cargo.toml` first, then `dep.workspace = true` in member crate.
* **Never inline version** in member `Cargo.toml` — `bin/lint-cargo` enforces.
* **Update version**: change once in root `Cargo.toml`.
* Cargo unifies features workspace-wide, so workspace declaration is union of all features across crates. Members just use `dep.workspace = true`.

### Cargo.lock

Never regenerate full Cargo.lock. When changing deps:

* **Add dep or change features**: run `cargo check` — updates only what changed.
* **Update specific crate**: `cargo update -p <crate>` (optional `--precise <version>`).
* **Never bare `cargo update`** — bumps every semver-compatible dep, causes unrelated breakage from transitive changes.
* **If lock regenerated**, diff before commit (`git diff Cargo.lock | grep '^[+-]version'`) and pin back unintended bumps with `cargo update -p <crate> --precise <old-version>`.

### Licensing

Two files control license policy, **keep in sync**: `deny.toml` (`[licenses].allow`) and `about.toml` (`accepted`). New dep with new license not already allowed: add SPDX identifier to both.

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
* When making code changes, run cheap checkers/linters and formatters before
  reporting success and/or committing changes. For Rust, these would be
  `bin/fmt` and `cargo check`.
* When debugging CI or lint failures, start by reproducing the exact failing
  command locally and reading its output. Do not run generic checks (clippy,
  fmt, grep) in a shotgun approach.
* We value simplicity and clear abstractions. We especially care about
  designing the interfaces or boundaries between components well. This includes
  components, traits, interfaces, modules, and crates.
* In code comments or inline documentation, we value clearly described
  contracts and assumptions.
* In code comments, we don't like "fluff" comments, comments that describe what
  code does when it is obvious from the code. Good code should be readable. It
  _is_ okay to call out tricky parts of the code or "nota benes".
* In code comments and code documentation we value concise but complete
  comments.
* In code comments and documentation, don't refer to potential previous states
  of the code, or things like future PRs, try not to use chronology in there,
  except when it's needed to explain why a certain thing behaves as it does and
  we need to record that knowledge. In general comments need to stand on their
  own and make sense from just looking at them and the code around it, not
  previous changes.
* Avoid em-dashes and semicolons for structuring sentences, everywhere: code
  comments, specs, design docs, all of it. Restructure with full stops and
  commas instead. In most cases a sentence that wants an em-dash or semicolon
  can just be split into two.
* Our guidance applies both when writing new code or designs, or when we notice
  deviations in code or architecture that we are working on. At the same time,
  we want to keep our changes minimal so it's good to call out deviations and
  then we can decide together what to do about it.
* Never put a side effect inside `debug_assert!` (or `debug_assert_eq!`, etc.).
  The macro body compiles out whenever debug-assertions are off, which includes
  `[profile.optimized]` (what `bin/environmentd` and mzcompose use) and
  `[profile.release]`. Only `[profile.ci]` turns them on. A side effect written
  there, for example `debug_assert!(map.insert(k, v).is_none())`, runs under
  `cargo test` but silently vanishes in optimized and release builds, leaving
  the logic it powered inert and profiler-blind. Bind the effect to a `let`
  outside the assert, then assert on the bound value. Clippy's
  `debug_assert_with_mut_call` catches `&mut self` receivers but not `&self` or
  free-function side effects.
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
* A new feature flag should default off in production but default ON in the
  test/CI configuration, so the new code path is exercised by sqllogictest,
  testdrive, and optimizer goldens before it earns trust. Production safety and
  test coverage are separate settings. Find how other recently added flags get
  forced on in CI, for example a default-features-for-testing list or a system
  variable test override, and follow that pattern.

## Code comments

Specifics for code comments and documentation:

Spend comments on the non-obvious: concurrency and async hazards (races,
lease/handle expiry, values that must not be held across an await point),
ordering constraints ("X must happen before Y, else Z"), invariants whose
violation panics or corrupts data, restart/recovery semantics, the origin of
magic constants, and why the obvious alternative was not taken. Idiomatic code
(match arms, iterator chains, getters, logging) needs none.

A doc comment is the caller's contract: a one-sentence summary, then only the
invariants and semantics a caller must know. A self-evident public item needs
only that one line. Don't narrate the body in rustdoc ("Phase 1 ... Phase
2 ...", or enumerating a flag's branches). Reasoning about how or why the code
works goes in an inline `//` at the decision point, or in a module-level `//!`
when it is about how the pieces fit together. Document a struct field only when
its meaning is subtle.

Mark counterintuitive gotchas with `NOTE:` and future work with `TODO:`.
