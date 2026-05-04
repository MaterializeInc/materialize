# Architecture review — `sql-parser::ast::defs`

Scope: `src/sql-parser/src/ast/defs/` (≈ 10,256 LOC of Rust).

## 1. `WithOptionName::redact_value` has no compile-time enforcement

**Files**
- `src/sql-parser/src/ast/display.rs:241` — `WithOptionName` trait; default
  `redact_value()` returns `true` (conservative).
- `src/sql-parser/src/ast/defs/statement.rs` — 32 `impl WithOptionName for
  XxxOptionName` blocks, each with a manual `match self { … => false, … =>
  true }` or `_ => true` arm.
- `src/sql-parser/src/ast/defs/ddl.rs` — 14 more such impls.

**Problem**
Adding a new variant to an option-name enum (e.g. `CreateSinkOptionName`) does
not cause a compile error if the corresponding `WithOptionName` impl uses a
wildcard arm (`_ => true`). The new variant silently defaults to "redact" —
which is the safe direction — but the codebase also has impls that match
specific non-redacted variants and use `_ => true`. Adding a non-sensitive
variant to such an enum will correctly redact it without any warning that the
impl may need updating. The inverse failure — adding a *sensitive* variant to
an enum whose impl defaults to `false` — would be a PII leak.

The doc comment in `display.rs` addresses this via the "honor system" (quoted
verbatim in the trait doc).

**Solution sketch**
Replace wildcard `_ => true` arms with exhaustive matches across all 36 impls.
The compiler will then surface any unhandled variant when one is added to the
option-name enum, forcing an explicit redaction decision at the point of the
new variant. This is a mechanical, low-risk change: `cargo check` will catch
every missed site.

**Deletion test passes**: the change is additive — it removes wildcards and
adds arms, not behavior.

**Risk**: low. All 36 impls can be updated in one pass. The default-`true`
trait default remains correct for any impl that forgets to override the method.

## 2. (Honest skip) 6 child modules in `defs/`

The split (name, value, expr, query, statement, ddl) maps to orthogonal SQL
grammar regions. `statement.rs` is large (5,768 LOC) but is a coherent
collection of statement structs; it cannot be meaningfully split further without
fragmenting the `Statement<T>` enum across files. Deletion test fails: collapse
would be 10K LOC in one file. The current split is correct.

## 3. (Honest skip) `#[allow(clippy::large_enum_variant)]` on `Statement<T>`

The annotation is correct: the enum has ~80 variants and the largest variant is
materially bigger than average. Boxing the large variants would be a perf change
(heap allocation per parse of a large statement), not an architecture change.
The existing suppression is the right call.
