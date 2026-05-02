# mz-sql-parser

Hand-written recursive-descent SQL lexer and parser for Materialize's SQL
dialect. Produces an unresolved `AST<Raw>` from SQL text. Downstream consumers
resolve `Raw` to `Aug` via `mz-sql::names`.

## Files (LOC ≈ 22,226 across this crate)

| File | What it owns |
|---|---|
| `src/lib.rs` | Crate root; public API (`pub mod ast`, `pub mod parser`); `datadriven_testcase` (feature-gated roundtrip harness) (164 LOC) |
| `src/parser.rs` | `Parser` struct and ~290 `parse_*` methods; public entry points `parse_statements`, `parse_statements_with_limit`, `parse_expr`, `parse_data_type`, `parse_item_name`; `ParserError` / `ParserStatementError` (10,249 LOC) |
| `src/ident.rs` | `ident!` macro for compile-time `Ident` construction (112 LOC) |
| `src/ast/` | All AST types, display, fold, visit — see [`ast/CONTEXT.md`](src/ast/CONTEXT.md) |

## Key concepts

- **Entry point** — `parse_statements(sql) -> Vec<StatementParseResult>`.
  `StatementParseResult` carries both the `Statement<Raw>` AST and the original
  SQL slice it was parsed from (used by `mz-sql` for error reporting).
- **Recursive descent** — one `parse_*` method per grammar production.
  Recursion depth is guarded by `CheckedRecursion` / `RecursionGuard` (limit
  128) to prevent stack overflow on pathological inputs.
- **`AstInfo` / `Raw`** — the AST is generic over `AstInfo`. The parser always
  produces `Statement<Raw>`; name resolution in `mz-sql::names` uses
  `Fold<Raw, Aug>` to produce `Statement<Aug>`.
- **`FormatMode`** — `AstDisplay` supports Simple, SimpleRedacted, and Stable
  modes. Stable forces all identifiers to be quoted; used when persisting
  catalog entries. Roundtrip invariant enforced by `datadriven_testcase`.
- **Build-generated traversals** — `mz-walkabout` parses `ast/defs/` at build
  time and emits `visit.rs`, `visit_mut.rs`, `fold.rs` into `OUT_DIR`. Adding
  a new AST node type automatically propagates to all three traversal traits.
- **Batch size limit** — `MAX_STATEMENT_BATCH_SIZE = 1 MB`; enforced in
  `parse_statements_with_limit` before tokenization.

## Bubbled from ast/ and ast/defs/

- **`WithOptionName::redact_value` exhaustiveness gap** (from `defs/`): 36
  `WithOptionName` impls use wildcard arms; adding a new option-name variant
  does not force an explicit redaction decision. Safe by default (wildcards
  return `true`), but the inverse failure — a new sensitive variant in an enum
  whose impl defaults to `false` — would be a silent PII leak. Fix: make all
  impls exhaustive. See `ast/defs/ARCH_REVIEW.md` §1.
- **Flat `ast` re-export** — `mz_sql_parser::ast` exposes all node types in
  one namespace. Consumers do not need to track which sub-module a type lives in.

## Architecture notes

- `mz-sql-parser` has **no planning logic** — it is a pure syntax layer.
  Semantic validation (type checking, name resolution) lives entirely in `mz-sql`.
- The seam between this crate and `mz-sql` is `Statement<Raw>` → `Statement<Aug>`
  via `mz-sql::names::resolve`.
- `mz-sql-lexer` is a separate crate for tokenization; `mz-sql-parser` depends
  on it but does not own it.
- Consumers: `mz-sql` (planning), `mz-sql-pretty` (formatting), `mz-lsp-server`,
  `mz-expr-parser`.

## Cross-references

- `src/sql/src/names.rs` — `resolve` implements `Fold<Raw, Aug>`.
- `mz-walkabout` — build-time visitor generator.
- Generated docs: `doc/developer/generated/sql-parser/`.
