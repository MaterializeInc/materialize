# sql-parser::ast::defs

All struct and enum definitions that constitute the SQL AST, split across six
child modules and re-exported as a flat namespace by `defs.rs`.

## Files (LOC ≈ 10,256 across this directory)

| File | What it owns |
|---|---|
| `defs.rs` | Re-export facade — `pub mod` declarations only (60 LOC) |
| `statement.rs` | Top-level `Statement<T>` enum (80+ variants); per-statement structs for every DDL/DML/query statement; `WithOptionValue<T>`; 32 `WithOptionName` impls (5,768 LOC) |
| `ddl.rs` | DDL-specific option enums/structs for connections, sources, sinks, load generators, Kafka/PG/MySQL/SQL Server config (1,935 LOC) |
| `query.rs` | `Query<T>`, `Select`, `SetExpr`, `OrderByExpr`, `Limit`, `Join`, `TableFactor`; SELECT/MutRec `WithOptionName` impls (835 LOC) |
| `expr.rs` | `Expr<T>` enum — all expression forms (954 LOC) |
| `name.rs` | Identifier and object-name types: `Ident`, `UnresolvedItemName`, etc. (470 LOC) |
| `value.rs` | Literal value types: `Value`, `DateTimeField`, etc. (294 LOC) |

## Key concepts

- **`Statement<T: AstInfo>`** — the top-level union; parameterized on `AstInfo`
  so the same AST types carry either `Raw` (parser output) or `Aug` (resolved,
  from `mz-sql`) name/id information. `#[allow(clippy::large_enum_variant)]`
  acknowledges that the largest variant is significantly bigger than the
  average.
- **`WithOptionValue<T>`** — a single polymorphic value type shared by all 36
  `WITH (…)` option structs. Redaction logic lives here: `Secret` and
  `ConnectionKafkaBroker` are always redacted; other variants are
  redact-aware via `FormatMode`.
- **`WithOptionName` trait** — implemented 36 times (one per option-name enum);
  each impl's `redact_value()` declares whether its value is PII-sensitive.
  Default is `true` (conservative). No compilation check enforces exhaustiveness;
  the trait doc carries a manual audit note.
- **Build-script boundary** — this subtree is parsed by `mz-walkabout` at build
  time to generate `visit.rs`, `visit_mut.rs`, and `fold.rs`. Only pure AST
  types may live here; parser helpers, metadata, and display logic are excluded.

## Cross-references

- Parent `ast/` re-exports everything here; consumers import from
  `mz_sql_parser::ast`.
- `mz-sql::names` implements `Fold<Raw, Aug>` to resolve raw names to IDs.
- Generated docs: `doc/developer/generated/sql-parser/ast/defs/`.
