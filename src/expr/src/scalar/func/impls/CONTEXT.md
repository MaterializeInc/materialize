# expr::scalar::func::impls

Per-type scalar function implementations. One submodule per SQL scalar type (34
submodules total). Functions are annotated with `#[sqlfunc]` or written as
manual `EagerUnaryFunc`/`EagerBinaryFunc` impls; `macros.rs` at the parent level
then collects them into the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums.

## Files (7,445 LOC total)

| File | What it owns |
|---|---|
| `string.rs` (1,309) | String casts, parse functions, `LIKE`/`ILIKE` matching, regex |
| `timestamp.rs` (914) | Timestamp/timestamptz arithmetic, extract, trunc, AT TIME ZONE |
| `float64.rs` (461) | Float64 arithmetic, casts, trig |
| `jsonb.rs` (420) | JSONB operations and casts |
| `numeric.rs` (345) | Numeric arithmetic and casts |
| `list.rs` (320) | List operations and casts |
| `array.rs` (294) | Array operations and casts |
| `record.rs` (239) | Record accessor, cast-record-to-record |
| `int32.rs` (239) | Int32 arithmetic and casts |
| *(22 other type files)* | One file per remaining SQL type |

## Key concepts

- **`#[sqlfunc]`** — proc-macro from `mz-expr-derive` that generates a
  newtype struct implementing `EagerUnaryFunc` (or `EagerBinaryFunc`) from a
  plain Rust function; null-propagation and error properties are inferred from
  the function signature.
- **Manual impls** — types with complex type-inference rules (e.g. `CastStringToArray`,
  `CastList1ToList2`) implement `LazyUnaryFunc` directly.
- **`impls.rs`** (76 LOC) — a pure re-export file; all submodules are declared
  and glob-exported from here.
- **`TODO? if typeconv was in expr`** — several casts (`CastStringToList`,
  `CastArrayToList`) hard-code `introduces_nulls = true` with TODO comments
  noting that the accurate answer depends on `mz-sql::typeconv`, which lives
  outside this crate.

## Cross-references

- Parent `macros.rs` defines `derive_unary!`, `derive_binary!`, `derive_variadic!`
  which collect impls into the three public enums.
- `mz-expr-derive` crate provides the `#[sqlfunc]` proc-macro.
- Generated docs: `doc/developer/generated/expr/scalar/func/impls/`.
