# repr::adt

PostgreSQL-compatible abstract data type implementations. Each file owns one
ADT family and matches PostgreSQL semantics for that type.

## Files (LOC ≈ 11,937 across this directory)

| File | What it owns |
|---|---|
| `datetime.rs` | Date/time parsing, formatting, casting, timezone ops (3,923 LOC — largest) |
| `interval.rs` | `Interval` struct and all arithmetic/parsing |
| `mz_acl_item.rs` | PostgreSQL ACL item encoding (privilege bitmasks, grantee/grantor) |
| `timestamp.rs` | `CheckedTimestamp<T>` wrapper with overflow-safe arithmetic |
| `range.rs` | Generic `Range<T>` (lower/upper bounds, inclusive/exclusive, empty) |
| `numeric.rs` | Arbitrary-precision decimal via `dec` crate; rounding modes |
| `jsonb.rs` | JSONB encoding/decoding on top of `serde_json` |
| `array.rs` | Multi-dimensional PostgreSQL array layout |
| `regex.rs` | Compiled `Regex` wrapper with serialization |
| `char.rs` / `varchar.rs` | Length-modifier enforcement for `char(n)` / `varchar(n)` |
| `date.rs` | `Date` newtype over `NaiveDate` with PostgreSQL epoch arithmetic |
| `pg_legacy_name.rs` | 64-byte fixed-width `name` type |
| `system.rs` | System OID types (`Oid`, `RegClass`, `RegProc`, `RegType`) |

## Key concepts

- **Datum variants** — each ADT family corresponds to one or more `Datum<'a>`
  variants defined in `scalar.rs`; the `adt/` implementations provide the
  supporting value types and operations those variants hold.
- **PostgreSQL parity** — semantics follow PostgreSQL; divergences are
  documented inline. `datetime.rs` carries the bulk of pg-compat complexity.
- **No intra-ADT dependencies** — each file is a leaf; ADTs depend on `scalar`
  and `row` but not on each other.

## Cross-references

- Types here are used as `Datum` variants in `src/scalar.rs`.
- String-format conversions live in `src/strconv.rs`, not here.
- Generated developer docs: `doc/developer/generated/repr/adt/`.
