# Network address types

- Associated: (issue TBD)

## The Problem

PostgreSQL has four network address types: `inet`, `cidr`, `macaddr` and
`macaddr8`. Materialize supports none of them. A PostgreSQL source with such a
column can only ingest it through `TEXT COLUMNS`, and users then lose
containment checks (`ip << '10.0.0.0/8'`), correct ordering and the usual
helper functions. Network data (flow logs, firewall and access logs, asset
inventories) is a common streaming workload, and a string comparison on IP
addresses gives wrong answers (`'10.0.0.9' > '10.0.0.10'`).

## Success Criteria

- `inet`, `cidr`, `macaddr` and `macaddr8` exist as first-class types with
  PostgreSQL's text input and output, binary wire format, ordering, casts,
  operators and functions.
- PostgreSQL sources ingest these columns natively, without `TEXT COLUMNS`.
- Tools that introspect `pg_type` (ORMs, BI tools, `psql \d`) see the
  PostgreSQL OIDs and names.
- Values survive persist round-trips, restarts and upgrades. Filter pushdown
  works for range and containment predicates on `inet` and `cidr`.
- PostgreSQL's `inet.sql` and `macaddr.sql` regression cases pass where they
  touch supported features.

## Out of Scope

- GiST and SP-GiST operator classes. Materialize has no index access methods
  of that kind. Containment predicates are evaluated as ordinary filters.
- `inet` aggregate functions not in PostgreSQL core.
- Native Avro or Arrow logical types. Sinks emit the text form. Avro has no
  standard logical type for these, and consumers already handle strings.
- MySQL and SQL Server sources. Neither has equivalent column types.

## Solution Proposal

### Types and datums

| SQL type   | OID  | Array OID | `Datum` variant      | `ReprScalarType` |
|------------|------|-----------|----------------------|------------------|
| `inet`     | 869  | 1041      | `Datum::Inet(Inet)`  | `Inet`           |
| `cidr`     | 650  | 651       | `Datum::Inet(Inet)`  | `Inet`           |
| `macaddr`  | 829  | 1040      | `Datum::MacAddr([u8; 6])`  | `MacAddr`  |
| `macaddr8` | 774  | 775       | `Datum::MacAddr8([u8; 8])` | `MacAddr8` |

OIDs follow PostgreSQL and must be checked against `pg_type` on PostgreSQL 16
before merge.

`inet` and `cidr` share one datum, the same way `varchar` and `text` share
`Datum::String`. PostgreSQL treats `cidr` as binary-coercible to `inet` and
compares the two with the same operators, so `'10.0.0.0/8'::cidr =
'10.0.0.0/8'::inet` is true. The only differences are an invariant and a
display rule, both owned by the SQL type:

- `cidr` values have no bits set right of the mask. Text input rejects them
  (`invalid cidr value ... has bits set to right of mask`), and the
  `inet -> cidr` cast zeroes them.
- `cidr` output always prints `/n`. `inet` output omits it when the mask is
  the full address length.

`Inet` lives in `mz_repr::adt::inet`:

```rust
pub struct Inet {
    addr: IpAddr, // std::net
    masklen: u8,  // <= 32 for V4, <= 128 for V6
}
```

Constructors validate `masklen`. `Ord` follows PostgreSQL's
`network_cmp_internal`: family, then network bits up to the shorter mask,
then mask length, then the full address. That order equals a lexicographic
comparison of `(family, network(addr, masklen), masklen, addr)`, where
`network` zeroes the host bits. The persist encoding below relies on this.
IPv4-mapped IPv6 addresses stay IPv6, as in PostgreSQL.

`macaddr` and `macaddr8` are separate datums because they are distinct types
with their own comparison. `macaddr -> macaddr8` inserts `FF:FE` in the middle,
and `macaddr8 -> macaddr` errors unless bytes 4 and 5 are `FF:FE`.

### Encodings

Three encodings change. Two of them are durable.

1. **In-memory `Row` tags** (`src/repr/src/row.rs`). New tags `Inet`,
   `MacAddr`, `MacAddr8`. `Inet` packs as `family | masklen | addr` (6 or
   18 bytes). This format is not durable and needs no compatibility care.
2. **`ProtoDatum`** (`row.proto`, durable, covered by the buf breaking lint).
   New `oneof` arms with fresh field numbers: `inet` as bytes in the same
   `family | masklen | addr` layout, `macaddr` and `macaddr8` as bytes.
3. **Persist columnar** (`src/repr/src/row/encode.rs`, durable).
   - `inet`/`cidr`: `FixedSizeBinaryArray(34)` holding
     `family | network | masklen | addr`, with IPv4 values zero-padded. The
     family byte orders IPv4 before IPv6 and padding is constant within a
     family, so byte order equals datum order. `preserves_order` returns
     true, and stats use a new `FixedSizeBytesStatsKind::PackedInet`.
     Variable-length byte stats would not work here: persist trims them to a
     short common prefix, which leaves bounds that no longer decode.
   - `macaddr`: `FixedSizeBinaryArray(6)`. `macaddr8`:
     `FixedSizeBinaryArray(8)`. Both preserve order.

   The redundant network bytes and IPv4 padding are the price of an
   order-preserving, fixed-width encoding. Parquet dictionary and run-length
   compression absorb most of it for real address data.

Once a release writes these encodings to persist or the catalog, every later
release must read them. The feature flag below makes sure no data is written
before a release that can read it is fully rolled out.

### Text and binary formats

The parsers are written in-tree on top of `std::net`. No new crate is needed.
`std::net` does not parse `cidr`'s abbreviated input (`'10/8'`, `'192.168'`),
so that is a small hand-written parser.

- `inet`/`cidr` text input: IPv4 or IPv6 with optional `/n`. `cidr` also
  accepts PostgreSQL's abbreviated forms. Output follows `inet_out`/`cidr_out`.
- `inet`/`cidr` binary: `family` (2 for IPv4, 3 for IPv6), `bits`, `is_cidr`,
  `nb`, then `nb` address bytes. This is PostgreSQL's `inet_send` layout.
- `macaddr` text input: every PostgreSQL format (`08:00:2b:01:02:03`,
  `08-00-2b-01-02-03`, `08002b:010203`, `08002b-010203`, `0800.2b01.0203`,
  `0800-2b01-0203`, `08002b010203`). Output is lowercase and colon-separated.
- `macaddr8` input accepts 6- or 8-byte forms. A 6-byte form gets `FF:FE`
  inserted.
- Binary for MAC types is the raw bytes.

### Casts

Mirror PostgreSQL's `pg_cast` rows for these types. Check each cast's context
against PostgreSQL 16 before merge. Expected:

- `cidr -> inet`: implicit, no-op.
- `inet -> cidr`: assignment, zeroes host bits.
- `inet`/`cidr -> text`, `varchar`, `char`: assignment, through output.
- `text -> inet`/`cidr`/`macaddr`/`macaddr8`: explicit, through input.
- `macaddr <-> macaddr8`: implicit.

### Operators and functions

`inet`/`cidr`:

- Comparison operators (`<`, `<=`, `=`, `<>`, `>=`, `>`).
- Containment operators: `<<`, `<<=`, `>>`, `>>=`, `&&`.
- Bitwise and arithmetic operators: `~`, `&`, `|`, `+ bigint`,
  `bigint +`, `- bigint`, `inet - inet`.
- Functions: `abbrev`, `broadcast`, `family`, `host`, `hostmask`,
  `inet_merge`, `inet_same_family`, `masklen`, `netmask`, `network`,
  `set_masklen`, `text`.

`macaddr`/`macaddr8`:

- Comparison operators.
- Bitwise operators: `~`, `&`, `|`.
- Functions: `trunc`, `macaddr8_set7bit`.

All are pure, deterministic `UnaryFunc`/`BinaryFunc`/`VariadicFunc`s in
`src/expr/src/scalar/func/impls/{inet,macaddr}.rs` and are registered in
`src/sql/src/func.rs` with PostgreSQL's function OIDs.

### Catalog

- `pg_catalog` builtin types for the four types and their arrays.
- Function and operator builtins with PostgreSQL OIDs, so
  `pg_proc`/`pg_operator` match.

### Sources and sinks

- PostgreSQL source: map OIDs 869, 650, 829, 774 and their arrays to the new
  types in `src/sql/src/pure/postgres.rs` and the replication decoder. The
  text format of logical replication goes through the new input functions.
  `TEXT COLUMNS` keeps working for users who want strings.
- Kafka sinks emit the type's output text as an Avro/JSON string: `inet`
  omits a full-length mask, `cidr` always prints it.
- Iceberg and `COPY TO` use Arrow `Utf8` with the `text(inet)` form, which
  always prints the mask. The Arrow builder sees only the datum and not the
  SQL type, and this form parses back as either type.

### Feature flag

`enable_network_types` gates the four type names during SQL name resolution.
Its production default is off. CI turns it on through
`get_variable_system_parameters` in `misc/python/materialize/mzcompose/__init__.py`.
With the flag off, no user object can have one of these types, so nothing is
written to persist or the catalog in the new encodings.

If a release with the flag on has to be rolled back to a release without
these types, objects that use them cannot be loaded. The flag must stay off
until the release is fully rolled out and rollback is no longer expected.

### Delivery

Two PRs, each complete on its own:

1. `inet` and `cidr`: datum, encodings, wire formats, casts, operators,
   functions, catalog, PostgreSQL source, sinks, docs, tests.
2. `macaddr` and `macaddr8`, the same way.

### Testing

- Port PostgreSQL's `inet.sql` and `macaddr.sql`/`macaddr8.sql` regression
  cases to sqllogictest, removing only the GiST and SP-GiST parts.
- Proptest round-trips for `Inet`: text, binary, `ProtoDatum` and persist
  columnar. Proptests check that persist byte order matches `Ord`.
- pgtest for binary wire format against PostgreSQL.
- PostgreSQL source testdrive with all four types, including arrays and
  `TEXT COLUMNS`.
- A platform check so the values survive upgrade and restart.
- Fuzz targets for the text parsers. They take untrusted input from sources
  and clients.

## Alternatives

- **Text with helper functions** (`inet_contains(text, text)`). This avoids
  new durable encodings, but ordering, equality and pushdown all stay wrong,
  and PostgreSQL clients still see `text`. It does not meet the success
  criteria.
- **Store `inet` as `Datum::Bytes` in persist.** This saves one datum variant
  but loses the type at the storage layer and needs a separate type tag
  anyway to tell `cidr` output from `inet` output. Not simpler in practice.
- **Use the `ipnet` or `cidr` crates.** These would cover parsing but not
  PostgreSQL's abbreviated `cidr` input or its output rules. The parser is
  small enough that a new dependency does not pay for itself.

## Open questions

- Does PostgreSQL's `pg_cast` mark `macaddr8 -> macaddr` implicit or
  assignment? Check before merge.
- Should `<<` pushdown use the persist stats directly, or first be rewritten
  to a range predicate during MIR optimization? The second gives other
  optimizations the same benefit and is probably the better seam.
