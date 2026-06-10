# Add Columns to Tables

- Associated: [issue#8233](https://github.com/MaterializeInc/database-issues/issues/8233)
- Associated: [pr#29694](https://github.com/MaterializeInc/materialize/pull/29694)

## The Problem

We want to support adding columns to relations in Materialize, both tables and sources. Concretely
for tables this means supporting Postgres’ syntax of `ALTER TABLE ... ADD COLUMN ...`, and for
sources supporting something like `ALTER SOURCE ... REFRESH SCHEMA ...` that will read the schema
from the upstream source and update the relations in Materialize accordingly.

When a column is added to a relation, it should not affect objects that depend on said relation.
For example:

```sql
CREATE TABLE t1 (a int);
INSERT INTO t1 VALUES (1), (2), (3);

CREATE VIEW v1 AS SELECT * FROM t1;

ALTER TABLE t1 ADD COLUMN b text;

-- view 'v1' does not have column 'b' since it was added after 'v1' was created.
SELECT * FROM v1;
 a
---
 1
 2
 3
```

The specific problem we’re aiming to address in this design doc is how can we support evolving the
`RelationDesc` of an object, while upholding existing invariants around the
`GlobalId -> RelationDesc` mapping.

## Success Criteria

We have aligned on a design that allows us to evolve the `RelationDesc` (schema) of an object in
the Adapter, Compute, and Storage layers of Materialize. This design should either conform to
the existing [Formalism](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/formalism.md#materialize-formalism),
or specifically describe how and why we will update the Formalism to support necessary changes.

## Out of Scope

- Schema evolution in Persist. For all intents and purposes you can assume that Persist supports
  evolving the schema of a shard and tracking the schemas of existing Parts.
- Other types of supported schema migrations. For all intents and purposes the only kind of schema
  migration we are concerned with is adding a nullable column.
- The syntax or implementation for supporting a feature like `ALTER SOURCE ... REFRESH SCHEMA ...`.
  For all intents and purposes we are only concerned with adding columns to tables.
- Unifying existing types of object IDs, i.e. [issue#6336](https://github.com/MaterializeInc/database-issues/issues/6336)

## Context

### `GlobalId`

Within Materialize a `GlobalId` generally identifies a single object and is used as the primary key
in the Catalog as well as numerous internal data structures. `GlobalId`s are also exposed to users
via catalog tables, e.g. [`mz_tables`](https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_tables),
where it is expected that they provide a stable mapping from ID to object name.

Additionally the [Formalism](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/formalism.md#globalids) defines `GlobalId`s as:

> A `GlobalId` is a globally unique identifier used in Materialize. One of the things Materialize
can identify with a `GlobalId` is a TVC. Every `GlobalId` corresponds to at most one TVC. This
invariant holds over all wall-clock time: `GlobalId`s are never re-bound to different TVCs.

By changing the `RelationDesc` for an object you are arguably rebinding the `GlobalId` to a new
TVC. A number of places all across our code base rely on this mapping of `GlobalId → RelationDesc`
being stable, so we can’t modify the `RelationDesc` for a given `GlobalId`. But we also need to
provide a stable external mapping of object ID to object name, so we can’t modify the `GlobalId`
for a given object.

## Solution Proposal

### SQL Persistence

Within the Catalog we persist objects with their `create_sql` string. To track when a column was
added to a table, and what version of a table a dependent object relies on, we plan to introduce a
`VERSION` keyword. For example our internal `create_sql` persistence will look like:

```sql
CREATE TABLE t1 (a int, b text VERSION ADDED 1);

-- view 'v1' references 't1' when it had only column 'a'
CREATE VIEW v1 AS SELECT * FROM [u1 as "materialize"."public"."t1" VERSION 0];
```

This would allow us to track the versions of a table that exist, and what version dependent objects
were initially planned against.

### New ID Mapping

Introduce a new `CatalogItemId` that will be a stable 1:1 mapping of object name to object ID and
keep the structure of `GlobalId` exactly how it exists currently. When adding a column to a table
we will allocate a new `GlobalId` that will be a unique reference to a `(CatalogItemId, VERSION)`.
In other words, a `(CatalogItemId, VERSION)` will uniquely identify a single TVC.

This new type will have two variants which are a subset of the variants of a `GlobalId`:

```rust
enum CatalogItemId {
  // System namespace.
  System(u64),
  // User namespace.
  User(u64),
}
```

### Relationships

This allows us to introduce the following relationships between our various types:

- 1 `CatalogItemId` can reference many `GlobalId`s
- 1 `GlobalId` will reference 1 `(CatalogItemId, VERSION)`
- 1 `GlobalId` will reference at most 1 `(ShardId, SchemaId)` (Persist)
- 1 `CatalogItemId` will reference at most 1 `ShardId` (Persist)

Using our example from before we’ll have the following:

- Name `"materialize"."public"."t1"`
- `CatalogItemId`: `u1`
- `GlobalId`s:
    - `u1` → `(CatalogItemId(u1), RelationDesc('a' int))`
    - `u2` → `(CatalogItemId(u1), RelationDesc('a' int, 'b' text))`
- `ShardId`: `sXXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX`

While not necessary and possibly out of scope of this design, with this new setup I begin to
imagine a `GlobalId` as uniquely referencing a collection; in other words, `GlobalId` could be
renamed to `CollectionId`.

> Despite the text representation of both a `CatalogItemId` and `GlobalId` being `u1`, they refer
> to different things. This discrepancy already exists in our code base, e.g. `RoleId` and
> `ClusterId` both have this same text representation but refer to different things.

## Implementation

`GlobalId`s are used all over the codebase: at the time of writing there are >2,000 matches for
“GlobalId” in Rust files. I will need to begin prototyping before I can speak to specifics of
exactly where `CatalogItemId`s will replace `GlobalId`s, but at a high level:

### Adapter

All current references to `GlobalId` in the Catalog will get replaced with `CatalogItemId`. The
text representation for IDs that is persisted in `create_sql` will get parsed as `CatalogItemId`s.

In planning (or possibly name resolution) is where we will convert from the `CatalogItemId(u1)`
and `VERSION` syntax in `create_sql` to `GlobalId`s.

In the durable Catalog we will reuse the existing ID allocator that currently mints `GlobalId`s to
mint `CatalogItemId`s. We also will create a new ID allocator specifically for `GlobalId`s that will be
initialized to the same value as the original allocator.

We need to start `CatalogItemId`s at the current value of the `GlobalId` allocator so all existing
items can continue to be identified by the same text representation of their current ID, and thus
to prevent ID re-use. For example, if a user has a table named "orders" with `GlobalId::User(42)`,
we'll migrate that to `CatalogItemId::User(42)` so externally that table continues to have the ID
of `'u42'`. This migration is only possible if we start `CatalogItemId`s at the current value of
the `GlobalId` allocator, so all existing IDs are considered "allocated". Additionally, resuming
`GlobalId` allocation from the current value prevents accidental `GlobalId` re-use if they are
persisted outside the Catalog. Additionally we will extend the existing
[ItemValue](https://github.com/MaterializeInc/materialize/blob/b579caa68b6d287426dead8626c0adc885205740/src/catalog/protos/objects.proto#L119-L126)
protobuf type to include a map of `VERSION -> GlobalId`. Externally to map between `CatalogItemId`s
and `GlobalId`s we’ll introduce a new Catalog table, `mz_internal.mz_collection_ids`.

### Storage

To me this is the largest unknown. The Storage Controller operates with `GlobalId`s which currently
have a 1:1 mapping with Persist’s `ShardId`s. This design calls for many `GlobalId`s to be able to
reference a single `ShardId` which breaks the existing relationship.

The Storage Controller will need to continue to use `GlobalId`s for operations like rendering a
source, but it will also need to have some careful management of Persist Handles, e.g. if there are
two open `WriteHandle`s to the same Persist Shard, writes to one of them would implicitly advance
the frontier of the other. Or, dropping a `GlobalId` will need to prevent finalizing the underlying
Persist Shard, if there are other `GlobalId`s that still reference said shard.

### Compute

Our Compute layer will operate entirely on `GlobalId`s and require only minor refactors the Catalog
APIs our Compute layer uses. Additionally, we'll should eventually add some Notices around Index
selection for tables. If a user creates an Index on a Table, then later adds a column to said
Table, that Index will no longer get used when querying the table because it is built on a previous
version.

## Minimal Viable Prototype

So far I have prototyped two alternate approaches, and am currently working on implementing the
approach that introduces a new `CatalogItemId` type.

- [pr#29694](https://github.com/MaterializeInc/materialize/pull/29694), implements adding columns
  to tables by changing the `RelationDesc` associated with a Table and applying a projection on to
  expose only the relevant columns.
- [pr#30018](https://github.com/MaterializeInc/materialize/pull/30018), stacked on top of
  `pr#29694`, only look at last commit. Implements adding columns to tables by adding `GlobalId`
  "aliases" to Tables so when a Table is altered we create a new `GlobalId`, and thus multiple
  `GlobalId`s can be associated with a single table.


## Alternatives

### Always Apply a Projection on top of a Source

If changing the shape of data in a TVC is not considered as creating a new TVC, then arguably
changing the `RelationDesc` of an object in Materialize would not be rebinding the `GlobalId`
of the object. This shrinks the theoretical scope of the problem to just constraining what columns
are used when planning objects. For example, when restarting Materialize we need to make sure when
re-planning objects, they’re planned against the same `RelationDesc` that was used when they were
originally created.

We can achieve this by threading through the correct `RelationDesc` in planning, and always
applying a projection on top of the operator that reads data. This technique has been prototyped in
[pr#29694](https://github.com/MaterializeInc/materialize/pull/29694). (Note: the test failures in
this PR are related to explain plans, notably the new
[alter-table.slt](https://github.com/MaterializeInc/materialize/pull/29694/files#diff-dff9699da8a6f3f1934d56574b8b3c8e47088e8149cd10847a459e9343d18f56) passes).

Practically an issue with this solution is that in a number of places within the codebase rely on
the `GlobalId -> RelationDesc` mapping to be stable. For example, an issue not solved in that PR is
how to handle Indexes that are created on Tables. While existing test cases pass there are plenty
more things that could break because of violating this invariant.

### Update the representation of a `GlobalId`

Instead of introducing a new `CatalogItemId` we could extend `GlobalId` to include version
information. For example:

```rust
// Current
enum GlobalId {
  // ... snipped
  User(u64),
}

// Alternate Approach
enum GlobalId {
  // ... snipped
  User(u64, u64),
}
```

Where the second `u64` in `GlobalId::User` would contain this new version information.

Outside of tests, everything in our codebase handles `GlobalId`s opaquely, they don’t look at the
inner value. Just adding more to the `GlobalId::User` variant would be a relatively small change
compared to adding a new ID type, but it would require more logical changes in the Adapter and
Storage layers. The Adapter still needs to maintain a stable mapping from object ID to object name
and Storage probably still needs to de-duplicate between `GlobalId`s and Persist’s `ShardId`s, both
of which would require looking at the inner value of the otherwise opaque `GlobalId`.

### Re-use `GlobalId`, allow multiple `GlobalId`s to refer to a single Table.

A combination of the proposed approach and the above alternative, instead of creating a new
`CatalogItemId` type or modifying the existing `GlobalId` type, just allow multiple `GlobalId`s to
refer to a single object. This can be modeled as "aliases" to a single object.

This approach requires the fewest code changes, but introduces the most ambiguity into the code
base. There are existing code paths that expect a `GlobalId` to uniquely refer to an object, e.g.
in the Catalog when dropping an object or maintaining `CriticalSinceHandle`s in the Storage
Controller. If we allow multiple `GlobalId`s to refer to a single object then the onus of making
sure we pass the _right_ `GlobalId`, or don't pass multiple `GlobalId`s that refer to the same
object, is put on the programmer. Whereas introducing a new `CatalogItemId` type designs away these
invalid states.

## Open questions

1. N/a
