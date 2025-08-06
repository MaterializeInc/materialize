# Persist schema migrations

## The Problem

The [Self Describing Persist Batches design doc from 2024](./20240328_persist_columnar.md) specified how Persist’s new columnar encoding should work. It covers most of the details of our columnar encoding, but says that “detailed logic for migrating the schema of a batch” is left to future work because it is “a large enough issue to warrant its own design doc”.

This design doc fleshes out the sketch from that original design, and gives a more precise semantics for how migrations can be handled in a safe and compatible way.

## Success Criteria

- Migrations need to be backwards compatible: old data should remain readable when we change the schema.
- Migrations should be metadata-only. Changing the schema of a shard should not require synchronously rewriting it; we should be able to lazily adapt the schema of old data at read or compaction time.
- Migrations need to behave well with compaction.
    - Compacting batches from before and after a migration should not fail.
    - Compaction should preserve the ordering of the input data when it can.
    - Compaction should remain “invisible” to readers: reads should get semantically-equivalent results both before and after compaction runs.

## Out of Scope

- We do not support arbitrary migrations. (Truly complex migrations, like rewriting columns using custom logic, will need to be supported at a higher level.)
- We specify only the migration logic that Persist uses for its internal data representation. (ie. at the `DataType` level, not the `RelationDesc` level.)
- Migrations do not need to be *forwards* compatible: changing the schema may cause readers with old, incompatible schemas to error out.
    - However, it’s important that these readers detect the issue and fail cleanly, instead of returning incorrect results.

## Solution Proposal

> This is partly a backwards-looking design, since the initial work was done last year. This section sketches out a consistent general design… and the MVP section below covers what parts of this have been implemented already and the simplifying assumptions we’ve made.
>

### Schemas and DataTypes

As part of the columnar migration, Persist introduced a new `Schema` trait which controls how a particular set of rows should be tranlated into a columnar format. (For example, the key type of an typical shard is `SourceData` and its schema type is `RelationDesc`; the arrow datatype will generally be a struct, with fields that vary depending on the exact set of columns and values in that schema.) Persist uses schema implementations like this to map inputs and outputs to and from its columnar representation at the edges, but otherwise works internally in terms of the underlying Arrow datatypes. This allows Persist to strongly enforce its internal requirements around data ordering and encoding, while allowing relations and their encoding to vary over time.

A Persist shard maintains an ordered sequence of schemas. This sequence is appended to with the `PersistClient::compare_and_evolve_schema` method. If the sequence is empty, this sets the initial schema of the shard. If the sequence is non-empty, this will check that the schema obeys the schema-migration rules; if it does, it will add the given schema as the latest schema of the shard. A monotonically-increasing `SchemaId` identifies a particular schema in this sequence.

### DataType migration rules

Permissible migrations are limited to:

- For a struct array, we allow:
    - Making a non-nullable field nullable. (We migrate by marking the field nullable, and leaving the data unchanged.)
    - Deleting a field. (We migrate by dropping the corresponding field from the array.)
    - Adding a nullable field that has not previously been deleted. (We migrate by filling in nulls for old data.)
- For all “nested” types, like a struct or list array, performing a valid migration on any of those nested types.

All other migrations are against the rules. This includes reordering fields, or re-creating previously-deleted fields. We may decide to add more forward-compatible migrations, like widening integer types, in the future.

Schema implementations can support more flexible migrations by being thoughtful about how they map data to datatypes. For example, we support re-using the names of deleted columns in `RelationDesc` by using the column id as our struct field name and ensuring column ids are not reused.

### Tracking deleted fields

To check whether it’s allowed to migrate a shard to a particular datatype, it’s not enough just to check the current datatype: we need to ensure that any added fields have never been used before in *any* of the previous schemas. IDLs like Protobuf uses “reserved fields” to track this information as part of the current schema, but Arrow datatypes have no similar concept built in.

Internally, we define a new “migration type” that *does* track deleted fields: identical to an arrow datatype, but where struct fields include whether or not a field has been deleted. We can recover the current datatype by filtering out the deleted fields.

### Reader schemas

Backward compatibility mean that readers using any valid schema should be able to read data that was written using any previous schema. However, we do not enforce forward compatibility… so we may apply a schema change that invalidates existing readers. In that case, the reader should be “fenced”: exit with an error instead of passing along data with an unexpected shape.

However, it’s possible that any individual schema change *is* forward-compatible, and if it is, existing readers should *not* be fenced.

Snapshots work by reading a particular version of the shard state (perhaps blocking to wait for a particular frontier) then returning each part that make up the snapshot. The reader should check that the reader schema is a valid migration for the schemas in that specific version of state; if not, it should raise an error. Listens can be made to work in a similar way, though they will need to re-validate the schema whenever the schema is evolved.

The errors raised by this read-time schema validation should not be passed along in the data stream, since that may make the results of any particular read nondeterministic. Instead, they should use a mechanism like the `ErrorHandler` in the persist source or report a usage error.

Some consequences of this rule:

- If a reader is using the latest schema, migrating from the shard’s schemas to the reader’s schema is a noop, and the reader will not be fenced.
- If a reader is using a “projection” of the current schema, migrating will also always succeed, since it’s always backwards compatible to delete a field. If a reader has projected away a column, any migrations to that column will not fence out the reader.
- If a reader includes a particular non-nullable column in its schema, and that column is made nullable, it will be fenced - migrating from a nullable to a non-nullable column is not allowed.
- If a reader includes a particular column, and that column is deleted, the reader will be fenced: re-creating a deleted column is not allowed.

### Compaction and consolidation

Compaction requires consolidation: taking a bunch of runs of data — which may have different datatypes — and converting them to a new run with a single datatype. It does this by first converting all the inputs into a uniform datatype, then compacting the uniformly-typed inputs together.

Compaction always migrates the data in the compaction to the datatype defined by the latest schema in state at the time the compaction was requested. This may be a lossy process if columns are deleted, but that is acceptable: since the compaction uses an output schema only once it’s durably recorded in state state, any reader that observes the compaction output will also have observed that schema, and any reads that would have depended on the now-deleted column will have been fenced out.

Deleting fields anywhere but the end of the struct may not preserve the order of the input data. This is a serious issue for consolidation, since it means we may not be able to generate sorted and consolidated output in a single pass over the inputs.

- Compaction can be made robust to order changes by eg. detecting when it’s generating out-of-order data and tagging it as unordered in the metadata. (After the current changes to compaction scheduling, this means that we’ll schedule followup runs of compaction to put it back in order.)
- If we know the schema history, we can check whether data has ever been reordered and thus might not be consolidatable in a single pass. This is useful for operations like Persist fast-path peeks, which need consolidated data; for example, if a shard has a primary key, we might avoid generating fast-path peeks against that shard if columns in that key have been affected by a schema migration.

## Minimal Viable Prototype

At time of writing, we have only implemented adding nullable columns to the end of a struct. This lets us make simplifying assumptions in various places:

- We can check for compatibility of a new schema by only looking at the latest schema.
- Migrating data does not reorder it.
- Schema changes in compaction are never lossy and never change ordering.
- We can check reader or writer schemas only at handle creation time: if they’re valid then they can never be invalidated.
    - This does have an ergonomics issue: since we check schemas at write-handle creation time, if we’re changing a schema between versions, we need to apply that change even in read-only mode. Delaying the check to batch-write time would resolve this.

So far, the demand for more advanced schema migrations has been limited. When we want to allow more advanced schema migrations, we’ll want to relax some of the assumptions above.

We’ve also implemented a few ad-hoc conversions, to deal with issues we found during the initial rollout — for example, compatibility issues around nullability or maps vs. lists. We will need to keep these special cases around until all the old data has been rewritten, which may be some time.

## Alternatives

### Forwards compatibility

Most schema tooling for languages like Protobuf and Avro assumes you want both forward and backwards compatibility: not only must new readers be allowed to read old data, but old readers should be able to understand new data, as long as the schema is evolved in legal ways. The original draft of this document was written with the idea that this was a requirement for Persist as well.

However, during review of that draft, it became clear that:

- Forwards compatibility is less important for Materialize than it is for other systems. Since changes to the catalog are totally ordered, it’s possible for us to validate that a column is unused before deleting it, for example. (The exception to this is “zombie” clients, which linger after they ought to have been shut down — this is what the fencing mechanism is for.)
- Requiring forward compatibility limits some functionality that Persist clients want, like deleting non-nullable columns or making columns nullable.

So: this draft proposes not enforcing forward compatibility by Persist in general. Persist clients, may choose to enforce forward compatibility and allow or a more limited set of migrations, or may allow a wider range of migrations as long as they don’t cause fencing in important dataflows… but in either case we propose leaving this decision to the client.

### Other options

- Don’t support migrations at the Persist level. Any changes to the schema would need to create a new shard and migrate the data over. This is possible - and currently the only option for complex rewrites - but we decided that the common cases were common and straightforward enough for Persist to support.
- Support even fancier migrations at the Persist level. For example, you could imagine a new method on the schema that would allow arbitrarily rewriting the data with custom logic. We decided against this because it would mean that Persist could give very few guarantees about performance or data ordering.

## Open questions

- **How much of this is necessary?** Persist’s current approach to schema migrations was written without a design, and the current state is somewhat confusing and internally incoherent. The main motive for this design doc was to make the principles clear so that migrations were easier to reason about. A secondary motive was to provide a roadmap for supporting more types of migrations in the future… but we do not have a set timeline for those changes, so it’s not clear if or when any particular new migration type will be needed or implemented.
