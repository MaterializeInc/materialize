# Builtin Schema Migration

- Associated: https://github.com/MaterializeInc/database-issues/issues/9793

## The Problem

Materialize has a number of builtin storage collections (tables and sources), defined in the code.
Over time, the schema of these collections can change, as we add new features or remove cruft.
When a schema change happens, existing collections created by previous Materialize versions need to be migrated to the new schema.

The existing mechanism for this, called "builtin item migration", is flawed.
When invoked in read-only mode, it can make changes to durable state that poison said state for any processes at previous versions.
As a result, after an in-progress upgrade to some version Y was aborted, it is not always guaranteed that a subsequent upgrade to a version X < Y is still possible.
This is a surprising and annoying limitation, especially for our self-managed offering where we don't control which upgrade paths users choose.

Another issue with the current builtin item migration mechanism is that it has no controls or safeguards.
It automatically either evolves the schema of persist shards, or replaces them, depending on whether or not a schema change is backward compatible.
This introduces the risk of accidentally losing data by making an incompatible schema change that causes the replacement of a builtin collection shard.
We would like these migrations to be performed in a more intentional fashion, so that they don't happen by accident.

## Success Criteria

These criteria are required to make builtin schema migration robust in self-managed deployments:

- Builtin schema migration, when performed in read-only mode, does not modify durable state in a way that prevents processes at other versions from starting up.
  "Other versions" here explicitly includes older versions.
- Builtin schema migration, when performed in leader mode, does not interfere with schema migration performed by read-only processes with higher versions.
  It may fence out processes running at lower versions.
- Each Materialize version explicitly defines in its code the builtin schema migrations to be performed to upgrade existing durable state to this version.
  If a builtin storage collection received a schema change without a corresponding migration instruction, upgrading to this version fails with an error.

Additionally, in anticipation of the use-case-isolation work:

- Builtin schema migration supports multiple concurrent processes executing the migration mechanism at the same time.

## Out of Scope

- Data migration of builtin collections, i.e. arbitrary rewriting of existing contents.

## Solution Proposal

The existing migration mechanism supports two approaches to schema migration:
  - For backward-compatible schema changes (i.e. data written with the old schema remains readable with the new schema) it uses persist schema evolution.
  - For incompatible schema changes it creates a new persist shard, effectively truncating the builtin storage collection.

Both approaches have their merits, the first allowing schema changes without data loss, the second allowing breaking schema changes (at the cost of data loss).
They are, however, problematic in how they are implemented:
  - Schema evolution durably registers the new schema with the persist shard, globally changing the shard's schema and thus potentially prevent older versions from performing migration by schema evolution.
  - Shard replacement uses a shared migration shard whose semantics are such that lower versions get fenced out by the existence of entries written by higher versions.

The proposed solution keeps both migration approaches but avoids the problematic implementation details.

### Schema Evolution

A read-only process performing builtin schema migration by schema evolution skips registering the new schema with the persist shard.
Instead, it only performs a sanity check ensuring that the new schema is indeed backward compatible with the current shard schema.
Doing so requires no writes to durable state, and therefore doesn't interfere with other processes in any way.

In the subsequent read-only bootstrap phase, the process creates persist read and write handles using the new schema.
Read handles perform transparent migration of any data updates that flow through them, so dataflow hydration can proceed using the new schema.
Write handles only require a matching registered shard schema when writing batches, which is something a read-only process doesn't do.

Once the read-only process gets promoted to a leader, and runs the builtin schema migration mechanism again, it this time registers the new schema with the persist shard.
Doing so fences out any processes that planned to evolve the schema to an earlier version.
It does not interfere with processes planning to evolve the schema to a later version, assuming that the later version's schema is backward compatible with all previous schemas.

### Shard Replacement

The general approach to shard replacement matches the existing implementation, but takes care to not needlessly interfere with processes at other versions.

To support schema migration by shard replacement, we need a place to store new shard IDs across restarts, and we will keep using the migration shard for this.
The migration shard contains entries of the form `(GlobalId, Version, DeployGeneration) -> ShardId`.

A read-only process performing shard replacement reads the migration shard to collect all existing entries at its version and deploy generation.
Notably, it ignores any entries at different versions or deploy generations, to avoid any interference with concurrent in-progress upgrades.
Depending on the existing migration shard entries, the process either decides to use the existing replacement shard, or to create the replacement shard and write its ID into the migration shard, at the current version.
It sets the new shard ID as the migrated collection's shard in its in-memory catalog and commences bootstrapping using the replacement shard.

A leader process performing shard replacement performs the same steps as in read-only mode.
Additionally, it cleans up durable state written by earlier versions and/or deploy generations by:
  - arranging for the previous shards used by the migrated storage collections to be finalized
  - removing entries from earlier versions from the migration shard, and arranging for those shards to be finalized as well

Note that the deploy generation is included in the migration shard key to support the scenario of two different upgrades to the same version running at the same time.
Without the deploy generation in the key, these upgrades would interfere in unintended ways.
While the scenario is not one we expect to occur during normal operation, we cannot rule out that it will ever occur in a self-managed context.

### Supporting Concurrent Processes

To prepare for use-case-isolation, which requires running multiple adapter processes concurrently, we need to make sure that builtin schema migration doesn't fail when processes of the same version race to perform it.
We achieve this by reacting to the various compare-and-append failures by retrying the migration process.

During schema evolution, if the `compare_and_evolve_schema` call fails, the process compares the expected schema with its new schema.
  - If the new schema is the same as the expected schema, the migration was already performed and nothing is left to do.
  - If the new schema is backward compatible with the expected schema, it tries to `compare_and_evolve_schema` again.
  - Otherwise it has been fenced out by a newer version and halts.

During shard replacement, if the `compare_and_append` call on the migration shard fails, the process restarts the shard replacement mechanism, re-reading the migration shard and acting according to its contents.
Note that here we do nothing to fence out older versions when a new leader has taken over.
We instead expect either one of the catalog fencing mechanisms or the upgrade orchestration to eventually take care of that.

### Migration Instructions

To avoid data loss and other surprises caused by automatic builtin schema migrations, we introduce the concept of explicit migration instructions.
A migration instruction instructs the process which builtin collection to migrate at which version, and which mechanism to use.

Migration instructions are kept in a hard-coded list:

```rust
const MIGRATIONS: &[(Version, SystemObjectDescription, Mechanism)] = &[
    (
        V123,
        SystemObjectDescription {
            schema_name: "mz_catalog".into(),
            object_name: "mz_tables".into(),
            object_type: CatalogItemType::Table,
        },
        Mechanism::Evolution,
    ),
    ...
];

enum Mechanism {
    Evolution,
    Replacement,
}
```

A migration from version X to version Y involves the following steps:

  * Collect from the `MIGRATIONS` list all entries with versions > X and <= Y.
  * Group the collected entries by the object description.
  * For each object to migrate, select a mechanism based on the collected `Mechanism` values:
    * If any value is `Mechanism::Replacement`, select shard replacement.
    * Otherwise select schema evolution.
  * For each object to migrate, perform the migration using the selected mechanism.

Note that merging `Evolution` migrations like this is sound because persist requires that each shard schema is backward compatible with any previous schema registered with the shard (i.e. schema compatibility is transitive).
Which means schema evolution will succeed even if we skip intermediary schemas.

We safeguard against schema changes that don't have an associated `MIGRATIONS` entry by using the existing builtin fingerprint mechanism.
Each builtin collection has a fingerprint, derived from its schema definition, stored in the catalog.
By comparing the stored fingerprint with the fingerprint of the object definition in the code, we can detect schema changes that require migration.
If we detect a schema change but no corresponding `MIGRATIONS` entry, we immediately abort the process.

For the migration instructions scheme to be sound, we require that later versions do not remove migrations added in earlier versions (unless they are before the last unskippable version).
The scenario where this is most likely to come up is when a migration gets backported into a patch release that isn't present in the already existing next minor release.
For example, consider the case there both v0.10.0 and v0.11.0 have been released, and a new v0.10.1 is cut with a new builtin scheme migration.
In that scenario, upgrading to v0.11.0 through v0.10.1 will fail because v0.11.0 expects a schema different from the one produced by the new migration in v0.10.1.
To ensure this doesn't happen, we must disallow backporting of builtin schema migrations into prior releases.
We can make sure we catch instances of such disallowed backports by running exhaustive upgrade tests:
Whenever a new patch release is introduced, we test that upgrades from that release to all existing later releases succeed.
In the example, we would test the upgrade from v0.10.1 to v0.11.0 (and any later versions), which would alert us about the issue.

## Alternatives

As an alternative to schema evolution, we can consider a migration scheme that creates a new shard and copies over all existing data from the old shard, performing the migration in the process.
Doing so would enable us to perform arbitrary rewrites of the data, as well as breaking schema changes without loss of historical data.
This approach would be very powerful, but isn't straightforward to implement.
In particular, it is not sufficient to perform the copy-and-migrate step once in read-only mode, since the leader environment might write more data to the old shard before the cutover.
After the cutover the new leader has to make sure to copy over the missed updates, but it is not clear how to do that without negatively affecting the environment startup time.
Persist schema evolution is easier to use and sufficiently powerful for now.

As an alternative to shard replacement, we could require that all schema changes to builtin collections need to be backward compatible and therefore eligible for schema evolution.
This seems like an unpractical constraint though, given that during development of new database features we often end up making breaking changes to `mz_internal` relations in response to feedback received.
