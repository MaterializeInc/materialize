# Source Versioning / Tables from Sources

This design encompases 2 existing proposals also known as
'Subsource Demuxing' and 'Subsources as Tables'.

Associated:

-   https://github.com/MaterializeInc/database-issues/issues/6051
-   https://github.com/MaterializeInc/database-issues/issues/7417
-   https://github.com/MaterializeInc/database-issues/issues/4576
-   https://github.com/MaterializeInc/database-issues/issues/4924

## The Problems

TLDR:

-   Subsources are confusing and not implemented uniformly across source types
-   It's impossible to do a blue-green workflow in sources for handling
    schema-changes in upstream systems, without creating another top-level source
    and reingesting all your data / losing your reclocking information
-   Since subsources aren't created using explicit statements its difficult to
    manage them using dbt

Users currently have to learn about _sources_, _subsources_, _tables_,
_materialized views_, _indexes_, and _sinks_ when onboarding to Materialize.

If they'd like to ingest data from both an upstream Postgres/MySQL database
and a Kafka topic, they also need to learn the fragmented user interface
for dealing with data ingested from upstream data sources. Our existing
model of a _source_ does not provide a consistent interface among source types.

Single-output sources like Kafka and webhook sources put their data under the
source's primary relation, whereas multi-output sources like PostgreSQL and
MySQL sources do not allow querying the source's primary relation and instead
multiplex into _subsources_, an entirely separate concept. In addition to these
core source types, we also support the concept of _push sources_ (e.g. the
Materialize Fivetran connector), which write data to tables.

Our existing _source_ models also make it difficult to deal with upstream schema changes,
such that users need to drop and recreate a _source_ or _subsource_ and any dependent objects
to begin ingesting data with a new schema. This can cause long-periods of downtime if the
new source needs to reingest the entire upstream history. This can also cause them to
lose their _reclocking_ information which makes it impossible to implement a correct
end-to-end _source_ -> _sink_ pipeline.

Since subsources are created automagically as the result of a `CREATE SOURCE ..` statement
it is difficult to manage them with tools like dbt, which require all objects to be
created and dropped using explicit statements per object.

This change aims to simplify the user experience and create a more unified
model for managing data ingested from an upstream external system, while providing
more flexibility in handling upstream schema changes and managing sources in tools like dbt.

## Success Criteria

-   The mental overhead & learning time for users to ingest data from an upstream system
    is decreased.
-   All source types use a consistent interface to retrieve progress information and
    ingested data. The primary _source_ object represents the same concept across
    all source types.
-   The same upstream table/topic can be ingested into multiple collections in Materialize,
    each with a potentially different projection of the upstream schema. This enables
    a user to do blue-green schema-change workflows without unavailability.
-   _sources_ can maintain their progress collection while the set of collections being
    ingested are modified, allowing an end-to-end _source_ -> _sink_ schema-change workflow
    that does not emit redundant data (reusable reclocking).
-   The object representing each upstream table/topic in materialize is created and dropped
    explicitly, such that they can be managed as models directly in dbt.

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

-   Accomplishing 'subsource demuxing' using the concept of _subsources_ in-use today.
    The goals of https://github.com/MaterializeInc/database-issues/issues/7417 will be
    accomplished during the process of replacing _subsources_ with _tables_ (see
    solution proposal below).

## Solution Proposal

The concept of a _source_ will be unified around a single relation representing
the _progress_ of that source's ingestion pipeline. A query of any _source_
object (of any type) will return the data that is currently in the
`<source>_progress` collection.

We have been investing in the user experience around _tables_, such that
replacing _subsources_ with _tables_ would reduce the overall
mental overhead for new users.

By allowing tables to be marked as _read-only_, we can easily migrate
existing subsources to be tables without needing to figure out how to merge
`INSERT/UPDATE/DELETE` statements with the data written by their corresponding source.

The default `CREATE SOURCE ..` statement will just create a top-level source object
that represents a single ingestion of data from an upstream system. To actually
ingest data from that upstream system into a persist collection, the user will
use a `CREATE TABLE .. FROM SOURCE` statement with an option that references the source
and the external reference.

We will allow more than one table to reference the same external upstream reference,
using a potentially different set of columns. This allows a user to handle an upstream
schema change by creating a new table for the same upstream table and then performing
a blue-green swap operation to switch their downstream dependencies to the new table,
and then drop the old one. While we could also enable multiple subsources to
reference the same external upstream reference, we will instead continue to restrict this
to provide an incentive for users to opt-in to the migration to tables and allow us
to deprecate subsources sooner.

The existing options to `CREATE SOURCE` for Postgres & MySQL sources that specify
automatic subsources to create (`FOR ALL TABLES`, `FOR SCHEMAS` and `FOR TABLES`) will be
removed. Instead, all upstream tables to ingest from this source will need to be
explicitly referenced in a `CREATE TABLE` statement. While this may seem controversial,
more often than not these options cause upstream data that does not need to be
brought into Materialize to be ingested, and by using explicit statements for each
table to be ingested it makes Materialize configuration much more amenable to
object<->state mappings in tools like dbt and Terraform.

We will instead introduce a SQL statement or catalog table to return all known upstream
tables for a given source, optionally filtered by schema(s). This will allow
the web console to expose a guided 'create source' workflow for users where
they can select from a list of upstream tables and the console can generate
the appropriate `CREATE TABLE .. FROM SOURCE` statements for those selected.

Each `CREATE TABLE .. FROM SOURCE` statement will refer to a `SourceExport` for
a given source, and each newly-added `SourceExport` for a Source will attempt to ingest
a full snapshot of its relevant upstream data upon creation. This allows the
new `SourceExport` to correctly backfill any newly added columns or defaults set
in the upstream system, rather than returning Null values for any 'old' rows.
The aim is to maintain correctness and consistency with the upstream when a
query selects from new columns on rows that have not been updated since the
new `SourceExport` was added.

### Implementation Plan

1. Separate the planning of _sources_ and _subsources_ such that sources are fully
   planned before any linked subsources.

2. Update the CREATE TABLE statement to allow creation of a read-only table
   with a reference to a source and an upstream-reference:
   (`CREATE TABLE <new_table> FROM SOURCE <source> (REFERENCE <upstream_reference>)`)

3. Copy subsource-specific details stored on `CREATE SOURCE` statement options to their
   relevant subsource statements.

4. Update the underlying `SourceDesc`/`IngestionDesc` and
   `SourceExport`/`IngestionExport` structs to include each export's specific
   details and options on its own struct, rather than using an implicit mapping into
   the top-level source's options.

5. Update the source rendering operators to handle the new structures and allow
   outputting the same upstream table to more than one souce export.

6. Implement planning for `CREATE TABLE .. FROM SOURCE` and include a purification
   step akin to the purification for `ALTER SOURCE .. ADD SUBSOURCE`. This will verify
   the upstream permissions, schema, etc for the newly added source-fed table.

7. Update the storage controller to use both _subsources_ and _read-only tables_
   as _source_exports_ for existing multi-output sources (postgres & mysql).

8. Update introspection tables to expose details of source-fed tables
   (`CREATE TABLE .. FROM SOURCE` tables)

9. Add a new SQL command that returns all possible known upstream tables and columns for a source.

10. Implement an opt-in migration using a feature-flag to convert subsources to tables
    for existing multi-output sources (Postgres, MySQL, Load Generators)

11. Restructure kafka source planning and rendering to use source_export structure and allow
    multiple source-exports for a given kafka topic

12. Implement an opt-in migration for kafka sources to be converted to table structure

13. Remove subsource purification logic from `purify_create_source`, subsource statement parsing,
    and related planning code.

### Core Implementation Details

#### SQL Parsing & Planning

Our existing `CREATE SUBSOURCE` statements look like this:

```sql
CREATE SUBSOURCE <name> (<cols>) OF SOURCE <source_name> WITH (EXTERNAL REFERENCE = <upstream table name>)
```

though these are mostly hidden from users, they are available if a user runs `SHOW CREATE SOURCE <existing_subsource>`

Our existing `CREATE TABLE` statement looks like:

```sql
CREATE TABLE <name> (<cols>) WITH (<options>)
```

We would update the `CREATE TABLE` statement to be able to optionally reference an upstream
source and reference using `FROM SOURCE <source> (REFERENCE <upstream reference>)` and
any additional options necessary for ingesting the upstrema data:

```sql
CREATE TABLE <name> FROM SOURCE <source_name> (REFERENCE = <upstream name>) WITH (TEXT COLUMNS = (..), ..)
```

A new `CreateTableFromSource` statement will be introduced that includes the source reference
`T::ItemName` and the external reference `UnresolvedItemName`.

We would then introduce a new `TableDataSource` enum and add a field to the `Table` objects
used for the in-memory catalog and SQL planning, that optionally includes details
of an upstream data source in the `DataSource` variant, which will contain the existing
`DataSourceDesc` structs used in sources:

```rust
pub enum TableDataSource {
   /// The table owns data created via INSERT/UPDATE/DELETE statements.
   TableWrites,

   /// The table receives its data from the identified data source.
   /// This table type does not support INSERT/UPDATE/DELETE statements.
   DataSource(DataSourceDesc)
}

pub struct Table {
   pub create_sql: Option<String>,
   pub desc: RelationDesc,
   ...
   pub data_source: TableDataSource
}
```

The `DataSourceDesc` struct's `IngestionExport` variant will be extended to
include `details` and `data_config` fields on each export, such that all the
individual details necessary to render that export are stored here and not on
the top-level `Ingestion`:

```rust
pub enum DataSourceDesc {
    /// Receives data from an external system.
    Ingestion(Ingestion),
    /// This source receives its data from the identified ingestion,
    /// specifically the output identified by `external_reference`.
    IngestionExport {
        ingestion_id: GlobalId,
        external_reference: UnresolvedItemName,
        details: SourceExportDetails,
        data_config: SourceExportDataConfig<C>,
    },

    ...
}

/// this is an example and these enum structs would likely be their own types
pub enum SourceExportDetails {
    Kafka {
        metadata_columns: ...,
    },
    Postgres {
        column_casts: ...,
        table: PostgresTableDesc,
    },
    MySql {
        table: MySqlTableDesc,
        text_columns: Vec<String>,
        ignore_columns: Vec<String>,
    },
    LoadGenerator {
        ...
    }
}

pub enum SourceExportDataConfig {
    pub encoding: Option<SourceDataEncoding<C>>,
    pub envelope: SourceEnvelope,
}
```

The planning for `CREATE TABLE` will be adjusted to include a purification step
akin to the purification in `purify_alter_source`.
This will verify the upstream permissions, schema, etc of the upstream table, and
create the `details` necessary for this new export to be rendered as part of the
source. It will not need to be merged with the top-level source during sequencing
since its own details are entirely self-contained, unlike existing `CREATE SUBSOURCE`
statements.

#### Storage Collection Coordination

The coordinator's `bootstrap_storage_collections` will be updated to handle
tables with the `TableDataSource::IngestionExport { .. }`, mapping them to
a storage controller `CollectionDescription` with a `DataSource::IngestionExport`
rather than the existing `DataSourceOther::TableWrites` value used for all tables.

The storage controller's `DataSource::IngestionExport` struct will be updated to
include the export details:

```rust
    IngestionExport {
        ingestion_id: GlobalId,
        details: SourceExportDetails,
        data_config: SourceExportDataConfig<C>,
    },
```

The `create_collections` method in the storage client currently maps a
`DataSource::IngestionExport` into a storage `SourceExport` struct and then
inserts this into the appropriate `IngestionDescription` of the top-level
source. This is used in source rendering to figure out what upstream
reference should be mapped to a specific output collection.

The storage `SourceExport` struct will be updated to include the `details`
field and `data_config` fields, populated from the `IngestionExport`:

```rust
pub struct SourceExport<S = (), C: ConnectionAccess = InlinedConnection> {
   /// The collection metadata needed to write the exported data
   pub storage_metadata: S,
   /// Details necessary for the source to export data to this export's collection.
   pub details: SourceExportDetails,
   /// Config necessary to handle (e.g. decode and envelope) the data for this export.
   pub data_config: SourceExportDataConfig<C>,
}
```

Then the subsource-specific values currently stored in `GenericSourceConnection`
such as the Postgres and MySQL `tables` vecs will be removed, since each
'source export' would contain its own information.

#### Source Rendering

Currently, when an `IngestionDescription` is used inside `build_ingestion_dataflow`
(the method that renders the various operators needed to run a source ingestion),
it determines the `output_index` of each collection to be output by the ingestion
by calling the `IngestionDescription::source_exports_with_output_indices` method.

This method maps the `external_reference` (the upstream table name) of each `SourceExport`
to the index of that upstream table in the source's `details` struct (which is currently
created during purification -- a vector of upstream table descriptions).

Then inside each source implementation, we currently assume that each
`SourceExport` uniqely corresponds to a single `output_index`, and that each `output_index`
corresponds to the same index of the upstream table in the source's upstream `details`.

Since the top-level source `details` will no longer contain the `tables` field, the
output index will be determined by the ordering of the `IngestionDescription::source_exports` `BTreeMap`.
Each `SourceExport` will output its own stream from the ingestion using
the `details` it contains. It will be up to each source implementation to map the
relevant upstream table to the correct `SourceExport`s using the `SourceExportDetails`
and output to the correct `output_index`.

### Kafka Sources

Kafka Sources will require more refactoring to move to a model where a single Kafka Source
can output to more than one `SourceExport`.

Since Kafka Sources only ever refer to a single Kafka Topic, a user will likely only ever
add a new 'Table' to a Kafka Source to handle a schema change in the messages published
to that topic.

When a new `SourceExport` is added to a Kafka Source, the new export needs to be hydrated
from the upstream topic to ensure it has a complete view of the topic contents. Each export
will potentially include a different set of metadata columns in its output relation so
the source will need to handle message construction on a per-export basis.

When a new `SourceExport` is added, its resume upper will be the minimum value. When this
resume upper is merged with the existing resume upper for the source it will cause the
consumer to begin reading from each partition at the lowest offset available (or the
'start offset' defined on the Kafka Source). This means that we will potentially do a
long rehydration to catch up to the previous resume-upper, such that existing
`SourceExports` will not see any new data until the rehydration completes.

This behavior is the same in the Postgres and MySQL sources, so will be left as-is
for now. In the future we need to refactor the source dataflows to track progress
on a per-export basis and do independent reclocking such that we can manage the
timely capabilities / progress of each source export independently.

Envelope and encoding/format options are currently specified on a per-source level.
These options will be moved to the `CREATE TABLE .. FROM SOURCE` statements such that
they can be independently set on a per-table basis. The `envelope` and `encoding` fields
will be moved from the primary source config to the `data_config` on each `SourceExport`,
and the source rendering pipeline will use per-export encoding/envelope settings rather
than assuming each export for a source requires the same config, as it currently does.

### Webhook Sources

Webhook sources do not currently behave like other source types, they do not actually
exist in the storage layer and are not rendered as any sort of dataflow. Instead
`environmentd` handles incoming HTTP requests bound for webhooks, validates the request
against any webhook validation options (such as checking an HMAC), and then decodes
the request body and writes the results directly to a persist collection.

Rather than continuing to try and have webhooks look like a 'source' at the SQL
layer while implementing completely different behavior for them under the hood,
instead we will introduce a new SQL statement called `CREATE TABLE .. FROM WEBHOOK`
that aligns well with our new source-statement model but more accurately describes
webhooks and allows future divergence without unnecessary coupling to the
source model.

For the purposes of this project, existing `CREATE SOURCE .. FROM WEBHOOK`
statements will be migrated to `CREATE TABLE .. FROM WEBHOOK` statements, and
the under-the-hood logic will remain the same for webhook sources.

In the future, we can refactor webhook objects in the codebase to resemble
tables more closely than source objects.

### Migration of source statements and collections

We would generate a catalog migration to generate new `CREATE TABLE` statements where
necessary and shuffle the collections being pointed at by each statement. The intent
is to preserve the names associated with each collection even if the statement that
identifies each collection is updated. The migration would do the following:

-   For existing sources where the primary relation has the data (e.g. Kafka sources):
    -   Assuming the existing source object is named `<source>` with id `u123`
    -   Generate a new `CREATE TABLE` statement that is tied to the current primary collection
        and name it `<source>`, but use a new id for this object
    -   Change the `CREATE SOURCE` statement to point to the current progress collection and
        change the source name to `<source>_progress` but keep the id `u123`
    -   Drop the existing `CREATE SUBSOURCE <source>_progress` statement since this will no
        longer have a collection to own
-   For existing multi-output sources (e.g. Postgres & MySQL sources):
    -   Convert subsources to `CREATE TABLE` statements, each using the same name and id
    -   Change the `CREATE SOURCE` statement to point to the current progress collection and
        change the source name to `<source>_progress` but keep the same id
    -   Drop the existing `CREATE SUBSOURCE <source>_progress` statement since this will no
        longer have a collection to own
    -   Drop the existing top-level collection tied to the source, since this is already
        unused and will no longer be owned by any statement

One important caveat is that the error shard of the primary collection currently contains
errors for the source as a whole. Since the progress collection doesn't contain any errors
we will need to combine the error shard of the main collection with the data shard of
the progress collection when we implement the migration of the top-level source object
to point to the progress collection.

The reason behind preserving the id of each statement even while we change the names is
to allow tools such as `terraform` to maintain their state about each object, since our
terraform provider uses each statement id to uniquely identify resources in terraform state.

### Exposing available upstream references to users

Since creating a source will no longer automatically create any subsources, users that
do not already know all the upstream references available in their system may find it difficult
to know which `CREATE TABLE .. FROM SOURCE` statements they could use to ingest their
upstream data. Additionally, the Web Console will need a way to present a dynamic UI
to users after they have created a source to allow them to create any downstream tables
from a selection of the available upstream references of that source.

We have two options available to us for exposing this information to users:

1.  Add a new statement to fetch from the upstream all possible 'upstream references' available
    for ingestion.
2.  Create a new system table that periodically updates with the set of available upstream
    references in each sources, and a new statement to trigger a refresh of this system table.

The 2nd option is more complex to implement, but presents the information in a form that
is more similar to other similar information in Materialize. However since the data can
become out of date, it's important that the refresh statement is highlighted prominently
in user-facing documentation and related guides.

The proposal for the 2nd option (a new system table):

Introduce a `mz_internal.mz_source_references` with the following columns:

-   `source_id`: The source (e.g. a Postgres, MySQL, Kafka source) this reference is available for.
-   `columns`: If available, a `text[]` array of columns that this external reference could have
    if it were ingested as a table in Materialize. This will be NULL for source types where an
    external registry is needed to determine the upstream schema (e.g. Kafka).
-   `name`: The object name of the reference. E.g. for a Postgres source this is the table name,
    and for kafka is the topic name.
-   `namespace`: Any namespace that this reference object exists in, if applicable. For Postgres
    and MySQL this would be the schema name, and for Kafka this would be NULL.
-   `updated_at`: The timestamp of when this reference was fetched from the upstream system.

Note that we do not expose the types of any of the upstream columns, since each source has
different constructs for how types are represented, and since some source-types can't
represent all upstream types (e.g. MySQL) without knowing which of them will be represented
as TEXT using a TEXT COLUMNS option. Therefore the list of `columns` does not imply that each
of those columns is one that Materialize can actually ingest.

Since we only expose one `namespace` column we are also limited to only one layer of namespacing
in this model. For all existing source types this is okay, since Kafka has no namespacing,
MySQL only has one level, and in Postgres all tables in a publication must belong to the same
'database' such that the 2nd level of namespacing doesn't matter. In the future if we encounter
a situation where we need to expose more levels we can add columns as needed.

A new `ALTER SOURCE <source_id or name> REFRESH REFERENCES` statement will be introduced that
can trigger a refresh of the rows in `mz_internal.mz_source_references` that pertain to the
requested source. Otherwise, this catalog table will only be populated at source creation.

### System Catalog & Introspection Updates

We currently surface details about both Sources and Tables through various system catalog
tables in both the `mz_catalog` and `mz_internal` schemas. These expose information
about the objects themselves as well as statistics and statuses emitted from the active dataflows for each source.

We would make the following updates to provide users and Materialize employees the same
ability to introspect source-fed tables that they currently have for subsources.

#### [mz_catalog.mz_tables](https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_tables)

`CREATE TABLE .. FROM SOURCE` statements would be added to this catalog table.

A `source_id` column will be added to this table: a nullable column that contains
the `id` of the primary source if there is one. For normal tables this will be `NULL`.

#### [mz_catalog.mz_sources](https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_sources)

`CREATE TABLE .. FROM SOURCE` statements will NOT be added to this catalog table, since
they are 'tables'.

#### [mz_internal.mz_postgres_source_tables](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_postgres_source_tables) and [mz_internal.mz_mysql_source_tables](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_mysql_source_tables)

Each row in these tables contains a mapping from a `subsource` `id` to the external
`schema` and `table` of the Postgres/MySQL table being ingested into that subsource.

These will be updated to include the same mappings for `CREATE TABLE .. FROM SOURCE`
table objects to the external `schema` and `table` they are linked to.

Additionally, an `mz_internal.mz_kafka_source_tables` will be added to map a Kafka
topic to each `CREATE TABLE .. FROM SOURCE` object that links to a Kafka source.

#### [mz_internal.mz_source_statistics](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_source_statistics)

This table does not require any changes to its schema. `CREATE TABLE .. FROM SOURCE` objects
will include the same set of information in this table that subsources do.

#### [mz_internal.mz_source_statuses](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_source_statuses)

The `"table"` variant will be added to the `type` column and all `CREATE TABLE .. FROM SOURCE`
statements will report a status in this table with the same semantics as existing subsources.

It may make sense to remove the `type` column to attempt to normalize this information
across the catalog, but this will require refactoring the Console too so will be done
outside the scope of this project.

#### [mz_internal.mz_source_status_history](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_source_status_history)

No change is necessary to this table, besides noting that the `source_id` column will
potentially represent an id of a `CREATE TABLE .. FROM SOURCE` statement.

## Minimal Viable Prototype

## Alternatives

These are not 'full' alternatives but are each solutions to one or more of the
problems outlined above:

1. Introduce a new `FOR TABLES ()` on existing multi-output source types to enable
   workflows with dbt. We could allow creating a postgres or mysql source with
   zero tables, and then have dbt use the existing `ALTER SOURCE .. ADD SUBSOURCE`
   and `DROP SOURCE` statements to manage creating/removing subsources. While
   this would be a simpler implementation, it doesn't work for single-output
   sources (e.g. Kafka).

2. Implement in-place schema changes on subsources. Support for in-place schema
   changes is predicated on evolving a collection's underlying persist
   schema, which isn't currently possible but is being worked on in the effort
   to use parquet column-level encoding for persist. Additionally, since subsources
   are not used for single-output sources, this would add to the inconsistent
   interface among source types (in-place schema modification statements would
   have to be implemented for both top-level sources and subsources).

3. Implement demuxing of upstream tables using existing subsource model
   (https://github.com/MaterializeInc/database-issues/issues/7417). This would be an
   easier effort to implement, but rather than doing this work in the short term
   for it to be later ripped out, it is preferable to implement the interface
   that solves both this need and the other problems outlined above.

### Webhook Source Alternatives

1.  Webhook sources will be migrated to the new model at the SQL level, using the
    same `CREATE TABLE .. FROM SOURCE` statement syntax as other sources.
    Since the `encoding` and `envelope` options will be moved to that table statement
    this will also allow a webhook request to be decoded differently for each table.

    Under the hood, we will continue to have `environmentd` receive Webhook requests
    and write directly to a persist collection. However this code will be modified
    to just do request validation and then write the _raw_ request body as bytes
    and a map of all headers to the persist collection without doing any decoding.

    We will then create a new `Persist Source` that operates exactly like all other source
    types such that the storage layer renders a dataflow and the source operator reads
    from an existing persist collection -- in this case the 'raw' webhook collection being
    written to by `environmentd`. This `Persist Source` can have multiple `SourceExports`
    like other sources, and each export can define its own `encoding` and `envelope` processing
    to convert the raw bytes of each request into the appropriate relation schema.
    The progress collection for the webhook source will essentially be the frontier of
    the underlying 'raw' persist collection that each request is written to.

    This approach was discussed offline and deemed unnecessary scope since we do not
    yet have a reason to need multiple 'exports' from a webhook source with different
    encoding/envelope configuration, and if we do we would likely implement something
    more specific to webhooks such as a way to de-multiplex a webhook request
    into multiple tables based on some inner substructure.

## Remaining open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->

## Project Update - November 2024

Almost all of the changes described in the design doc above have been implemented
and are available for use internally with the appropriate feature-flag(s).

The notable changes not yet implemented are:

-   Converting webhook statements to `CREATE TABLE .. FROM WEBHOOK`. This is an open
    task to complete.
-   Removing the separate `<source>_progress` subsource and making the primary source
    collection hold the progress data. This was punted to be done at a future date since
    it is difficult to do while the legacy source syntax is still supported.
-   Console changes to support the new syntax: https://github.com/MaterializeInc/console/issues/3400
    Actively being worked on.

Open bugs are being tracked in the associated epic:
https://github.com/MaterializeInc/database-issues/issues/8322

There are two feature-flags that control usage of the new syntax:

-   `enable_create_table_from_source`: This flag unlocks the ability to use the new
    `CREATE TABLE .. FROM SOURCE` statement and allows defining sources with
    `CREATE SOURCE ..` statements that don't need to specify legacy 'required' fields.
    Enabling this feature-flag for customers allows them to test the new syntax and to
    use both the old and new syntaxes.
-   `force_source_table_syntax`: This flag forces use of the new syntax and activates
    a catalog migration to convert any legacy sources over to the new syntax model. It
    also changes single-output sources to no longer output to their primary data collection.

There are multiple ways to migrate a customer to the new syntax model:

-   self migration: Once the customer has the `enable_create_table_from_source` flag
    activated they can freely use the new syntax as desired. They can then opt to do one of
    two things to migrate to the new model:

    -   Keep their existing sources and add new `CREATE TABLE .. FROM SOURCE` statements
        that reference these sources. Move all downstream dependencies to read from these
        tables instead of existing subsources and primary sources. Then drop any legacy
        subsources using `DROP SOURCE <subsource>`.
    -   Create completely new sources and new tables explicitly on those sources. Move
        over all downstream dependencies. Then drop all old sources using
        `DROP SOURCE <source> CASCADE`.

    Note that there is an important caveat: Kafka and single-output load generator sources
    will continue to export data to the the primary source collection until
    `force_source_table_syntax` is activated, in addition to any exports to tables.
    The additional export can have adverse performance implications when using the `upsert`
    envelope, such as requiring more disk when there are two upsert exports instead of one.

-   auto migration: The customer can coordinate with Materialize to activate the
    `force_source_table_syntax` flag and trigger a restart of their environment, which will
    activate the catalog migration that automatically converts all existing sources and
    depenencies to the new syntax. All name -> data relationships are unchanged such that
    all queries should continue to work without modification, but state in dbt and terraform
    may need to be updated to reference new statements.

    Since the migration activated by the `force_source_table_syntax` flag will rewrite existing
    source and subsource statements, activating this flag for a customer requires more care
    than usual. This flag can cause weird behavior if activated in an environment containing
    legacy sources without also triggering an environment restart or upgrade since it will
    cause existing legacy sources to fail SQL planning until they are auto-migrated by the
    catalog migration on the next environmentd boot.

    When the migration is applied, the customer may also need to update their
    existing dbt and/or terraform code to accommodate the changed statements.
    In an ideal situation, each customer would work with their field-engineer to choose
    the appropriate week to activate this migration during the usual weekly upgrade maintenance
    window, and the release managers during the weekly maintenance window will activate the flag
    for any customers designated for that week.

The catalog migration activated by `force_source_table_syntax` does the following:

-   For existing sources where the primary relation has the data (e.g. Kafka sources
    and single-output load generator sources):
    -   Assuming the existing source object is named `<source>` with id `u123`
    -   Generate a new `CREATE TABLE` statement named `<source>` with a new id.
        Make this new table statement use the current source's primary data collection.
        Copy over all the options from the source statement that apply to the
        table instead (envelope, format, etc).
    -   Change the existing `CREATE SOURCE` statement to be named `<source>_source`
        or `<source>_source_N` if `_source` is taken. Keep the same id, but use
        a new empty primary data collection. Remove all the options on this source
        statement that are no longer necessary.
    -   Re-write any id-based references to the source statement in the catalog to
        point to the new table id instead, so downstream statements continue reading
        from the same data.
-   For existing multi-output sources (e.g. Postgres, MySQL, and some load-gen sources):
    -   Convert subsources to `CREATE TABLE` statements, each using the same name, id,
        and options. In a previous migration we had already copied all the
        export-specific options onto the subsource statements (e.g. `WITH (TEXT COLUMNS ..)`)
    -   Change the `CREATE SOURCE` statement to remove any subsource-specification fields
        (e.g. `FOR ALL TABLES`) and options `e.g. WITH (TEXT COLUMNS ..)`

The suggested overall migration plan for moving customers to the new model:

1. Enable private-preview for the new syntax with a select group of customers by
   activating the `enable_create_table_from_source` flag for them. Encourage this group
   to add new tables to their existing sources or to create entirely new sources using
   the new syntax and report feedback on the experience.

2. Also activate the `force_source_table_syntax` flag for any private-preview customers that
   would like to test the auto-migration of their sources.

3. Begin the 'public' migration:

    - Set the `enable_create_table_from_source` syntax to `true` for all environments.

    - Explicitly **set the `force_source_table_syntax` flag to `false` for each existing customer**
      environment in Launch Darkly.

    - Set the `force_source_table_syntax` flag default to `true`, such that all **new** customer
      environments must use the new syntax.

    - Roll out documentation changes that use the new syntax as the default and move the existing
      syntax docs to a 'legacy' section.

    - Send out an announcement to all existing customers of the migration plan. Outline a specific
      window (e.g. 4 weeks) during which they can either self-migrate their sources to the new
      model or request an auto-migration.

    - Merge the terraform PR https://github.com/MaterializeInc/terraform-provider-materialize/pull/647
      that starts warning users the old syntax is deprecated and allows using the new syntax
      going forward.

4. On a per-customer basis, offer to do the automatic catalog migration during a chosen
   weekly release window. Coordinate with the release managers to ensure the correct customer
   environments are activated with the `force_source_table_syntax` flag before the upgrade.

5. After the multi-week migration 'window', notify all existing customers that their environments
   will be migrated during the next release window if they haven't yet done so themselves.
   Coordinate with the release manager to turn on `force_source_table_syntax` for all
   environments right before the next release rollout.

6. Delete all code related to the legacy syntax: all subsource code (besides being able
   to create and parse `_progress` subsources), all code related to parsing and planning
   options that apply to the primary data collection of a source, and code in the storage
   layer to handle outputting to the primary collection of a source.
