# Source Versioning / Tables from Sources

This design encompases 2 existing proposals also known as
'Subsource Demuxing' and 'Subsources as Tables'.

Associated:

-   https://github.com/MaterializeInc/materialize/issues/20208
-   https://github.com/MaterializeInc/materialize/issues/24843
-   https://github.com/MaterializeInc/materialize/issues/15897
-   https://github.com/MaterializeInc/materialize/issues/17021

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
    The goals of https://github.com/MaterializeInc/materialize/issues/24843 will be
    accomplished during the process of replacing _subsources_ with _tables_ (see
    solution proposal below).

-   Dealing with Webhook sources. These are fundamentally different than the other
    storage-owned source types whose operators implement the `SourceRender` trait,
    and they do not contain a 'progress' collection. While they may also be
    converted to use 'tables' in the future, that work is not in-scope for this proposal.

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
   **Update: completed in [PR](https://github.com/MaterializeInc/materialize/pull/28310)**

2. Update the CREATE TABLE statement to allow creation of a read-only table
   with a reference to a source and an upstream-reference:
   (`CREATE TABLE <new_table> FROM SOURCE <source> (REFERENCE <upstream_reference>)`)
   **Update: statement introduced in [PR](https://github.com/MaterializeInc/materialize/pull/28125)**

3. Copy subsource-specific details stored on `CREATE SOURCE` statement options to their
   relevant subsource statements.
   **Update: completed in [PR](https://github.com/MaterializeInc/materialize/pull/28493)**

4. Update the underlying `SourceDesc`/`IngestionDesc` and
   `SourceExport`/`IngestionExport` structs to include each export's specific
   details and options on its own struct, rather than using an implicit mapping into
   the top-level source's options.
   **Update: done in [PR](https://github.com/MaterializeInc/materialize/pull/28503)**

5. Update the source rendering operators to handle the new structures and allow
   outputting the same upstream table to more than one souce export.
   **Update: open PRs: [MySQL](https://github.com/MaterializeInc/materialize/pull/28671) [Postgres](https://github.com/MaterializeInc/materialize/pull/28676)**

6. Implement planning for `CREATE TABLE .. FROM SOURCE` and include a purification
   step akin to the purification for `ALTER SOURCE .. ADD SUBSOURCE`. This will verify
   the upstream permissions, schema, etc for the newly added source-fed table.
   **Update: open PRs [purification](https://github.com/MaterializeInc/materialize/pull/28943) and [planning](https://github.com/MaterializeInc/materialize/pull/28954)**

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
include a `details: SourceExportDetails` field on each export, such that all
the individual details necessary to render that export are stored here and
not on the top-level `Ingestion`:

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
    },

    ...
}


/// this is an example and these enum structs would likely be their own types
pub enum SourceExportDetails {
    Kafka,
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
        external_reference: UnresolvedItemName,
        details: ExportDetails,
    },
```

The `create_collections` method in the storage client currently maps a
`DataSource::IngestionExport` into a storage `SourceExport` struct and then
inserts this into the appropriate `IngestionDescription` of the top-level
source. This is used in source rendering to figure out what upstream
reference should be mapped to a specific output collection.

The storage `SourceExport` struct will be updated to include the `details`
field, populated from the `IngestionExport`'s `details`:

```rust
pub struct SourceExport<O: proptest::prelude::Arbitrary, S = ()> {
   /// Which output from the ingestion this source refers to.
   pub ingestion_output: O,
   /// The collection metadata needed to write the exported data
   pub storage_metadata: S,
   /// How to project the ingestion output to this export's collection.
   pub details: ExportDetails,
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

#### Kafka Sources

Kafka Sources will require more refactoring to move to a model where a single Kafka Source
can output to more than one `SourceExport`.

Since Kafka Sources only ever refer to a single Kafka Topic, a user will likely only ever
add a new 'Table' to a Kafka Source to handle a schema change in the messages published
to that topic.

When a new `SourceExport` is added to a Kafka Source, the new export needs to be hydrated
from the upstream topic to ensure it has a complete view of the topic contents.

This creates several potential issues regarding the semantics of Kafka Sources:

-   If a new `SourceExport` is added, its resume upper will be the minimum value. If this
    resume upper is merged with the existing resume upper for the source (which is what
    happens in Postgres and MySQL sources), it will cause the consumer to begin reading
    from each partition at the lowest offset available (or the 'start offset' defined
    on the Kafka Source). This means that we will potentially do a long rehydration
    to catch up to the previous resume-upper, such that existing `SourceExports` will not
    see any new data until the rehydration completes.
-   There is one existing Consumer Group ID used by the Kafka Source, and the source
    commits the consumer offset to the kafka broker each time the progress collection
    advances through reclocking. Since the Consumer may need to read from earlier
    offsets to complete the snapshot of the new `SourceExport`, it's unclear how this
    may affect external progress tracking and broker behavior.

Open Question:

-   Do we need to do anything special to handle any of the above?

#### Migration of source statements and collections

We would generate a catalog migration to generate new `CREATE TABLE` statements where
necessary and shuffle the collections being pointed at by each statement. The intent
is to preserve the names associated with each collection even if the statement that
identifies each collection is updated. The migration would do the following:

-   For existing sources where the primary relation has the data (e.g. Kafka sources):
    -   Generate a new `CREATE TABLE` statement that is tied to the current primary collection
        and name it `<source>`
    -   Change the `CREATE SOURCE` statement to point to the current progress collection and
        change the source name to `<source>_progress`
    -   Drop the existing `CREATE SUBSOURCE <source>_progress` statement since this will no
        longer have a collection to own
-   For existing multi-output sources (e.g. Postgres & MySQL sources):
    -   Convert subsources to `CREATE TABLE` statements with the same name
    -   Change the `CREATE SOURCE` statement to point to the current progress collection and
        change the source name to `<source>_progress`
    -   Drop the existing `CREATE SUBSOURCE <source>_progress` statement since this will no
        longer have a collection to own
    -   Drop the existing top-level collection tied to the source, since this is already
        unused and will no longer be owned by any statement

#### Exposing available upstream references to users

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

Introduce a `mz_catalog.mz_available_source_references` with the following columns:

-   `source_id`: The source (e.g. a Postgres, MySQL, Kafka source) this reference is available for.
-   `reference`: The 'external reference' for this available object, such that using `reference`
    in a `CREATE TABLE <table_name> FROM SOURCE <source_id> (REFERENCE <reference>)` would
    construct a table ingesting this upstream reference. This is essentially the fully-qualified
    upstream name of the reference.
-   `columns`: If available, an array of of columns that this external reference would have if it
    were ingested as a table in Materialize. This will be NULL for source types where an
    external registry is needed to determine the upstream schema (e.g. Kafka).
-   `name`: The object name of the reference. E.g. for a Postgres source this is the table name,
    and for kafka is the topic name.
-   `namespace`: Any namespace that this reference object exists in, if applicable. For Postgres
    this would be `<database>.<schema>`, for MySQL this would be `<schema>`, and for Kafka this
    would be NULL.
-   `last_updated_at`: The timestamp of when this reference was fetched from the upstream system.

Note that we do not expose the types of any of the upstream columns, since each source has
different constructs for how types are represented, and since some source-types can't
represent all upstream types (e.g. MySQL) without knowing which of them will be represented
as TEXT using a TEXT COLUMNS option. Therefore the list of `columns` does not imply that each
of those columns is one that Materialize can actually ingest.

A new `REFRESH SOURCE <source_id or name>` statement will be introduced that can trigger
a refresh of the rows in `mz_catalog.mz_available_source_references` that pertain to the
requested source. Otherwise, this catalog table will only be populated at source creation
time and upon a defined refresh interval in the source operator (perhaps every 60 seconds).

### System Catalog & Introspection Updates

We currently surface details about both Sources and Tables through various system catalog
tables in both the `mz_catalog` and `mz_internal` schemas. These expose information
about the objects themselves as well as statistics and statuses emitted from the active dataflows for each source.

We would make the following updates to provide users and Materialize employees the same
ability to introspect source-fed tables that they currently have for subsources.

#### [mz_catalog.mz_tables](https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_tables)

`CREATE TABLE .. FROM SOURCE` statements would be added to this catalog table.

Two new columns would be added:

`data_source_type`: distinguishes these tables
from 'normal' tables that are written to directly by users. The value of this will
be one of `'direct'` or `'source'`.

`data_source_id`: a nullable column that contains the `id` of the data source,
if there is one. In the case of `CREATE TABLE .. FROM SOURCE` statements this would
contain the `id` of the primary source.

#### [mz_catalog.mz_sources](https://materialize.com/docs/sql/system-catalog/mz_catalog/#mz_sources)

`CREATE TABLE .. FROM SOURCE` statements will be added to this catalog table, despite
them being 'tables', since this catalog table contains columns that are unique to
source-related objects (e.g. `connection_id`).

We will add the variant `'table'` to the `type` column which is currently a `text` column
containing one of:
`'kafka', 'mysql', 'postgres', 'load-generator', 'progress', or 'subsource'.`

Once all customer's sources have been migrated to the new model, the `'subsource'` variant will be removed from the `type` column.

Open Question: Will it be confusing to users that these source-fed tables will appear
in both the `mz_sources` and `mz_tables` catalog tables?

#### [mz_internal.mz_postgres_source_tables](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_postgres_source_tables) and [mz_internal.mz_mysql_source_tables](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_mysql_source_tables)

Each row in these tables contains a mapping from a `subsource` `id` to the external
`schema` and `table` of the Postgres/MySQL table being ingested into that subsource.

These will be updated to include the same mappings for `CREATE TABLE .. FROM SOURCE`
table objects to the external `schema` and `table` they are linked to.

#### [mz_internal.mz_source_statistics](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_source_statistics)

This table does not require any changes to its schema. However it is important to highlight
that since multiple tables will be able to ingest from the same upstream external reference,
the values reported in the following fields will be representative of the total number of
records output to all tables rather than the number of rows ingested from the external references
of the upstream system:

-   updates_staged
-   updates_committed
-   records_indexed
-   bytes_indexed
-   snapshot_records_known
-   snapshot_records_staged

#### [mz_internal.mz_source_statuses](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_source_statuses)

Similar to the change to `mz_catalog.mz_sources`, The `"table"` variant will be added
to the `type` column and all `CREATE TABLE .. FROM SOURCE` statements
will report a status in this table with the same semantics as existing subsources.

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
   (https://github.com/MaterializeInc/materialize/issues/24843). This would be an
   easier effort to implement, but rather than doing this work in the short term
   for it to be later ripped out, it is preferable to implement the interface
   that solves both this need and the other problems outlined above.

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->

1. How does this affect the system catalog, and source-specific introspection?
   (e.g. `mz_source_statistics`)

    Update: Section added above
