# Subsource deprecation & source blue-green schema changes

This design encompases 2 existing proposals also known as
'Subsource Demuxing' and 'Subsources as Tables'.

Associated:
- https://github.com/MaterializeInc/materialize/issues/20208
- https://github.com/MaterializeInc/materialize/issues/24843
- https://github.com/MaterializeInc/materialize/issues/15897
- https://github.com/MaterializeInc/materialize/issues/17021

## The Problems

TLDR:
- Subsources are confusing and not implemented uniformly across source types
- It's impossible to do a blue-green workflow in sources for handling
  schema-changes in upstream systems

Users currently have to learn about *sources*, *subsources*, *tables*,
*materialized views*, *indexes*, and *sinks* when onboarding to Materialize.

If they'd like to ingest data from both an upstream Postgres/MySQL database
and a Kafka topic, they also need to learn the fragmented user interface
for dealing with data ingested from upstream data sources. Our existing
model of a *source* does not provide a consistent interface among source types.

Single-output Kafka sources put their data under the source's primary relation,
whereas multi-output Postgres/MySQL sources do not allow querying the source's primary
relation and instead contain *subsources*, an entirely separate concept.

Our existing *source* models also make it difficult to deal with upstream schema changes,
such that users need to drop and recreate a *source* or *subsource* and any dependent objects
to begin ingesting data with a new schema. This can cause long-periods of downtime if the
new source needs to reingest the entire upstream history. This can also cause them to
lose their _reclocking_ information which makes it impossible to implement a correct
end-to-end *source* -> *sink* pipeline.

This change aims to simplify the user experience and create a more unified
model for managing data ingested from an upstream source, while providing
more flexibility in handling upstream schema changes.


## Success Criteria

- The mental overhead & learning time for users to ingest data from an upstream system
  is decreased.
- All source types use a consistent interface to retrieve progress information and
  ingested data. The primary *source* object represents the same concept across
  all source types.
- The same upstream table/topic can be ingested into multiple collections in Materialize,
  each with a potentially different projection of the upstream schema. This enables
  a user to do blue-green schema-change workflows without unavailability.
- *sources* can maintain their progress collection while the set of collections being
  ingested are modified, allowing an end-to-end *source* -> *sink* schema-change workflow
  that does not emit redundant data (reusable reclocking).

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

- Accomplishing 'subsource demuxing' using the concept of *subsources* in-use today.
  The goals of https://github.com/MaterializeInc/materialize/issues/24843 will be
  accomplished during the process of replacing *subsources* with *tables* (see
  solution proposal below).

- Dealing with Webhook sources. These are fundamentally different than the other
  storage-owned source types whose operators implement the `SourceRender` trait,
  and they do not contain a 'progress' collection. While they may also be
  converted to use 'tables' in the future, that work is not in-scope for this proposal.


## Solution Proposal

The concept of a *source* will be unified around a single relation representing
the *progress* of that source's ingestion pipeline. A query of any *source*
object (of any type) will return the data that is currently in the
`<source>_progress` collection.

We have been investing in the user experience around *tables*, such that
replacing *subsources* with *tables* would reduce the overall
mental overhead for new users.

By allowing tables to be marked as *read-only*, we can easily migrate
existing subsources to be tables without needing to figure out how to merge
`INSERT/UPDATE/DELETE` statements with the data written by their corresponding source.

The default `CREATE SOURCE ..` statement will just create a top-level source object
that represents a single ingestion of data from an upstream system. To actually
ingest data from that upstream system into a persist collection, the user will
use something akin to
`CREATE TABLE <table> (<cols>) OF SOURCE <source> WITH (<upstream_ref>)`.

We will allow more than one table to reference the same `<upstream_ref>`, using a
potentially different set of columns. This allows a user to handle an upstream
schema change by creating a new table for the same upstream table and then performing
a blue-green swap operation to switch their downstream dependencies to the new table,
and then drop the old one.

The existing options to `CREATE SOURCE` for Postgres & MySQL sources that specify
which tables to ingest (`FOR ALL TABLES`, `FOR SCHEMAS` and `FOR TABLES`) will be
removed. Instead, all upstream tables to ingest from this source will need to be
explicitly referenced in a `CREATE TABLE` statement. While this may seem controversial,
more often than not these options cause upstream data that does not need to be
brought into Materialize to be ingested, and by using explicit statements for each
table to be ingested it makes Materialize configuration much more amenable to
object<->state mappings in tools like DBT and Terraform.

We can eventually reintroduce syntactic sugar to perform a similar function to
`FOR ALL TABLES` and `FOR SCHEMAS` if necessary.


### Implementation Plan

1. Separate the planning of *sources* and *subsources* such that sources are fully
   planned before any linked subsources. Part of this work has been prototyped in:
   https://github.com/MaterializeInc/materialize/pull/27320

2. Introduce the concept of *read-only tables* that link to a source ingestion
   and an upstream 'reference' specifying which upstream table to ingest into this table.

3. Update the CREATE TABLE statement to allow creation of a read-only table
   with a reference to a source and an upstream-reference:
   (`CREATE TABLE <new_table> FROM SOURCE <source> (upstream_reference)`)

4. Update planning for `CREATE TABLE` to include a purification step akin to the
   purification for `ALTER SOURCE .. ADD SUBSOURCE`. This will verify the upstream
   permissions, schema, etc for the newly added source-fed table.

5. Update the underlying *source* (ingestion) and source_export/ingestion_export
   structures to allow more than one *source_export* to refer to an upstream
   reference, and each *source_export* to have its own projection of the upstream data

5. Update the storage controller to use both *subsources* and *read-only tables*
   as *source_exports* for existing multi-output sources (postgres & mysql)

6. Update the source rendering operators to handle the new structures and allow
   outputting the same upstream table to more than one souce export.

7. Migrate existing sources to the new source model (make all sources 'multi-output' sources):
   - For existing sources where the primary relation has the data (e.g. Kafka sources):
     - Create a read-only table for the current primary relation called `<source>`
     - Rename the source itself `<source>_progress` and drop the existing
      `<source>_progress` object
   - For existing multi-output sources (e.g. Postgres & MySQL sources):
     - Convert subsources to read-only tables with the same name
     - Rename the source itself `<source>_progress` and drop the existing
      `<source>_progress` object

8. Remove subsource purification logic from `purify_create_source`, subsource statement parsing,
   and related planning code


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

We would update the `CREATE TABLE` statement to look similar to `CREATE SUBSOURCE` and
be able to optionally reference an upstream source and reference using `OF SOURCE` and `EXTERNAL REFERENCE`:
```sql
CREATE TABLE <name> (<cols>) OF SOURCE <source_name> WITH (EXTERNAL REFERENCE = <upstream table name>)
```
`<cols>` can be optionally specified by the user to request a subset of the upstream table's
columns, but will not be permitted to include user-specified column types, since these will
be determined by the upstream source details.

`CreateTableStatement` would be updated to include the new option type `CreateTableOptionName::ExternalReference` and a `pub of_source: Option<T::ItemName>`
field to reference the optional upstream source.

We would then introduce a new `TableDataSource` enum and add a field to the `Table` objects
used for the in-memory catalog and SQL planning:
```rust
pub enum TableDataSource {
   /// The table owns data created via INSERT/UPDATE/DELETE statements.
   TableWrites,

   /// The table receives its data from the identified ingestion, specifically
   /// the output identified by `external_reference`, using the projection
   /// `external_projection` to map the upstream relation to this relation.
   /// This table type does not support INSERT/UPDATE/DELETE statements.
   IngestionExport {
      ingestion_id: GlobalId,
      external_reference: UnresolvedItemName,
      external_projection: Vec<u8>,
   }
}

pub struct Table {
   pub create_sql: Option<String>,
   pub desc: RelationDesc,
   ...
   pub data_source: TableDataSource
}
```

The planning for `CREATE TABLE` will be adjusted to include a purification step
akin to the purification in `purify_alter_source`. This will also map any
specified columns into a projection of the upstream table's column order.
This will also verify the upstream permissions, schema, etc of the upstream table,
and generate a new `details` for the top-level source, that is merged with the
top-level source during sequencing (how alter source works now).


#### Storage Collection Coordination

The coordinator's `bootstrap_storage_collections` will be updated to handle
tables with the `TableDataSource::IngestionExport { .. }`, mapping them to
a storage controller `CollectionDescription` with a `DataSource::IngestionExport`
rather than the existing `DataSourceOther::TableWrites` value used for all tables.

The storage controller's `DataSource::IngestionExport` struct will be updated to
include an optional projection, populated based on the table projection mentioned above:
```rust
    IngestionExport {
        ingestion_id: GlobalId,
        // This is an `UnresolvedItemName` because the structure works for PG,
        // MySQL, and load generator sources. However, in the future, it should
        // be sufficiently genericized to support all multi-output sources we
        // support.
        external_reference: UnresolvedItemName,
        external_projection: Option<Vec<u8>>,
    },
```

The `create_collections` method in the storage client currently maps a
`DataSource::IngestionExport` into a storage `SourceExport` struct and then
inserts this into the appropriate `IngestionDescription` of the top-level
source. This is used in source rendering to figure out what upstream
reference should be mapped to a specific output collection.

The storage `SourceExport` struct will be updated to include an optional
projection field, populated from the `IngestionExport`'s `external_projection`:
```rust
pub struct SourceExport<O: proptest::prelude::Arbitrary, S = ()> {
   /// Which output from the ingestion this source refers to.
   pub ingestion_output: O,
   /// The collection metadata needed to write the exported data
   pub storage_metadata: S,
   /// How to project the ingestion output to this export's collection.
   pub output_projection: Option<Vec<u8>>,
}
```

#### Source Rendering

Currently, when an `IngestionDescription` is used inside `build_ingestion_dataflow`
(the method that renders the various operators needed to run a source ingestion),
it determines the `output_index` of each collection to be output by the ingestion
by calling the `IngestionDescription::source_exports_with_output_indices` method.

This method maps the `external_reference` (the upstream table name) of each `SourceExport`
to the index of that upstream table in the source's `details` struct (which is created
during purification -- its a vector of upstream table descriptions).

Then inside each source implementation, we currently assume that each
`SourceExport` uniqely corresponds to a single `output_index`, and that each `output_index`
corresponds to the same index of the upstream table in the source's upstream `details`.

This assumption is part of what prevents an upstream table from being ingested into
more than one collection.

We would update each source implementation to assume that more than one `SourceExport`
can share an `output_index` and we would update the `source_exports` map created
by `source_exports_with_output_indices` to include the `details` index explicitly.

The `build_ingestion_dataflow` method currently demuxes the output collection of
each source by each `output_index` and pushes each data stream to the appropriate
`persist_sink`. We would update this method so that it applies the `output_projection`
for each `SourceExport` on each data stream before being pushed to its `persist_sink`.

#### Migration of source names (progress collections) & kafka source -> table migration

TODO

## Minimal Viable Prototype


## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

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
