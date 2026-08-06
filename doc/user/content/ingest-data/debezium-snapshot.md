---
title: "Snapshot + Debezium CDC Pattern"
description: "How to combine a point-in-time snapshot with Debezium CDC to bring data into Materialize"
menu:
  main:
    parent: "ingest-data"
    name: "Snapshot + Debezium CDC"
    weight: 30
---

This is a guide for TiDB, Spanner, or any other source that does not have a native Materialize integration. To use this pattern you will need:

- To produce a point-in-time snapshot that can be loaded via `COPY FROM`
- To stream ongoing changes via Debezium (Kafka source with `APPEND ONLY` envelope)
- A timestamp field in the existing source that orders the records by time
  - This timestamp field will exist in both the snapshot and stream

{{< warning >}}
This is an advanced integration pattern. If you need help setting this up, reach out to Materialize support — we're happy to work through your specific use case and help you get started.
{{< /warning >}}

## Overview

This guide builds a materialized view (MV) source that combines a table point-in-time snapshot with a Debezium-formatted Kafka source. The materialized view keeps up to date with the source data by continuously applying the change stream onto the point-in-time table snapshot.

### The high-level idea (snapshot + incremental changes)

1. Start consuming CDC events
2. Take a snapshot of the source table
3. Record the cutover timestamp for that snapshot
4. Keep only CDC events after the cutover timestamp
5. Combine:
   - The snapshot rows
   - The "after" rows from the CDC event stream, which represent the new versions of the rows
   - And remove the "before" rows

The SQL logic of the materialized view looks like:

```sql
snapshot
UNION ALL after_rows
EXCEPT ALL before_rows
```

## Source setup (snapshot + Debezium CDC)

You will create three things:

### 1. Snapshot table in Materialize

Load the snapshot file into a Materialize table using `COPY FROM`. This table is a static "base" that represents the source data before the cutover moment.

### 2. Kafka source in Materialize

Create a Kafka source with the `APPEND ONLY` envelope that produces a stream of change events from the same source as the snapshot table. The stream should contain the Debezium structure with `before` and `after` fields. See example below:

```json
{
  "before": {
    "id": 123,
    "name": "John Doe",
    "email": "john@example.com"
  },
  "after": {
    "id": 123,
    "name": "John Doe",
    "email": "john.doe@example.com"
  },
  "source": {
    "record_timestamp": 1647623456789
  },
  "op": "u"
}
```

**Note:** The Debezium records should contain timestamps that are consistent with those in the snapshot table.

### 3. Materialized view

Create a materialized view that combines the snapshot and CDC stream and represents all source data that exists from the upstream system.

This MV will contain a parsing step to extract the following from the CDC stream:
- Typed columns for the `after` record (may be null for deletes)
- Typed columns for the `before` record (may be null for inserts)
- A cutover timestamp that is used to combine snapshot and CDC stream records

## Cutover timestamp setup

The preferred pattern is to inject it as a constant using a dbt macro:

- Render the timestamp into the MV SQL at deploy time
- This lets Materialize push down the filter:
  - Records with timestamps ≤ cutover timestamp can be discarded early, saving memory usage

Otherwise:

- Compute the timestamp in the derived source MV SQL that combines the snapshot and CDC records
- This logic would select a timestamp that is greater than the minimum timestamp of the CDC records recorded, and before the max timestamp of the snapshot records recorded

## Generalized MV SQL (template)

- `:cutover_ts` is the constant cutover timestamp of the snapshot. It's recommended that this is calculated outside of MV SQL definition and placed in using a dbt macro
- `<cols...>` are the columns of the source
- `<record_timestamp>` is your CDC sequence number or integer timestamp

```sql
CREATE MATERIALIZED VIEW <source_table> AS
WITH params AS (
  SELECT :cutover_ts AS cutover_ts
),

-- Extract AFTER records from the Debezium-formatted CDC stream
after_rows AS (
  SELECT
    <cols...>,
    <record_timestamp>
  FROM <debezium_cdc_source>
  WHERE after IS NOT NULL
),

-- Extract BEFORE records from the Debezium-formatted CDC stream
before_rows AS (
  SELECT
    <cols...>,
    <record_timestamp>
  FROM <debezium_cdc_source>
  WHERE before IS NOT NULL
),

-- Keep only records that occurred after the snapshot cutover
after_filtered AS (
  SELECT <cols...>
  FROM after_rows
  WHERE record_timestamp > (SELECT cutover_ts FROM params)
),

before_filtered AS (
  SELECT <cols...>
  FROM before_rows
  WHERE record_timestamp > (SELECT cutover_ts FROM params)
)

-- Combine base snapshot + adds - removes
SELECT <cols...>
FROM <snapshot_table>

UNION ALL

SELECT <cols...>
FROM after_filtered

EXCEPT ALL

SELECT <cols...>
FROM before_filtered;
```

## Re-snapshots and schema changes (without downtime)

Re-snapshots may be required when:

- The upstream schema changes
- The CDC stream stalls in a failure scenario

### High-level workflow

1. Start consuming CDC events with a new Kafka source
2. Create a new snapshot table with a new table name
3. Pick a new cutover timestamp
   - Record the timestamp that aligns the new snapshot with the CDC stream
4. Build a replacement MV definition
   - Create a new MV definition that references the new snapshot table and new cutover timestamp
5. **Swap it in using `ALTER REPLACE MATERIALIZED VIEW`**
   - This updates the MV definition without requiring downstream consumers to change what they reference

### Example

```sql
-- New snapshot table loaded via COPY FROM
-- (exact COPY statement depends on your file source and shape)

-- Replace the MV to point at the new snapshot + new cutover
ALTER MATERIALIZED VIEW <canonical_table>
REPLACE AS
WITH params AS (
  SELECT CAST(:new_cutover_ts AS <watermark_type>) AS cutover_ts
),
...
SELECT <cols...> FROM <new_snapshot_table>
UNION ALL
SELECT <cols...> FROM after_filtered
EXCEPT ALL
SELECT <cols...> FROM before_filtered;
```

## Related pages

- [Debezium](/ingest-data/debezium)
- [Kafka source with UPSERT envelope](/sql/create-source/kafka/#using-debezium)
- [`COPY FROM`](/sql/copy-from)
- [`ALTER MATERIALIZED VIEW`](/sql/alter-materialized-view)
