---
title: "CREATE TABLE: Webhook"
description: "Reference page for `CREATE TABLE`. `CREATE TABLE` creates a table that is persisted in durable storage."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'create-table'
    name: "Webhook"
aliases:
  - /sql/create-source/webhook/
---

{{< source-versioning-disambiguation is_new=true other_ref="[old syntax](/sql/create-source-v1/webhook/)" >}}

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create webhook-populated tables. Webhook-populated
tables cannot be written to by the user; they are read-only. These tables are
populated through data posted to the associated **public** webhook URL, which is
automatically created with the table creation.

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, and views; and you
can create views/materialized views/indexes on tables.

## Syntax

{{% include-example file="examples/create_table/example_webhook_table"
 example="syntax" %}}

{{% include-example file="examples/create_table/example_webhook_table"
 example="syntax-options" %}}

## Details

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>



## Request limits

Webhook sources apply the following limits to received requests:

* The maximum size of the request body is **`2MB`**. Requests larger than this
  will fail with `413 Payload Too Large`.
* The rate of concurrent requests/second across **all** webhook sources
  is **500**. Trying to connect when the server is at capacity will fail with
  `429 Too Many Requests`.
* Requests that contain a header name specified more than once will be rejected
  with `401 Unauthorized`.


### Required privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-table.md" >}}

## Examples

### Create a table (Webhook-populated)

{{% include-example file="examples/create_table/example_kafka_table_webhook"
 example="create-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_webhook"
 example="post-to-webhook" %}}

{{% include-example file="examples/create_table/example_kafka_table_webhook"
 example="read-from-table" %}}

{{% include-example file="examples/create_table/example_kafka_table_webhook"
 example="create-a-view-from-table" %}}

#### JSON arrays

You can automatically expand a batch of requests formatted as a JSON array into
separate rows using `BODY FORMAT JSON ARRAY`.

```mzsql
-- Webhook source that parses request bodies as a JSON array.
CREATE SOURCE webhook_source_json_array FROM WEBHOOK
  BODY FORMAT JSON ARRAY
  INCLUDE HEADERS;
```

If you `POST` a JSON array of three elements to `webhook_source_json_array`,
three rows will get appended to the source.

```bash
POST webhook_source_json_array
[
  { "event_type": "a" },
  { "event_type": "b" },
  { "event_type": "c" }
]
```

```mzsql
SELECT COUNT(body) FROM webhook_source_json_array;
----
3
```

You can also post a single object to the source, which will get appended as one
row.

```bash
POST webhook_source_json_array
{ "event_type": "d" }
```

```mzsql
SELECT body FROM webhook_source_json_array;
----
{ "event_type": "a" }
{ "event_type": "b" }
{ "event_type": "c" }
{ "event_type": "d" }
```

#### Newline-delimited JSON (NDJSON)

You can automatically expand a batch of requests formatted as NDJSON into
separate rows using `BODY FORMAT JSON`.

```mzsql
-- Webhook source that parses request bodies as NDJSON.
CREATE SOURCE webhook_source_ndjson FROM WEBHOOK
BODY FORMAT JSON;
```

If you `POST` two elements delimited by newlines to `webhook_source_ndjson`, two
rows will get appended to the source.

```bash
POST 'webhook_source_ndjson'
  { 'event_type': 'foo' }
  { 'event_type': 'bar' }
```

```mzsql
SELECT COUNT(body) FROM webhook_source_ndjson;
----
2
```

### Handling duplicated and partial events

Given any number of conditions, e.g. a network hiccup, it's possible for your application to send
an event more than once. If your event contains a unique identifier, you can de-duplicate these events
using a [`MATERIALIZED VIEW`](/sql/create-materialized-view/) and the `DISTINCT ON` clause.

```mzsql
CREATE MATERIALIZED VIEW my_webhook_idempotent AS (
  SELECT DISTINCT ON (body->>'unique_id') *
  FROM my_webhook_source
  ORDER BY id
);
```

We can take this technique a bit further to handle partial events. Let's pretend our application
tracks the completion of build jobs, and it sends us JSON objects with following structure.

Key           | Value   | Optional? |
--------------|---------|-----------|
_id_          | `text`  | No
_started_at_  | `text`  | Yes
_finished_at_ | `text`  | Yes

When a build job starts we receive an event containing _id_ and the _started_at_ timestamp. When a
build finished, we'll receive a second event with the same _id_ but now a _finished_at_ timestamp.
To merge these events into a single row, we can again use the `DISTINCT ON` clause.

```mzsql
CREATE MATERIALIZED VIEW my_build_jobs_merged AS (
  SELECT DISTINCT ON (id) *
  FROM (
    SELECT
      body->>'id' as id,
      try_parse_monotonic_iso8601_timestamp(body->>'started_at') as started_at,
      try_parse_monotonic_iso8601_timestamp(body->>'finished_at') as finished_at
    FROM my_build_jobs_source
  )
  ORDER BY id, finished_at NULLS LAST, started_at NULLS LAST
);
```

{{< note >}}
When casting from `text` to `timestamp` you should prefer to use the [`try_parse_monotonic_iso8601_timestamp`](/sql/functions/pushdown/)
function, which enables [temporal filter pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).
{{< /note >}}

## Related pages

- [`INSERT`](/sql/insert)
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](/sql/drop-table)
- [Ingest data](/ingest-data/)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
