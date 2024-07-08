---
title: "CREATE SOURCE: Webhook"
description: "Ingesting data into Materialize with HTTP requests"
pagerank: 50
menu:
  main:
    parent: 'create-source'
    identifier: webhook
    name: Webhook
    weight: 40
---

{{% create-source/intro %}}
Webhook sources expose a [public URL](#webhook-url) that allows your applications to push webhook events into Materialize.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-webhook.svg" >}}

### `webhook_check_option`

{{< diagram "webhook-check-option.svg" >}}

Field                            | Use
---------------------------------|--------------------------
  _src_name_                     | The name for the source.
 **IN CLUSTER** _cluster_name_   | The [cluster](/sql/create-cluster) to maintain this source.
 **INCLUDE HEADER**              | Map a header value from a request into a column.
 **INCLUDE HEADERS**             | Include a column named `'headers'` of type `map[text => text]` containing the headers of the request.
 **CHECK**                       | Specify a boolean expression that is used to validate each request received by the source.

### `CHECK WITH` options

Field                  | Type                | Description
-----------------------|---------------------|--------------
`BODY`                 | `text` or `bytea`   | Provide a `body` column to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the data type can be changed to `bytea` with the optional **BYTES** keyword.
`HEADERS`              | `map[text=>text]` or `map[text=>bytea]` | Provide a column `'headers'` to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the data type can be changed to `map[text => bytea]` with the optional **BYTES** keyword.
`SECRET` _secret_name_ | `text` or `bytea`    | Securely provide a [`SECRET`](/sql/create-secret) to the check expression. The `constant_time_eq` validation function **does not support** fully qualified secret names: if the secret is in a different namespace to the source, the column can be renamed with the optional **AS** _alias_ statement. The data type can also be changed to `bytea` using the optional **BYTES** keyword.

## Supported formats

|<div style="width:290px">Body format</div> | Type      | Description       |
--------------------------------------------| --------- |-------------------|
| `BYTES`                                   | `bytea`   | Does **no parsing** of the request, and stores the body of a request as it was received. |
| `JSON`                                    | `jsonb`   | Parses the body of a request as JSON. Also accepts events batched as newline-delimited JSON (`NDJSON`). If the body is not valid JSON, a response of `400` Bad Request will be returned. |
| `JSON ARRAY`                              | `jsonb`   | Parses the body of a request as a list of JSON objects, automatically expanding the list of objects to individual rows. Also accepts a single JSON object. If the body is not valid JSON, a response of `400` Bad Request will be returned. |
| `TEXT`                                    | `text`    | Parses the body of a request as `UTF-8` text. If the body is not valid `UTF-8`, a response of `400` Bad Request will be returned. |

## Output

If source creation is successful, you'll have a new source object with
name _src_name_ and, based on what you defined with `BODY FORMAT` and `INCLUDE
HEADERS`, the following columns:

Column     | Type                        | Optional?                                      |
-----------|-----------------------------|------------------------------------------------|
 `body`    | `bytea`, `jsonb`, or `text` |                                                |
 `headers` | `map[text => text]`         | ✓ . Present if `INCLUDE HEADERS` is specified. |

### Webhook URL

After source creation, the unique URL that allows you to **POST** events to the
source can be looked up in the [`mz_internal.mz_webhook_sources`](/sql/system-catalog/mz_internal/#mz_webhook_sources)
system catalog table. The URL will have the following format:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

A breakdown of each component is as follows:

- `<HOST>`: The Materialize instance URL, which can be found on the [Materialize console](https://console.materialize.com/).
- `<database>`: The name of the database where the source is created (default is `materialize`).
- `<schema>`: The schema name where the source gets created (default is `public`).
- `<src_name>`: The name you provided for your source at the time of creation.

{{< note >}}
This is a public URL that is open to the internet and has no security. To
validate that requests are legitimate, see [Validating requests](#validating-requests).
For limits imposed on this endpoint, see [Request limits](#request-limits).
{{< /note >}}

## Features

### Exposing headers

In addition to the request body, Materialize can expose headers to SQL. If a
request header exists, you can map its fields to columns using the `INCLUDE
HEADER` syntax.

```mzsql
CREATE SOURCE my_webhook_source FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADER 'timestamp' as ts
  INCLUDE HEADER 'x-event-type' as event_type;
```

This example would have the following columns:

Column      | Type    | Nullable? |
------------|---------|-----------|
 body       | `jsonb` | No        |
 ts         | `text`  | Yes       |
 event_type | `text`  | Yes       |

All of the header columns are nullable. If the header of a request does not
contain a specified field, the `NULL` value will be used as a default.

#### Excluding header fields

To exclude specific header fields from the mapping, use the `INCLUDER HEADERS`
syntax in combination with the `NOT` option. This can be useful if, for
example, you need to accept a dynamic list of fields but want to exclude
sensitive information like authorization.

```mzsql
CREATE SOURCE my_webhook_source FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADERS ( NOT 'authorization', NOT 'x-api-key' );
```

This example would have the following columns:

Column      | Type                | Nullable?  |
------------|---------------------|------------|
 body       | `jsonb`             | No         |
 headers    | `map[text => text]` | No         |

All header fields but `'authorization'` and `'x-api-key'` will get included in
the `headers` map column.

### Validating requests

{{< warning >}}
Without a `CHECK` statement, **all requests will be accepted**. To prevent bad
actors from injecting data into your source, it is **strongly encouraged** that
you define a `CHECK` statement with your webhook sources.
{{< /warning >}}

It's common for applications using webhooks to provide a method for validating a
request is legitimate. You can specify an expression to do this validation for
your webhook source using the `CHECK` clause.

For example, the following source HMACs the request body using the `sha256`
hashing algorithm, and asserts the result is equal to the value provided in the
`x-signature` header, decoded with `base64`.

```mzsql
CREATE SOURCE my_webhook_source FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS, BODY AS request_body,
      SECRET my_webhook_shared_secret AS validation_secret
    )
    -- The constant_time_eq validation function **does not support** fully
    -- qualified secret names. We recommend always aliasing the secret name
    -- for ease of use.
    constant_time_eq(
        decode(headers->'x-signature', 'base64'),
        hmac(request_body, validation_secret, 'sha256')
    )
  );
```

The headers and body of the request are only subject to validation if `WITH
( BODY, HEADERS, ... )` is specified as part of the `CHECK` statement. By
default, the type of `body` used for validation is `text`, regardless of the
`BODY FORMAT` you specified for the source. In the example above, the `body`
column for `my_webhook_source` has a type of `jsonb`, but `request_body` as
used in the validation expression has type `text`. Futher, the request headers
are not persisted as part of `my_webhook_source`, since `INCLUDE HEADERS` was
not specified — but they are provided to the validation expression.

#### Debugging validation

It can be difficult to get your `CHECK` statement correct, especially if your
application does not have a way to send test events. If you're having trouble
with your `CHECK` statement, we recommend creating a temporary source without
`CHECK` and using that to iterate more quickly.

```mzsql
CREATE SOURCE my_webhook_temporary_debug FROM WEBHOOK
  -- Specify the BODY FORMAT as TEXT or BYTES,
  -- which is how it's provided to CHECK.
  BODY FORMAT TEXT
  INCLUDE HEADERS;
```

Once you have a few events in _my_webhook_temporary_debug_, you can query it with your would-be
`CHECK` statement.

```mzsql
SELECT
  -- Your would-be CHECK statement.
  constant_time_eq(
    decode(headers->'signature', 'base64'),
    hmac(headers->'timestamp' || body, 'my key', 'sha512')
  )
FROM my_webhook_temporary_debug
LIMIT 10;
```

{{< note >}}
It's not possible to use secrets in a `SELECT` statement, so you'll need to
provide these values as raw text for debugging.
{{< /note >}}

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

### Handling batch events

The application pushing events to your webhook source may batch multiple events
into a single HTTP request. The webhook source supports parsing batched events
in the following formats:

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

## Request limits

Webhook sources apply the following limits to received requests:

* The maximum size of the request body is **`2MB`**. Requests larger than this
  will fail with `413 Payload Too Large`.
* The rate of concurrent requests/second across **all** webhook sources
  is **500**. Trying to connect when the server is at capacity will fail with
  `429 Too Many Requests`.
* Requests that contain a header name specified more than once will be rejected
  with `401 Unauthorized`.

## Examples

### Using basic authentication

[Basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme)
enables a simple and rudimentary way to grant authorization to your webhook
source.

To store the sensitive credentials and make them reusable across multiple
`CREATE SOURCE` statements, use [secrets](https://materialize.com/docs/sql/create-secret/).

```mzsql
CREATE SECRET basic_hook_auth AS 'Basic <base64_auth>';
```

### Creating a source

After a successful secret creation, you can use the same secret to create
different webhooks with the same basic authentication to check if a request is
valid.

```mzsql
CREATE SOURCE webhook_with_basic_auth
FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS request_body,
        SECRET basic_hook_auth AS validation_secret
      )
      -- The constant_time_eq validation function **does not support** fully
      -- qualified secret names. We recommend always aliasing the secret name
      -- for ease of use.
      constant_time_eq(headers->'authorization', validation_secret)
    );
```

Your new webhook is now up and ready to accept requests using basic
authentication.

#### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that maps the individual fields to columns with the
required data types. To avoid doing this tedious task manually, you can use
[this **JSON parsing widget**](/sql/types/jsonb/#parsing)!

### Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)
