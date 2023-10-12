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

{{< public-preview />}}

{{% create-source/intro %}}
Webhook sources expose a [public URL](#webhook-url) that allow your applications to push webhook events into Materialize.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-webhook.svg" >}}

### `webhook_check_option`

{{< diagram "webhook-check-option.svg" >}}

Field                            | Use
---------------------------------|--------------------------
  _src_name_                     | The name for the source
 **IN CLUSTER** _cluster_name_   | The [cluster](/sql/create-cluster) to maintain this source.
 **INCLUDE HEADER**              | Map a header value from a request into a column.
 **INCLUDE HEADERS**             | Include a column named `'headers'` of type `map[text => text]` containing the headers of the request.
 **CHECK**                       | Specify a boolean expression that is used to validate each request received by the source.

### `CHECK WITH` options

Field                  | Type                | Description
-----------------------|---------------------|--------------
`BODY`                 | `text` or `bytea`   | Provide a `body` column to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the type can be changed to `bytea` with the optional **BYTES** keyword.
`HEADERS`              | `map[text=>text]` or `map[text=>bytea]` | Provide a column `'headers'` to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the type can be changed to `map[text => bytea]` with the optional **BYTES** keyword.
`SECRET` _secret_name_ | `text` or `bytea`    | Provide a _secret_name_ column to the check expression, with the value of the [`SECRET`](/sql/create-secret). The column can be renamed with the optional **AS** _alias_ statement, and the type can be changed to `bytea` with the optional **BYTES** keyword.

## Supported formats

|<div style="width:290px">Body format</div> | Type      | Description       |
--------------------------------------------| --------- |-------------------|
| Bytes                                     | `bytea`   | Does **no parsing** of the request, and stores the body of a request as it was received. |
| JSON                                      | `jsonb`   | Parses the body of a request as JSON. If the body is not valid JSON, a response of `400` Bad Request will be returned. |
| Text                                      | `text`    | Parses the body of a request as `UTF-8` text. If the body is not valid `UTF-8`, a response of `400` Bad Request will be returned. |

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

```sql
CREATE SOURCE my_webhook_source IN CLUSTER my_cluster FROM WEBHOOK
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

```sql
CREATE SOURCE my_webhook_source IN CLUSTER my_cluster FROM WEBHOOK
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

```sql
CREATE SOURCE my_webhook_source IN CLUSTER my_cluster FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS, BODY AS request_body,
      SECRET my_webhook_shared_secret
    )
    decode(headers->'x-signature', 'base64') = hmac(request_body, my_webhook_shared_secret, 'sha256')
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

```sql
CREATE SOURCE my_webhook_temporary_debug IN CLUSTER my_cluster FROM WEBHOOK
  -- Specify the BODY FORMAT as TEXT or BYTES,
  -- which is how it's provided to CHECK.
  BODY FORMAT TEXT
  INCLUDE HEADERS;
```

Once you have a few events in _my_webhook_temporary_debug_, you can query it with your would-be
`CHECK` statement.

```sql
SELECT
  -- Your would-be CHECK statement.
  decode(headers->'signature', 'base64') = hmac(headers->'timestamp' || body, 'my key', 'sha512')
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
using a [`MATERIALIZED VIEW`](/sql/create-materialized-view/) and the `DISCINCT ON` clause.

```sql
CREATE MATERIALIZED VIEW my_webhook_idempotent IN CLUSTER my_compute_cluster AS (
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

```sql
CREATE MATERIALIZED VIEW my_build_jobs_merged IN CLUSTER my_compute_cluster AS (
  SELECT DISTINCT ON (id) *
  FROM (
    SELECT
      body->>'id' as id,
      (body->>'started_at')::timestamptz as started_at,
      (body->>'finished_at')::timestamptz as finished_at
    FROM my_build_jobs_source
  )
  ORDER BY id, finished_at NULLS LAST, started_at NULLS LAST
);
```

{{< note >}}

If the feature is enabled, when casting from `text` to `timestamp` you should prefer to use the
[`try_parse_monotonic_iso8601_timestamp`](/sql/functions/pushdown/) function, which enables
[temporal filter pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

{{< /note >}}

## Request limits

Webhook sources apply the following limits to received requests:

* Maximum size of the request body is `2MB`. Requests larger than this will fail
  with 413 Payload Too Large.
* Maximum number of concurrent connections is 250, across all webhook sources.
  Trying to connect when the server is at the maximum will return 429 Too Many
  Requests.
* Requests that contain a header name specified more than once will be rejected
  with `401 Unauthorized`.

## Examples

### Using basic authentication

[Basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme)
enables a simple and rudimentary way to grant authorization to your webhook
source.

To store the sensitive credentials and make them reusable across multiple
`CREATE SOURCE` statements, use [secrets](https://materialize.com/docs/sql/create-secret/).

```sql
CREATE SECRET basic_hook_auth AS 'Basic <base64_auth>';
```

### Creating a source

After a successful secret creation, you can use the same secret to create
different webhooks with the same basic authentication to check if a request is
valid.

```sql
CREATE SOURCE webhook_with_basic_auth IN CLUSTER my_cluster
FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS request_body,
        SECRET basic_hook_auth
      )
      headers->'authorization' = BASIC_HOOK_AUTH
    );
```

Your new webhook is now up and ready to accept requests using basic
authentication.

### Connecting with Amazon EventBridge

[Amazon EventBridge](https://aws.amazon.com/eventbridge/) is a serverless event
bus that allows you to send events from your AWS services to external
destinations. You can ingest these events into Materialize using a webhook
source, and join them with your other data!

The first step for setting up a webhook source is to create a shared secret.
While this isn't required, it's the recommended best practice.

```sql
CREATE SECRET event_bridge_api_key AS 'abc123';
```

When you create a new EventBridge rule, you should make sure to include this
shared secret as a header in each request, so that Materialize can validate
it.

After defining the shared secret, create the source:

```sql
CREATE SOURCE my_event_bridge_source IN CLUSTER my_cluster FROM WEBHOOK
  BODY FORMAT JSON
  -- Includes all headers, but filters out our shared secret.
  INCLUDE HEADERS ( NOT 'x-mz-api-key' )
  CHECK (
    WITH ( HEADERS, SECRET event_bridge_api_key AS secret)
    headers->'x-mz-api-key' = secret
  );
```

This creates a source called _my_event_bridge_source_ and installs it in cluster
named _my_cluster_. The source will have two columns, _body_ of type `jsonb`
and _headers_ of type `map[text=>text]`. The shared secret will be used to
validate each request, but will be filtered out of the map in the _headers_
column.

After the source is created, you can connect with EventBridge. Follow
[Amazon's tutorial for connecting with Datadog](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-tutorial-datadog.html),
using the shared secret specified in the `CHECK` statement (`x-mz-api-key`) as
the **API key name** for request validation.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)
