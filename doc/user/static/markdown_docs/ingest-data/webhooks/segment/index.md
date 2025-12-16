<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Ingest
data](/docs/self-managed/v25.2/ingest-data/)

</div>

# Segment

This guide walks through the steps to ingest data from
[Segment](https://segment.com/) into Materialize using the [Webhook
source](/docs/self-managed/v25.2/sql/create-source/webhook/).

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

### Before you begin

Ensure that you have:

- A Segment [source](https://segment.com/docs/connections/sources/) set
  up and running.

## Step 1. (Optional) Create a cluster

<div class="note">

**NOTE:** If you are prototyping and already have a cluster to host your
webhook source (e.g. `quickstart`), **you can skip this step**. For
production scenarios, we recommend separating your workloads into
multiple clusters for [resource
isolation](/docs/self-managed/v25.2/sql/create-cluster/#resource-isolation).

</div>

To create a cluster in Materialize, use the [`CREATE CLUSTER`
command](/docs/self-managed/v25.2/sql/create-cluster):

<div class="highlight">

``` chroma
CREATE CLUSTER webhooks_cluster (SIZE = '25cc');

SET CLUSTER = webhooks_cluster;
```

</div>

## Step 2. Create a secret

To validate requests between Segment and Materialize, you must create a
[secret](/docs/self-managed/v25.2/sql/create-secret/):

<div class="highlight">

``` chroma
CREATE SECRET segment_webhook_secret AS '<secret_value>';
```

</div>

Change the `<secret_value>` to a unique value that only you know and
store it in a secure location.

## Step 3. Set up a webhook source

Using the secret from the previous step, create a [webhook
source](/docs/self-managed/v25.2/sql/create-source/webhook/) in
Materialize to ingest data from Segment. By default, the source will be
created in the active cluster; to use a different cluster, use the
`IN CLUSTER` clause.

<div class="highlight">

``` chroma
CREATE SOURCE segment_source IN CLUSTER webhooks_cluster FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADER 'event-type' AS event_type
  INCLUDE HEADERS
  CHECK (
    WITH ( BODY BYTES, HEADERS, SECRET segment_webhook_secret BYTES AS validation_secret)
    -- The constant_time_eq validation function **does not support** fully
    -- qualified secret names. We recommend always aliasing the secret name
    -- for ease of use.
    constant_time_eq(decode(headers->'x-signature', 'hex'), hmac(body, validation_secret, 'sha1'))
  );
```

</div>

After a successful run, the command returns a `NOTICE` message
containing the unique [webhook
URL](/docs/self-managed/v25.2/sql/create-source/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You
will need it for the next step.

The URL will have the following format:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

If you missed the notice, you can find the URLs for all webhook sources
in the
[`mz_internal.mz_webhook_sources`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_webhook_sources)
system table.

### Access and authentication

<div class="warning">

**WARNING!** Without a `CHECK` statement, **all requests will be
accepted**. To prevent bad actors from injecting data into your source,
it is **strongly encouraged** that you define a `CHECK` statement with
your webhook sources.

</div>

The `CHECK` clause defines how to validate each request. At the time of
writing, Segment validates requests by signing them with an HMAC in the
`X-Signature` request header. The HMAC is a hex-encoded SHA1 hash using
the secret from **Step 2.** and the request body. Materialize decodes
the signature using the
[`decode`](/docs/self-managed/v25.2/sql/functions/#decode) function,
getting the raw bytes, and generate our own HMAC using the
[`hmac`](/docs/self-managed/v25.2/sql/functions/#hmac) function. If the
two values are equal, then the request is legitimate!

## Step 4. Create a webhook destination in Segment

1.  In Segment, go to **Connections \> Catalog**.

2.  Search and click **Webhooks (Actions)**.

3.  Click **Add destination**.

4.  Select a data source and click **Next**.

5.  Jot a *Destination Name* and click **Create Destination**.

## Step 5. Configure the mapping in Segment

A webhook destination in Segment requires a [data
mapping](https://segment.com/blog/data-mapping/) to send events from the
source to the destination. For this guide, the destination is the
Materialize source. Follow these steps to create the correct mapping:

1.  Go to your webhook destination created in the previous step.

2.  Click on **Mappings \> New Mapping**.

3.  Click **Send** and fill the configuration as follows:

    1.  In **Select events to map and send**, fill the conditions as you
        wish.
    2.  In **Add test**, select any of the events.
    3.  In **Select mappings**, fill the fields as follows:
        | Field | Value |
        |----|----|
        | Url | Use the URL from the [Step 3](#step-3-set-up-a-webhook-source). |
        | Method | `POST` |
        | Batch Size | `0` |
        | Headers | \- |
        | Data | `$event` |
        | Enable Batching | `No`. |

4.  Click **Test Mapping** and validate the webhook is working.

5.  After a succesful test, click **Save**.

<div class="note">

**NOTE:**

If **Test Mapping** fails in and throws a *“failed to validate the
request”* error, it means the shared secret is not right. To fix this,
follow this steps:

1.  In **Segment**, go to your webhook destination created in the **Step
    4**.
2.  Click **Settings**.
3.  In **Shared Secret**, enter the secret created in the **Step 2**.
4.  Click **Save Changes**.

</div>

## Step 6. Validate incoming data

With the source set up in Materialize and the webhook destination
configured in Segment, you can now query the incoming data:

1.  [In the Materialize console](/docs/self-managed/v25.2/console/),
    navigate to the **SQL Shell**.

2.  Use SQL queries to inspect and analyze the incoming data:

    <div class="highlight">

    ``` chroma
    SELECT * FROM segment_source LIMIT 10;
    ```

    </div>

## Step 7. Transform incoming data

### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing
view on top of your webhook source that uses [`jsonb`
operators](/docs/self-managed/v25.2/sql/types/jsonb/#operators) to map
the individual fields to columns with the required data types.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-page" class="tab-pane" title="Page">

<div class="highlight">

``` chroma
CREATE VIEW parse_segment AS SELECT
    body->>'anonymousId' AS anonymousId,
    body->>'channel' AS channel,
    body->'context'->>'ip' AS context_ip,
    body->'context'->>'userAgent' AS context_user_agent,
    (body->'integrations'->>'All')::bool AS integrations_all,
    (body->'integrations'->>'Mixpanel')::bool AS integrations_mixpanel,
    (body->'integrations'->>'Salesforce')::bool AS integrations_salesforce,
    body->>'messageId' AS messageId,
    body->>'name' AS name,
    body->'properties'->>'title' AS properties_title,
    body->'properties'->>'url' AS properties_url,
    try_parse_monotonic_iso8601_timestamp(body->>'receivedAt') AS received_at,
    try_parse_monotonic_iso8601_timestamp(body->>'sentAt') AS sent_at,
    try_parse_monotonic_iso8601_timestamp(body->>'timestamp') AS timestamp,
    body->>'type' AS type,
    body->>'userId' AS user_id,
    body->>'version' AS version
FROM segment_source;
```

</div>

</div>

<div id="tab-track" class="tab-pane" title="Track">

<div class="highlight">

``` chroma
CREATE VIEW parse_segment AS SELECT
    body->>'anonymousId' AS anonymous_id,
    body->'context'->'library'->>'name' AS context_library_name,
    (body->'context'->'library'->>'version') AS context_library_version,
    body->'context'->'page'->>'path' AS context_page_path,
    body->'context'->'page'->>'referrer' AS context_page_referrer,
    body->'context'->'page'->>'search' AS context_page_search,
    body->'context'->'page'->>'title' AS context_page_title,
    body->'context'->'page'->>'url' AS context_page_url,
    body->'context'->>'userAgent' AS context_userAgent,
    body->'context'->>'ip' AS context_ip,
    body->>'event' AS event,
    body->>'messageId' AS message_id,
    body->'properties'->>'title' AS properties_title,
    try_parse_monotonic_iso8601_timestamp(body->>'receivedAt') AS received_at,
    try_parse_monotonic_iso8601_timestamp(body->>'sentAt') AS sent_at,
    try_parse_monotonic_iso8601_timestamp(body->>'timestamp') AS timestamp,
    body->>'type' AS type,
    body->>'userId' AS user_id,
    try_parse_monotonic_iso8601_timestamp(body->>'originalTimestamp') AS original_timestamp
FROM segment_source;
```

</div>

</div>

<div id="tab-identity" class="tab-pane" title="Identity">

<div class="highlight">

``` chroma
CREATE VIEW parse_segment AS SELECT
    body->>'anonymousId' AS anonymous_id,
    body->>'channel' AS channel,
    body->'context'->>'ip' AS context_ip,
    body->'context'->>'userAgent' AS context_user_agent,
    (body->'integrations'->>'All')::bool AS integrations_all,
    (body->'integrations'->>'Mixpanel')::bool AS integrations_mixpanel,
    (body->'integrations'->>'Salesforce')::bool AS integrations_salesforce,
    body->>'messageId' AS messageId,
    try_parse_monotonic_iso8601_timestamp(body->>'receivedAt') AS received_at,
    try_parse_monotonic_iso8601_timestamp(body->>'sentAt') AS sent_at,
    try_parse_monotonic_iso8601_timestamp(body->>'timestamp') AS timestamp,
    body->'traits'->>'name' AS traits_name,
    body->'traits'->>'email' AS traits_email,
    body->'traits'->>'plan' AS traits_plan,
    (body->'traits'->>'logins')::numeric AS traits_logins,
    body->'traits'->'address'->>'street' AS traits_address_street,
    body->'traits'->'address'->>'city' AS traits_address_city,
    body->'traits'->'address'->>'state' AS traits_address_state,
    (body->'traits'->'address'->>'postalCode') AS traits_address_postalCode,
    body->'traits'->'address'->>'country' AS traits_address_country,
    body->>'type' AS type,
    body->>'userId' AS user_id,
    body->>'version' AS version
FROM segment_source;
```

</div>

</div>

</div>

</div>

Manually parsing JSON-formatted data in SQL can be tedious. 🫠 You can
use the widget below to **automatically** turn a sample JSON payload
into a parsing view with the individual fields mapped to columns.

<div class="json_widget">

<div class="json">

<div id="error_span" class="error">

</div>

</div>

<span class="input_container"> <span class="input_container-text">
</span> </span>

Target object type View Materialized view

``` sql_output
```

</div>

### Timestamp handling

We highly recommend using the
[`try_parse_monotonic_iso8601_timestamp`](/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/#temporal-filter-pushdown)
function when casting from `text` to `timestamp`, which enables
[temporal filter
pushdown](/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Deduplication

With the vast amount of data processed and potential network issues,
it’s not uncommon to receive duplicate records. You can use the
`DISTINCT ON` clause to efficiently remove duplicates. For more details,
refer to the webhook source [reference
documentation](/docs/self-managed/v25.2/sql/create-source/webhook/#handling-duplicated-and-partial-events).

## Next steps

With Materialize ingesting your Segment data, you can start exploring
it, computing real-time results that stay up-to-date as new data
arrives, and serving results efficiently. For more details, check out
the [Segment
documentation](https://segment.com/docs/connections/destinations/catalog/actions-webhook/)
and the [webhook source reference
documentation](/docs/self-managed/v25.2/sql/create-source/webhook/).

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/webhooks/segment.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
