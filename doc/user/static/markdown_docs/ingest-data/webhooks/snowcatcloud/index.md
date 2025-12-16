<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Ingest
data](/docs/self-managed/v25.2/ingest-data/)

</div>

# SnowcatCloud

This guide walks through the steps to ingest data from
[SnowcatCloud](https://www.snowcatcloud.com/) into Materialize using the
[Webhook source](/docs/self-managed/v25.2/sql/create-source/webhook/).

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

## Before you begin

Ensure that you have:

- A [SnowcatCloud account](https://app.snowcatcloud.com/register)
- A Snowplow or Analytics.js compatible pipeline set up and running.

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

To validate requests between SnowcatCloud and Materialize, you must
create a [secret](/docs/self-managed/v25.2/sql/create-secret/):

<div class="highlight">

``` chroma
CREATE SECRET snowcat_webhook_secret AS '<secret_value>';
```

</div>

Change the `<secret_value>` to a unique value that only you know and
store it in a secure location.

## Step 3. Set up a webhook source

Using the secret from the previous step, create a [webhook
source](/docs/self-managed/v25.2/sql/create-source/webhook/) in
Materialize to ingest data from SnowcatCloud. By default, the source
will be created in the active cluster; to use a different cluster, use
the `IN CLUSTER` clause.

<div class="highlight">

``` chroma
CREATE SOURCE snowcat_source IN CLUSTER webhooks_cluster
  FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS body,
        SECRET snowcat_webhook_secret AS validation_secret
      )
      -- The constant_time_eq validation function **does not support** fully
      -- qualified secret names. We recommend always aliasing the secret name
      -- for ease of use.
      constant_time_eq(headers->'authorization', validation_secret)
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

The above webhook source uses [basic
authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme).
This enables a simple and rudimentary way to grant authorization to your
webhook source.

## Step 4. Create a webhook destination in SnowcatCloud

To configure a Materialize webhook as a destination in SnowcatCloud,
follow the steps outlined below:

1.  **Select your SnowcatCloud pipe**

    Identify the pipeline you wish to add Materialize to as a
    destination.

2.  **Select Materialize as a destination**

    1.  Navigate to the destinations section.
    2.  Click **Configure** on the **Materialize** destination.

#### Connection settings

On the **Materialize Settings** page:

- **Webhook URL**: Define the endpoint where events will be dispatched
  by SnowcatCloud. Use the URL from **Step 3.**.

- **Secret**: Use the secret created in **Step 2.**.

- **Click Save & Test**: If the webhook is configured successfully, you
  will see a success message and the destination will start
  `PROVISIONING`; once it is `ACTIVE`, all your data will be streaming
  to Materialize’s webhook.

## Step 5. Validate incoming data

With the source set up in Materialize and the webhook destination
configured in SnowcatCloud, you can now query the incoming data:

1.  [In the Materialize console](/docs/self-managed/v25.2/console/),
    navigate to the **SQL Shell**.

2.  Use SQL queries to inspect and analyze the incoming data:

    <div class="highlight">

    ``` chroma
    SELECT * FROM segment_source LIMIT 10;
    ```

    </div>

    Note that while the destination is `PROVISIONING`, you will only see
    the test event.

## Step 6. Transform incoming data

Webhook data is ingested as a JSON blob. We recommend creating a parsing
view on top of your webhook source that uses [`jsonb`
operators](/docs/self-managed/v25.2/sql/types/jsonb/#operators) to map
the individual fields to columns with the required data types.

To see what columns are available for your pipeline (enrichments), refer
to the [SnowcatCloud documentation](https://docs.snowcatcloud.com/).

<div class="highlight">

``` chroma
CREATE VIEW events AS
SELECT
    body ->> 'app_id' AS app_id,
    body ->> 'platform' AS platform,
    try_parse_monotonic_iso8601_timestamp(body ->> 'etl_tstamp') AS etl_tstamp,
    try_parse_monotonic_iso8601_timestamp(body ->> 'collector_tstamp') AS collector_tstamp,
    try_parse_monotonic_iso8601_timestamp(body ->> 'dvce_created_tstamp') AS dvce_created_tstamp,
    body ->> 'event' AS event,
    body ->> 'event_id' AS event_id,
    body ->> 'txn_id' AS txn_id,
    body ->> 'name_tracker' AS name_tracker,
    body ->> 'v_tracker' AS v_tracker,
    body ->> 'v_collector' AS v_collector,
    body ->> 'v_etl' AS v_etl,
    body ->> 'user_id' AS user_id,
    body ->> 'user_ipaddress' AS user_ipaddress,
    body ->> 'user_fingerprint' AS user_fingerprint,
    body ->> 'domain_userid' AS domain_userid,
    body ->> 'domain_sessionidx' AS domain_sessionidx,
    body ->> 'network_userid' AS network_userid,
    (body -> 'contexts_com_dbip_location_1' -> 0 -> 'country' -> 'names' ->> 'en')::text AS geo_country,
    (body -> 'contexts_com_dbip_location_1' -> 0 -> 'subdivisions' -> 0 -> 'names' ->> 'en')::text AS geo_region,
    (body -> 'contexts_com_dbip_location_1' -> 0 -> 'city' -> 'names' ->> 'en')::text AS geo_city,
    (body -> 'contexts_com_dbip_location_1' -> 0 -> 'postal' ->> 'code')::text AS geo_zipcode,
    (body -> 'contexts_com_dbip_location_1' -> 0 -> 'location' ->> 'latitude')::numeric AS geo_latitude,
    (body -> 'contexts_com_dbip_location_1' -> 0 -> 'location' ->> 'longitude')::numeric AS geo_longitude,
    (body -> 'contexts_com_dbip_isp_1' -> 0 -> 'traits' ->> 'organization')::text AS ip_organization,
    (body -> 'contexts_com_dbip_isp_1' -> 0 -> 'traits' ->> 'isp')::text AS ip_isp,
    (body -> 'contexts_com_dbip_isp_1' -> 0 -> 'traits' ->> 'asn')::text AS ip_asn,
    (body -> 'contexts_com_dbip_isp_1' -> 0 -> 'traits' ->> 'connection_type')::text AS ip_connection_type,
    (body -> 'contexts_com_dbip_isp_1' -> 0 -> 'traits' ->> 'user_type')::text AS ip_user_type,
    body ->> 'page_url' AS page_url,
    body ->> 'page_title' AS page_title,
    body ->> 'page_referrer' AS page_referrer,
    body ->> 'page_urlscheme' AS page_urlscheme,
    body ->> 'page_urlhost' AS page_urlhost,
    body ->> 'page_urlport' AS page_urlport,
    body ->> 'page_urlpath' AS page_urlpath,
    body ->> 'page_urlquery' AS page_urlquery,
    body ->> 'page_urlfragment' AS page_urlfragment,
    body ->> 'refr_urlscheme' AS refr_urlscheme,
    body ->> 'refr_urlhost' AS refr_urlhost,
    body ->> 'refr_urlport' AS refr_urlport,
    body ->> 'refr_urlpath' AS refr_urlpath,
    body ->> 'refr_urlquery' AS refr_urlquery,
    body ->> 'refr_urlfragment' AS refr_urlfragment,
    body ->> 'refr_medium' AS refr_medium,
    body ->> 'refr_source' AS refr_source,
    body ->> 'refr_term' AS refr_term,
    body ->> 'mkt_medium' AS mkt_medium,
    body ->> 'mkt_source' AS mkt_source,
    body ->> 'mkt_term' AS mkt_term,
    body ->> 'mkt_content' AS mkt_content,
    body ->> 'mkt_campaign' AS mkt_campaign,
    body ->> 'se_category' AS se_category,
    body ->> 'se_action' AS se_action,
    body ->> 'se_label' AS se_label,
    body ->> 'se_property' AS se_property,
    (body ->> 'se_value')::numeric AS se_value,
    body ->> 'unstruct_event' AS unstruct_event,
    body ->> 'tr_orderid' AS tr_orderid,
    body ->> 'tr_affiliation' AS tr_affiliation,
    (body ->> 'tr_total')::numeric AS tr_total,
    (body ->> 'tr_tax')::numeric AS tr_tax,
    (body ->> 'tr_shipping')::numeric AS tr_shipping,
    body ->> 'tr_city' AS tr_city,
    body ->> 'tr_state' AS tr_state,
    body ->> 'tr_country' AS tr_country,
    body ->> 'ti_orderid' AS ti_orderid,
    body ->> 'ti_sku' AS ti_sku,
    body ->> 'ti_name' AS ti_name,
    body ->> 'ti_category' AS ti_category,
    (body ->> 'ti_price')::numeric AS ti_price,
    body ->> 'ti_quantity' AS ti_quantity,
    body ->> 'pp_xoffset_min' AS pp_xoffset_min,
    body ->> 'pp_xoffset_max' AS pp_xoffset_max,
    body ->> 'pp_yoffset_min' AS pp_yoffset_min,
    body ->> 'pp_yoffset_max' AS pp_yoffset_max,
    body ->> 'useragent' AS useragent,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'agentNameVersion')::text AS br_name,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'agentName')::text AS br_family,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'agentVersion')::text AS br_version,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'layoutEngineClass')::text AS br_type,
    body ->> 'br_lang' AS br_lang,
    body ->> 'br_features_pdf' AS br_features_pdf,
    body ->> 'br_features_flash' AS br_features_flash,
    body ->> 'br_features_java' AS br_features_java,
    body ->> 'br_features_director' AS br_features_director,
    body ->> 'br_features_quicktime' AS br_features_quicktime,
    body ->> 'br_features_realplayer' AS br_features_realplayer,
    body ->> 'br_features_windowsmedia' AS br_features_windowsmedia,
    body ->> 'br_features_gears' AS br_features_gears,
    body ->> 'br_features_silverlight' AS br_features_silverlight,
    body ->> 'br_cookies' AS br_cookies,
    body ->> 'br_colordepth' AS br_colordepth,
    body ->> 'br_viewwidth' AS br_viewwidth,
    body ->> 'br_viewheight' AS br_viewheight,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'operatingSystemName')::text AS os_name,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'operatingSystemClass')::text AS os_family,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'deviceBrand')::text AS os_manufacturer,
    body ->> 'os_timezone' AS os_timezone,
    (body -> 'contexts_nl_basjes_yauaa_context_1' -> 0 ->> 'deviceClass')::text AS dvce_type,
    (body ->> 'dvce_screenwidth')::numeric AS dvce_screenwidth,
    (body ->> 'dvce_screenheight')::numeric AS dvce_screenheight,
    body ->> 'doc_charset' AS doc_charset,
    (body ->> 'doc_width')::numeric AS doc_width,
    (body ->> 'doc_height')::numeric AS doc_height,
    body ->> 'tr_currency' AS tr_currency,
    (body ->> 'tr_total_base')::numeric AS tr_total_base,
    (body ->> 'tr_tax_base')::numeric AS tr_tax_base,
    (body ->> 'tr_shipping_base')::numeric AS tr_shipping_base,
    body ->> 'ti_currency' AS ti_currency,
    (body ->> 'ti_price_base')::numeric AS ti_price_base,
    body ->> 'base_currency' AS base_currency,
    body ->> 'geo_timezone' AS geo_timezone,
    body ->> 'mkt_clickid' AS mkt_clickid,
    body ->> 'mkt_network' AS mkt_network,
    body ->> 'etl_tags' AS etl_tags,
    try_parse_monotonic_iso8601_timestamp(body ->> 'dvce_sent_tstamp') AS dvce_sent_tstamp,
    body ->> 'refr_domain_userid' AS refr_domain_userid,
    try_parse_monotonic_iso8601_timestamp(body ->> 'refr_dvce_tstamp') AS refr_dvce_tstamp,
    body ->> 'domain_sessionid' AS domain_sessionid,
    try_parse_monotonic_iso8601_timestamp(body ->> 'derived_tstamp') AS derived_tstamp,
    body ->> 'event_vendor' AS event_vendor,
    body ->> 'event_name' AS event_name,
    body ->> 'event_format' AS event_format,
    body ->> 'event_version' AS event_version,
    body ->> 'event_fingerprint' AS event_fingerprint
FROM
    snowcat_source;
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

With Materialize ingesting your SnowcatCloud data, you can start
exploring it, computing real-time results that stay up-to-date as new
data arrives, and serving results efficiently. For more details, check
out the [SnowcatCloud documentation](https://docs.snowcatcloud.com/) and
the [webhook source reference
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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/webhooks/snowcatcloud.md"
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
