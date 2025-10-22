---
title: "SnowcatCloud"
description: "How to stream data from SnowcatCloud to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "SnowcatCloud"
aliases:
  - /ingest-data/snowcatcloud/
---

This guide walks through the steps to ingest data from [SnowcatCloud](https://www.snowcatcloud.com/)
into Materialize using the [Webhook source](/sql/create-source-v1/webhook/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

Ensure that you have:

- A [SnowcatCloud account](https://app.snowcatcloud.com/register)
- A Snowplow or Analytics.js compatible pipeline set up and running.

## Step 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your webhook
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

To create a cluster in Materialize, use the [`CREATE CLUSTER` command](/sql/create-cluster):

```mzsql
CREATE CLUSTER webhooks_cluster (SIZE = '25cc');

SET CLUSTER = webhooks_cluster;
```

## Step 2. Create a secret

To validate requests between SnowcatCloud and Materialize, you must create a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET snowcat_webhook_secret AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in a secure location.

## Step 3. Set up a webhook source

Using the secret from the previous step, create a [webhook source](/sql/create-source-v1/webhook/)
in Materialize to ingest data from SnowcatCloud. By default, the source will be
created in the active cluster; to use a different cluster, use the `IN
CLUSTER` clause.

```mzsql
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

After a successful run, the command returns a `NOTICE` message containing the
unique [webhook URL](/sql/create-source-v1/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You will need
it for the next step.

The URL will have the following format:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

If you missed the notice, you can find the URLs for all webhook sources in the
[`mz_internal.mz_webhook_sources`](/sql/system-catalog/mz_internal/#mz_webhook_sources)
system table.

### Access and authentication

{{< warning >}}
Without a `CHECK` statement, **all requests will be accepted**. To prevent bad
actors from injecting data into your source, it is **strongly encouraged** that
you define a `CHECK` statement with your webhook sources.
{{< /warning >}}

The above webhook source uses [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme).
This enables a simple and rudimentary way to grant authorization to your webhook source.

## Step 4. Create a webhook destination in SnowcatCloud

To configure a Materialize webhook as a destination in SnowcatCloud, follow the
steps outlined below:

1.  **Select your SnowcatCloud pipe**

    Identify the pipeline you wish to add Materialize to as a destination.

1.  **Select Materialize as a destination**
    1. Navigate to the destinations section.
    1. Click **Configure** on the **Materialize** destination.

#### Connection settings

On the **Materialize Settings** page:

- **Webhook URL**: Define the endpoint where events will be dispatched by
    SnowcatCloud. Use the URL from **Step 3.**.

- **Secret**: Use the secret created in **Step 2.**.

- **Click Save & Test**: If the webhook is configured successfully, you will see
    a success message and the destination will start `PROVISIONING`; once it is
    `ACTIVE`, all your data will be streaming to Materialize's webhook.

## Step 5. Validate incoming data

With the source set up in Materialize and the webhook destination configured in
SnowcatCloud, you can now query the incoming data:

1. [In the Materialize console](/console/), navigate to
   the **SQL Shell**.

1. Use SQL queries to inspect and analyze the incoming data:

    ```mzsql
    SELECT * FROM segment_source LIMIT 10;
    ```

    Note that while the destination is `PROVISIONING`, you will only see the
    test event.

## Step 6. Transform incoming data

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [`jsonb` operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types.

To see what columns are available for your pipeline (enrichments), refer to
the [SnowcatCloud documentation](https://docs.snowcatcloud.com/).

```mzsql
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

### Timestamp handling

We highly recommend using the [`try_parse_monotonic_iso8601_timestamp`](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown)
function when casting from `text` to `timestamp`, which enables [temporal filter
pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Deduplication

With the vast amount of data processed and potential network issues, it's not
uncommon to receive duplicate records. You can use the `DISTINCT ON` clause to
efficiently remove duplicates. For more details, refer to the webhook source
[reference documentation](/sql/create-source-v1/webhook/#handling-duplicated-and-partial-events).

## Next steps

With Materialize ingesting your SnowcatCloud data, you can start exploring it,
computing real-time results that stay up-to-date as new data arrives, and
serving results efficiently. For more details, check out the
[SnowcatCloud documentation](https://docs.snowcatcloud.com/) and the
[webhook source reference documentation](/sql/create-source-v1/webhook/).
