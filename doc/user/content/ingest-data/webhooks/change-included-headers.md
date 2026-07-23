---
title: "Change a webhook source's included headers"
description: "Use ALTER SCHEMA ... SWAP WITH to change which headers a webhook source exposes, without changing the endpoint URL your senders use."
menu:
  main:
    parent: "webhooks"
    name: "Changing included headers"
    weight: 2
---

A webhook source fixes its column shape when you create it, including which
request headers it exposes through [`INCLUDE HEADER` /
`INCLUDE HEADERS`](/sql/create-source/webhook/#exposing-headers). You cannot alter
the header configuration of an existing webhook source in place. To change it,
create a new source with the header configuration you want in a separate schema,
then swap it into place with [`ALTER SCHEMA ...
SWAP WITH`](/sql/alter-schema/#swap-with). Because the swap only renames schemas,
the [webhook endpoint URL](/sql/create-source/webhook/#webhook-url) that your
senders post to stays the same.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

{{< warning >}}

Unlike a Kafka or database source, a webhook source has no upstream system to
re-read. The replacement source starts **empty**: it only contains webhook calls
received **after** the swap. Events delivered to the original source are **not**
copied to the replacement, and are lost for good once you drop the original
source. Only perform the swap when losing the original source's history is
acceptable, or keep the original source around to continue querying its past
data.

{{< /warning >}}

## How it works

The webhook endpoint URL is derived from the source's fully qualified name:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

Because the schema is part of the URL, building the replacement in a different
schema and then swapping the two schemas atomically leaves the original URL
resolving to the new source. Senders keep posting to the same URL, and downstream
objects keep referencing the same name.

## Before you begin

You need a Materialize account and a cluster to host the source. The examples
below use the `quickstart` cluster and omit the `CHECK` clause for brevity. In
production, always [validate requests](/sql/create-source/webhook/#validating-requests)
with a `CHECK` clause.

## Step 1. Create the initial source

Create the original source in a `prod` schema. This version exposes a single
`foo` header as its own column:

```mzsql
CREATE SCHEMA prod;

CREATE SOURCE prod.events
  IN CLUSTER quickstart
  FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADER 'foo' AS foo;
```

The source has a `foo` column alongside the `body`:

Column | Type    | Nullable?
-------|---------|-----------
 body  | `jsonb` | No
 foo   | `text`  | Yes

Point your webhook sender at
`https://<HOST>/api/webhook/materialize/prod/events`. Incoming events accumulate
in `prod.events`.

## Step 2. Create the replacement with the new header configuration

Suppose you now want to expose **all** headers except a sensitive `bar` header,
rather than only `foo`. Build the replacement in a separate `staging` schema
using the `INCLUDE HEADERS ( NOT ... )` syntax:

```mzsql
CREATE SCHEMA staging;

CREATE SOURCE staging.events
  IN CLUSTER quickstart
  FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADERS ( NOT 'bar' );
```

Instead of a single `foo` column, this version has a `headers` map column that
contains every header except `bar`:

Column  | Type                | Nullable?
--------|---------------------|-----------
 body   | `jsonb`             | No
 headers | `map[text => text]` | No

At this point nothing is posting to `staging.events` yet, so it is empty. Your
senders are still delivering to `prod.events`.

## Step 3. Swap the schemas

Swap the two schemas to cut over. The swap is atomic:

```mzsql
ALTER SCHEMA prod SWAP WITH staging;
```

After the swap:

- `prod.events` now resolves to the new source (the `headers` map version), so
  the unchanged endpoint URL `.../materialize/prod/events` and any downstream
  objects that reference `prod.events` immediately pick up the new shape.
- The original source now lives at `staging.events`, retaining the data it
  ingested before the swap.

New webhook calls to `prod.events` land in the new source and expose the
`headers` map. As noted in the warning above, the events that arrived before the
swap remain only in `staging.events` (the old source) and are **not** present in
the new `prod.events`.

## Step 4. Clean up

Once you have validated the cutover, drop the retired source. This permanently
discards the data it ingested before the swap:

```mzsql
DROP SCHEMA staging CASCADE;
```

If you need to preserve the original source's historical events, copy them into a
table or downstream object **before** dropping it, or leave the `staging` schema
in place and query it as needed.

## See also

- [`CREATE SOURCE: Webhook`](/sql/create-source/webhook/)
- [`ALTER SCHEMA ... SWAP WITH`](/sql/alter-schema/#swap-with)
- [Webhooks quickstart](/ingest-data/webhooks/webhook-quickstart/)
