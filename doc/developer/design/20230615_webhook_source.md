# Webhook Source

### Associated
 * [Epic] Webhook Source - [materialize#19951](https://github.com/MaterializeInc/materialize/issues/19951)
 * Support and HTTP Post Source - [materialize#1701](https://github.com/MaterializeInc/materialize/issues/1701#issuecomment-1537908820)

## Context

We want to make it easier for folks to get data into Materialize. Currently the only way is via one
of our [supported sources](https://materialize.com/docs/sql/create-source/), which depending on
the environment, can be pretty hard to setup. A common method of publishing data from an
application is via [webhooks](https://www.redhat.com/en/topics/automation/what-is-a-webhook). When
some event occurrs, e.g. a new analytics event from Segment, or a new Pull Request on a GitHub
repository, an HTTP endpoint you define will get called so you can receive the event in near
real-time.

Supporting webhooks in Materialize allows users to easily get their data into MZ, and the low
latency of the events works very well with the premise of being an "operational" data warehouse.

> **_Note_** To achieve the goal of "make it easier to get data in Materialize" we also considered
building support for Fivetran. We may still do this in the future, but the amount of work to get
webhooks working is significantly less, and the frequency of data is more suited to the goals of
Materialize.

## Goals

1. Provide URLs that users can push data to. In Materialize this data is represented as a source
with two columns, `headers` and `body`.
2. Support an extensible syntax similar to `CREATE SOURCE <name> FROM HTTP REQUESTS` that allows
users to generate the aforementioned URLs.
3. Handle at least a total of 100 requests per-second across all `HTTP REQUESTS` sources.
5. Code complete by August 15th, 2023.


## Non-Goals

The following are all features we want to support, but are out of scope in the immediate term.

* Extracting keys from the data for `UPSERT`-like behavior.
* Validation that the request comes from a trusted source.
* Custom filtering of headers.
* Manipulating the incoming data, splitting a single request into any kind of "sub-sources".

## Overview

Our implementation will mostly exist in the adapter, and we can re-use existing APIs on the
`storage-client` to handle the persistence of data.

We'll update the SQL parser to support our new kind of `HTTP REQUESTS` source, and add
a new endpoint to `environmentd`'s HTTP server `/api/webhook/:id`. When a user creates this new
kind of source, we'll generate an id, persist the new `Source` in the catalog, use the
`storage-client` to create a new collection, and finally return a well formatted URL to the user.

When we receive a event we'll use the created `SessionClient` to send the raw `headers` and `body`
to the coordinator, pack the data into a row, and append it to the necessary collection using a
new monotonic append API on the `StorageController`.

## Detailed description

We'll update the parser to support creating a new kind of Source, `HTTP REQUESTS`, for which you can
specify a `FORMAT`, which denotes how we decode the body of a request, the only supported format
for the time being is `BYTES`.

```
CREATE SOURCE <name> FROM HTTP REQUESTS FORMAT BYTES;
```

After executing this request, we'll create a source with the following columns:

| name    | type              |
|---------|-------------------|
| headers | map[text -> text] |
| body    | bytea             |

Using `bytea` might not be optimal from a storage perspective (e.g. no chance for MFP Pushdown) but
it makes this initial attempt at webhooks maximally compatible. On the completion of the statement
we'll return an ID to the user, which they would specify in the webhook URL. The returned ID would
be 6 character base62 encoded string, a pattern commonly used in URL shorteners.

> **_Note_**: An alternative to `bytea` encoded bodies would be to use `jsonb`. Looking through a
few common apps that support webhooks, the payloads all default to JSON, or have JSON as the only
option. Using `bytea` makes us compatible with all webhooks, with the tradeoff of adding work for
the user to cast to their desired serialization format downstream.

We'll add a new endpoint to the existing [`base_router`](https://github.com/MaterializeInc/materialize/blob/6e1f4c7352427301d782438d614feafb0f644442/src/environmentd/src/http.rs#L747)
in `environmentd` with the path: `/api/webhook/:id`. This follows the existing pattern of our other
two endpoints, whose paths also start with `/api`.

> **_Note_**: The existing Web Sockets API is at `/api/experimental/ws`, we could move the webhooks
API under the path `/api/experimental/webhook`, if we think there will be significant breaking
changes in the future.

We'll also add a new API `SessionClient::append_webhook` which is very similar to the existing
[`insert_rows`](https://github.com/MaterializeInc/materialize/blob/6e1f4c7352427301d782438d614feafb0f644442/src/adapter/src/client.rs#L392-L397).
This new API will send the captured `:id` from the URL path, the headers and body of the request,
straight to the coordinator, via a new `Command`. In the coordinator we'll do the following:

1. Spawn a new `tokio::task` to do the work off the main thread.
2. Map the `id` to a previously created Source.
3. Remove sensitive header values from the received headers (e.g. [Authorization](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization)).
4. Decode the body of the request according to the Source's `FORMAT`.
5. Pack the filtered headers and decoded body into a `Row`.
6. Commit that `Row` with a new `StorageController::monotonic_append` API.
7. Respond once the `StorageController` reports the data as committed.

Today we expose a method [`append`](https://github.com/MaterializeInc/materialize/blob/149d2e5bca1f54e94acfe02ab5f5751b9cb2adfc/src/storage-client/src/controller.rs#L403-L406)
on the `StorageController` trait, I would like to add a new method:

```
pub async fn monotonic_append(&self, id: GlobalId, updates: Vec<(Row, Diff)>) -> Result<(), StorageError>;
```

This new method would call the already existing [`append_to_managed_collection`](https://github.com/MaterializeInc/materialize/blob/149d2e5bca1f54e94acfe02ab5f5751b9cb2adfc/src/storage-client/src/controller.rs#L2718)
and `.await`-ing the function would complete once the data has been successfully committed. I
believe we should add this new API instead of using the already existing `append` API, because
`append` requires the coordinator to pick a timestamp, meanwhile `monotonic_append` would have
storage pick the timestamp. Having storage select the timestamp aligns better with our goal of
having a storage owned "push source" in the future.

### Request Limits
[request_limits]: #request_limits

To start we'll aim to support at least 100 requests per second, in other words, 10 sources could
receive 10 requests per second. We can probably scale further, but this seems like a sensible limit
to start at. Also we'll add an [`axum::middleware`](https://docs.rs/axum/latest/axum/middleware/index.html)
that enforces rate limiting on our new endpoint. This should be a pretty easy way to prevent a
misbehaving source from taking down all of `environmentd`.

### Metrics
[metrics]: #metrics

There are three metrics we should track, to measure success and performance of this new source:

1. Number of `HTTP REQUESTS` sources created.
2. Number of requests made to the `/api/webhook` endpoint.
3. Response time of the `/api/webhook` endpoint.

A second way to measure success, and get feedback on this new feature, is to use it internally.
To start, we plan to dogfood the new source with the following applications:

1. Segment
2. GitHub
3. Slack
4. Buildkite

This internal usage will help us quickly determine if we built the right thing, and how to
prioritize further improvements.

## Alternatives

### Fivetran

For the idea of "making it easier to get data into Materialize", an alternative approach we
considered was building Fivetran support. While this is feasible, and something we might pursue in
the future, we're not pursuing this path for two reasons:

1. It requires more work to build.
2. The at best 5 minute sync time of Fivetran, does not align well with being an "operational" data
warehouse.

### `environmentd` vs `clusterd`

In the proposed design, `environmentd` handles all of the incoming requests, which is not good for
scalability. An alternative is to have a `clusterd` handle requests for a given Source, like every
other Source does today. The reason we chose `environmentd` to primarily handle webhook requests is
because it is already setup to handle _incoming_ requests from an external network. All of our
other sources are pull based, where we send a request and ask for data. Whereas webhooks are push
based, we receive a request, when there is new data available. Exposing a `clusterd` outside of our
network, maybe isn't even something we want to do (e.g. in the future maybe there is a reverse
proxy that talks to `clusterd`), and would require significant lift from the Cloud Team.

### Tables vs Sources vs What we Chose

An alternative approach is to use a `TABLE` for these new sources, and "translate" any incoming
requests to `INSERT` statements. There are two reasons we did not chose this approach:

1. `INSERT`s into a table are linearizable, whereas requests from a webhook don't need to be. By
using tables we would be imposing an unnecessary performance bottleneck.
2. In the future to support features like validation or custom mapping of the request body, we'll
need a storage owned Source so we can run this logic. The migration path from a `TABLE` to this new
kind of Source isn't entirely clear.

The second alternative is to build this new kind of storage owned Source, which gets installed on a
cluster and can do more complex operations everytime a request is received. We plan to build
something like this in the future, but at the moment this would require a large amount of work,
and we first we want to validate that webhooks are a feature users actually want.

## Future

There is plenty of future work that can be done for Webhooks, but by limiting our scope and
shipping quickly, we can collect feedback from users to help inform our priorities.

1. A `clusterd` primarily handles requests. This would improve our ability to scale, e.g. 100s of
QPS per source, and allow us to build the more complex features listed below.
2. `TRANSFORM USING` a given request. Being able to map a JSON (or Avro, protobuf, etc.) body to
a relation at the time of receiving a request, would be a win for the user as it's more ergonomic,
and a win for us to reduce the amount of storage used.
3. Validate requests at the time of receiving them. Certain applications provide a signature in
the header of each request. Being able to validate and optionally reject invalid requests, could
improve user confidence in their data.
4. Subsources and batching. Being able map a single event to multiple independent sources, could
be a big win for the ergonomics of the feature.


As a _rough sketch_, the north star for this feature in terms of SQL syntax could be:

```
CREATE SOURCE <name> FROM HTTP REQUESTS
  -- parse the body as JSON.
  FORMAT JSON
  -- reject the request if this expression returns False.
  VALIDATE USING SELECT headers->>'signature' = sha256hash(body),
  -- transform the body and headers into a well formed relation, before persisting it.
  AS (
    SELECT
        body->>'id' as id,
        body->>'name' as name,
        body->>'event_type' as event_type,
        body->>'country' as country,
        headers->>'X-Application' as application
  )
  -- create a subsource that contains specific info.
  SUBSOURCE <name> (
    SELECT
      body->>'id' as id,
      body->>'ip_addr' as id_address
  )
```

## Open questions

None at the moment!
