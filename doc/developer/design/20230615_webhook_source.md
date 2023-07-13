# Webhook Source

### Associated
 * [Epic] Webhook Source - [materialize#19951](https://github.com/MaterializeInc/materialize/issues/19951)
 * Support an HTTP Post Source - [materialize#1701](https://github.com/MaterializeInc/materialize/issues/1701#issuecomment-1537908820)

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

1. Provide URLs that users can push data to.
2. Support an extensible syntax similar to `CREATE SOURCE <name> FROM WEBHOOK` that allows
users to generate the aforementioned URLs.
3. Handle at least a total of 100 requests per-second across all `WEBHOOK` sources.
5. Code complete by August 15th, 2023.


## Non-Goals

The following are all features we want to support, but are out of scope in the immediate term.

* Extracting keys from the data for `UPSERT`-like behavior.
* Custom filtering of headers.
* Manipulating the incoming data, splitting a single request into any kind of "sub-sources".
* Handling batch requests.

## Overview

Our implementation will mostly exist in the adapter, and we can re-use existing APIs on the
`storage-client` to handle the persistence of data.

We'll update the SQL parser to support our new kind of `WEBHOOK` source, and add a new endpoint to
`environmentd`'s HTTP server `/api/webhook/:database/:schema/:name`. When a user creates this new
kind of source, we'll persist the new `Source` in the catalog, use the `storage-client` to create a
new collection.

When we receive a event we'll use the `mz_adapter::Client` that the HTTP server has to send the raw
`headers` and `body` to the coordinator, pack the data into a row, and append it to the necessary
collection using a new monotonic append API on the `StorageController`.

## Detailed description

We'll update the parser to support creating a new kind of Source, `WEBHOOK`, for which you can
specify a `BODY FORMAT`, this denotes how we decode the body of a request. To start we'll support
three formats, `BYTES`, `JSON`, or `TEXT`. By default we will not include the headers of the
request, they can optionally be added by specifying `INCLUDE HEADERS`.

```
CREATE SOURCE <name> FROM WEBHOOK
  BODY FORMAT [BYTES | JSON | TEXT]
  (INCLUDE HEADERS)?
;
```

After executing this request, we'll create a source with the following columns:

| name    | type                        | optional?                                      |
|---------|-----------------------------|------------------------------------------------|
| body    | `bytea`, `jsonb`, or `text` | No                                             |
| headers | `map[text -> text]`         | Yes, present if `INCLUDE HEADERS` is specified |

We'll add a new endpoint to the existing [`base_router`](https://github.com/MaterializeInc/materialize/blob/6e1f4c7352427301d782438d614feafb0f644442/src/environmentd/src/http.rs#L747)
in `environmentd` with the path: `/api/webhook/:database/:schema/:name`. This follows the existing
pattern of our other two endpoints, whose paths also start with `/api`. Users can then send events
to their source, using its fully qualified name.

> **_Note_**: The existing Web Sockets API is at `/api/experimental/ws`, we could move the webhooks
API under the path `/api/experimental/webhook`, if we think there will be significant breaking
changes in the future.

We'll also add a new API `mz_adapter::Client::append_webhook` which is very similar to the existing
[`insert_rows`](https://github.com/MaterializeInc/materialize/blob/6e1f4c7352427301d782438d614feafb0f644442/src/adapter/src/client.rs#L392-L397)
on the `SessionClient`. This new API will send the following to the `Coordinator` via a new `Command`:

1. The captured `:database`, `:schema`, and `:name` from the URL path.
2. The body and optionally headers of the request.
3. A `oneshot::Sender` that can be used to send a response.

Note, the reason we'll use the `mz_adapter::Client` and not the `SessionClient`, is because to push
data to a source we don't need a `Session`. Creating and tearing down a `Session` adds non-trivial
overhead and also possibly results in unneccessary writes to CockroachDB.

In the coordinator we'll do the following:

1. Map the `database`, `schema`, and `name` to a webhook source using the `Catalog`. Erroring if no
   object can be found or the object is not a webhook source.
2. Get the sending side of a channel that can be used to send new data to the `StorageController`.
   This would be using the new `StorageController::monotonic_appender(...)` detailed below.
3. Spawn a new `tokio::task`, do all further work in this task and off the `Coordinator` thread.
4. Decode the body of our request according to `BODY FORMAT`.
5. Pack our data into a `Row`.
6. Send a tuple of `(Row, 1)` to the `StorageController`, wait for it the data to committed.
7. Use the provided `oneshot::Sender` to respond with success or failure.

Today we expose a method [`append`](https://github.com/MaterializeInc/materialize/blob/149d2e5bca1f54e94acfe02ab5f5751b9cb2adfc/src/storage-client/src/controller.rs#L403-L406)
on the `StorageController` trait, I would like to add a new method:

```
pub async fn monotonic_appender(&self, id: GlobalId) -> CollectionAppender;
```

This new method would return a struct called `CollectionAppender`, which essentially acts as a
`oneshot::Sender<Vec<(Row, Diff)>>`. It's a `Send`-able and non-`Clone`-able struct that has one
method `async fn append(self, ...)` where it takes ownership of `self` so it can only be used once.
This would be implemented as part of [`collection_mgmt`](https://github.com/MaterializeInc/materialize/blob/main/src/storage-client/src/controller/collection_mgmt.rs)
and would approximately be structed like:

```
struct CollectionAppender {
  id: GlobalId,
  tx: mpsc::Sender<(GlobalId, Vec<(Row, Diff)>)>,
}
```

In addition to the `CollectionAppender`, we'll update the `CollectionManager` to receive a
`oneshot::Sender<Result<(), StorageError>>` that it can use to optionally notify any listeners when
and if the updates were successfully committed.

Adding the `CollectionAppender` introduces a possible correctness issue though:

1. Create a `CollectionAppender` for `GlobalId(A)`.
2. Drop source associated with `GlobalId(A)`.
3. Try to append data.

To protect against this we can change the `CollectionManager` to return an error via the
`oneshot::Sender` if the [provided GlobalId no longer exists](https://github.com/MaterializeInc/materialize/blob/main/src/storage-client/src/controller/collection_mgmt.rs#L83).

Adding this new API gets us two important properties that the existing `append` API does not have:

1. The ability to send updates to the `StorageController` from a thread other than the main
   `Coordinator` thread.
2. Allows the `StorageController` to pick the timestamp for the update, instead of the
   `Coordinator`, which aligns with our long term goals if having a storage owned "push source" in
   the future.

### Request Validation
[request_validation]: #request_validation

Every `WEBHOOK` source will be open to the entire internet, as such we need to include some sort of
validation so we can verify that requests are legitimate. In general webhooks are validated by
HMAC-ing the request body using SHA256, and validating the result with a signature provided in the
request headers. An issue though is everyone does this just a little bit different:

* Segment signature = HMAC of just the body of the request
* Buildkite signature = HMAC of "#{timestamp}.#{body}"
* GitHub signature = "body=" + HMAC of {body}

As such we'll need to support some custom logic for validation. What we can do is support a
`VALIDATE USING` statement that accepts only a single scalar expression. This expression will
be provided the request `headers` as `map[text] -> text`, and the `body` as `text` _regardless of
whether or not the source has `INCLUDE HEADERS` or what `FORMAT BODY` is specified._ For example:

```
VALIDATE USING (
  headers['X-Signature'] = hmac('sha256', SECRET webhook_secret, body)
)
```

Internally the provided expression will be turned into a `MirScalarExpr` which will be used to
evaluate each request. This evaluation will happen off the main coordinator thread.

### Request Limits
[request_limits]: #request_limits

To start we'll aim to support at least 100 requests per second, in other words, 10 sources could
receive 10 requests per second. We can probably scale further, but this seems like a sensible limit
to start at. Also we'll add an [`axum::middleware`](https://docs.rs/axum/latest/axum/middleware/index.html)
that enforces rate limiting on our new endpoint. This should be a pretty easy way to prevent a
misbehaving source from taking down all of `environmentd`.

We'll also introduce a maximum size the body of each request can be, the default limit will be
16KiB.

Both the maximum number of requests per second, and max body size, will be configurable via
LaunchDarkly, and later we can introduce SQL syntax so the user can `ALTER` these parameters on a
per-source basis.

> **_Note_**: Some applications provide batching of webhook events. To reduce scope we are not
going to support this, our current thought is handling batched requests should come naturally with
support for `TRANSFORM USING`.

### Metrics
[metrics]: #metrics

There are three metrics we should track, to measure success and performance of this new source:

1. Number of `WEBHOOK` sources created.
    *  This will be collected via the Prometheus SQL exporter running a query like:
       `SELECT count(*) FROM mz_sources WHERE type = 'webhook';`.
2. Number of requests made to the `/api/webhook` endpoint.
    * Subdivided by number of successes and failures.
    * This will be a new Prometheus counter called `mz_webhook_source_requests`.
3. Response time of the `/api/webhook` endpoint.
    * This will be a new Prometheus histogram called `mz_webhook_source_request_duration_ms`.

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
proxy that talks to `clusterd`), and would require significant lift from the Cloud Team.  We also
have a separate project in the works that will allow `environmentd` to scale horizontally, which
will mitigate these concerns.

### Tables vs Sources vs What we Chose

An alternative approach is to use a `TABLE` for these new sources, and "translate" any incoming
requests to `INSERT` statements. There are two reasons we did not chose this approach:

1. In the future to support features like validation or custom mapping of the request body, we'll
need a storage owned Source so we can run this logic. The migration path from a `TABLE` to this new
kind of Source isn't entirely clear.
2. `INSERT`s into a table are linearizable, whereas requests from a webhook should not be. We can
update `TABLE`s to make `INSERT`s optionally linearizable, but this work probably isn't worth it
given we also have reason 1.

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
3. Subsources and batching. Being able map a single event to multiple independent sources, could
be a big win for the ergonomics of the feature.
4. User configuration of request limits via `ALTER <source>`, i.e. rate and size.
5. Consider using some "external ID" that users can use to reference Webhook sources, e.g.
   `/api/webhook/abcdefg`. This would allow users to drop and re-create sources without needing to
   update external systems.
   See <https://github.com/MaterializeInc/materialize/pull/20002#discussion_r1234726035> for more
   details.
6. Supporting "recipes" for request validation for common webhook applications. For example,
   `VALIDATE STRIPE (SECRET ...)` would handle HMAC-ing the body of the request, and matching it
   against the exact header that Stripe expects.


As a _rough sketch_, the north star for this feature in terms of SQL syntax could be:

```
CREATE SOURCE <name> FROM WEBHOOK
  -- parse the body as JSON.
  BODY FORMAT JSON,
  INCLUDE HEADERS,
  -- reject the request if this expression returns False.
  VALIDATE USING (
    SELECT headers->>'signature' = sha256hash(body)
  ),
  -- transform the body and headers into a well formed relation, before persisting it.
  AS (
    SELECT
        body->>'id' as id,
        body->>'name' as name,
        body->>'event_type' as event_type,
        body->>'country' as country,
        headers->>'X-Application' as application
  ),
  -- create a subsource that contains specific info.
  SUBSOURCE <name> (
    SELECT
      body->>'id' as id,
      body->>'ip_addr' as id_address
  );
```

## Open questions

* Security. Users can now push data to Materialize via a publically accessible address. This opens
  up a new attack vector that we probably need to lock down. For example, maybe we support an allow
  list or block list of IP addresses. Do we need to have DDOS protection higher up in the stack? We
  should at least come up with a plan here so we know how we're exposed and prioritize from there.
