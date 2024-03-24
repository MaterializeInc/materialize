# Webhook idempotency

- Associated:
    - [Webhook design
      doc](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20230615_webhook_source.md)

## Context

To make webhook sources offer sane durability semantics, we need to make their
"inserts" (i.e. `POST`s) idempotent––for example, Segment will try to make
webhook `POST`s for multiple hours until they receive a terminal status (i.e. I
believe they retry on 5xx errors).

## Goals

Propose a solution to make webhook sources support idempotency on a configurable
timeline (e.g. 24 hours). This style of idempotency is supported by other
webhook integrations, such as
[SaaSquatch](https://docs.saasquatch.com/developer/event-idempotency#event-idempotency).

Note that this form of idempotency should also be durable, e.g. idempotent for
24 hours independent of Materialize (or any of its components) restarting.

## Non-Goals

- Make all requests idempotent for all time (requires infinite memory)
- Ensure that webhooks are usable within a certain price point
- Ensure that webhooks achieve some specific throughput/latency. We want these
  to be fast enough to be useful but introducing additional disk writes cannot
  be a deal breaker. 

## Overview

The simplest means by which we can offer idempotency for webhooks is using
`UPSERT` envelope semantics for an idempotency key included in the requests'
headers or bodies.

To simplify the implementation, we plan to use the exact same machinery that the
`UPSERT` envelope uses, which means that webhook sources will need to run on
`clusterd` (they currently run on `environmentd`).

## Detailed description

### Semantic overview

#### SQL

When creating a source, just like `CHECK`, we will have a new `IDEMPOTENT`
clause that will give us an expression through which we can extract the
idempotency key from the message.

This will generate an expression which can be run on either `environmentd` or
`clusterd` to extract the idempotency key and store it wherever need be. Will
discuss this further in the **Architectural overview** below.

#### Windowed idempotency

We must retain idempotency keys for committed operations, which means that the
collection would grow without bound.

To make the data we retain more likely to fit on a single machine, we should
introduce the notion of an idempotency window--i.e. operations are guaranteed to
be idempotent if they are reapplied within some amount of time from the first
time the operation was applied.

With a sufficiently large window (e.g. 24 hours), this should offer idempotency
while providing us peace of mind that the `UPSERT` envelope state will not grow
without bound.

##### Figuring out the time

The ultimate goal here is to reduce the amount of data we have to retain in the
envelope state and a simple means of doing that is comparing two calls to a
`NowFn` and determining if their difference is within some configurable bound.

To simplify this tracking, we can include one of the `NowFn` calls as a column
of data, which we can then compare to another call to a `NowFn` when assessing
which data to keep in the envelope state. While this is remedial, it's something
simple that we can generalize in some way.

More on this in the **Upsert state reduction** section of the design doc.

Note that because we want to bound the amount of state we keep in the upsert
envelope, we must have _something_ that correlates time to a write. An
alternative here would be to include batch numbers in the row in lieu of a
timestamp, and change the compaction window of the remap collection to whatever
retention period we want--however, this adds complexity in successfully dropping
the data from the upsert envelope.

#### Idempotency guarantees

We plan to offer `UPSERT` idempotency, which means that we will prevent adding
state to a persist shard if we receive a second write whose `kv` pair matches
one we have in the `UPSERT` state.

However, this means if we receive multiple requests with the same idempotency
token, we will retract the initial state and leave the last-seen value in the
persist shard. Because we plan have add the time the request was received to the
row of data we append, this means we will continually update the time the
request was written and extend the lifetime of the idempotency key from the last
time the write occurred.

I think this is reasonable, but wanted to make sure this behavior's is
explicitly understood as a tradeoff of storing the time.

**Alternative**

It's possible to achieve a similar outcome by increasing the compaction period
for the remap shard such that we can get the time at which each event occurred
in Materialize time--this feedback loop seems challenging to instrument,
however.

### Architectural overview

#### Dataflow

##### Source rendering

The rendered dataflow will simply read updates out of a channel and give them to
a data capability at some cadence, downgrading on either some interval's tick or
when the amount of buffered data reaches some threshold.

Providing users a threshold of requests to buffer offers some knob to control
throughput vs. latency for the source.

**Timestamping**

The native timestamp of the source could just be a `u64`, which we increment for
each batch we produce.

The annoying corner of this, though, is that the latency of any request is
limited by the frequency with which we reclock batches. The most feasible option
I see here is providing users the option to reclock on a shorter period. The
current period is 1s, but it could be set to some configurable duration. I'm not
sure how low this could be set to produce a meaningful effect for the user.

**Request responses**

To provide durability guarantees for webhook sources, we cannot respond to the
client with a `200` message until we are sure that the request has been
committed to the shard.

We can retain an in-memory map of `BTreeMap<u64, BTreeSet<IdempotencyKey>>`, and
then as we get feedback that a batch has been committed can send back all
idempotency keys committed along response edge.

This has the side-effect of forcing us to send `200` responses on writes whose
idempotency keys we've already seen because the entire batch commits, i.e. we do
not have the ability in the rendering to understand if a write committed _now_
or was already present. I don't believe this to be problematic--a `500` response
followed by a `200` response, irrespective of the "underlying" result of the
`500` response still unambiguously states that the write committed.

##### Upsert state reduction

Currently, the `UPSERT` envelope purposefully retains all state for all time.
However, that is _not_ desirable for a webhook source, whose domain of keys is
unreasonably large.

_I have not meaningfully dug into the feasibility of the following approach, so
suggestions very welcome._

To facilitate this, we could introduce some kind of closure that permits
mutating upsert state which could take in e.g. all upsert state + an immutable
reference to the source config.

In the case of webhook source, we could determine the last time we reduced the
state and every `<period of time>`, we could iterate over the upsert state and
remove keys we no longer want.

Another option is to set a limit on the amount of state we keep in the upsert
envelope and evict state based on some closure (e.g. we would want oldest values
in the case of webhook sources). Tuning this seems annoying, though, because you
would want to evict batches of records to avoid having to clean up records on
every insert.

#### Webhook gRPC server

We can set up a new gRPC server that runs in `clusterd` alongside the persist
and storage server gRPC services.

This server has a few functions:
- External API
    - Ingest webhook requests from `environmentd`
    - Send responses when request durably recorded
- `clusterd`-internal API
    - Maintain a set of channels to demultiplex requests into a channel where a
      running dataflow can ingest it.
    - Maintain an `mpsc` to` receive responses from dataflows once they've
      durably recorded the request and send responses to `environemntd`.

The external API can be facilitated through a standard gRPC service
implementation.

The `clusterd`-internal API requires modifying in-memory state, so needs to
provide a sender and receiver for requests that can both register and
un-register `GlobalId`s to bidirectional channels.

In essence, this gRPC server works as a reverse proxy for sending requests to
Timely workers.

**Early-arriving messages**

Each `clusterd` will spawn a webhook server on boot, just as it does with the
storage server.

This means that there is possibility of webhook requests coming into the service
because it's been given a channel to propagate them to. In this case, I suggest
we just return a status code equivalent to service not available yet, please
retry later.

**`mz_service`?**

`mz_service`'s module-level documentation says:

> The code shared here is not obviously reusable by future services we may split
> out of environmentd.

And poking around the code, this seems to be true. Paul and I noodled on how to
use this module for this service and while it seems possible, it seems more
legible and simpler to maintain to just roll a new gRPC server.

During the implementation, I plan to use persist's PubSub service as a reference
implementation and will extract common code from it into some _other_ shard
module and/or unify the implementation with `mz_service`.

#### Storage server + `clusterd`

`clusterd` will house the new webhook gRPC server.

We will spin up the webhook gRPC server before the storage server.

The storage server will grow a handle to the webhook server, which will be able
to handle registering and un-registering webhook sources. Commands to register a
source will provide a `GlobalId` and, in response, get handle to a stream to
read requests from, as well as send responses to. This handle will eventually
end up as part of a worker's `StorageState`, and the channels will end up in the
dataflow for the webhook source.

#### `environmentd` Handler

Some endpoint in `environmentd` will grow the ability to send gRPC requests to
the webhook service running on each cluster. This bit of code will need the
ability to understand which cluster each source is running on, as well as have a
means of understanding (and likely caching) which port on the cluster to find
the webhook service.

For simplicity's sake, we will still have this endpoint perform validation and
extract the request's idempotency token.

However, we will now have it also stash the channel to respond to in a
`BTreeMap<IdempotencyToken, ResponseChannel>`. As it receives responses from the
webhook gRPC service, it will send the appropriate response along to the
awaiting client. Responses will be akin to some set of:

- `200` for successes (response from webhook server)
- `404` if the webhook server doesn't yet have a channel set up for the source
  yet (response from webhook server)
- `408` if the webhook server doesn't respond (response from handler)
- `502` if `environmentd` doesn't believe the request is for a webhook source
  (response from handler)

For the length of time that we have 1 `envd` process, this also gives us a very
simple tool to understand the number of outstanding writes the handler has,
giving us a simple tool to e.g. limit concurrency. Writes in excess of the
concurrency limit can simply be given a `503`.

## Alternatives

I considered whether or not this source needs to run in Timely whatsoever and
determined that making a `clusterd` not compatible with Timely's cooperative
scheduling was an idea whose time has not come.

There are many pivot points within this design, some of which I've touched on.
However, the main point is that we have not seriously considered any
alternatives to using the `UPSERT` envelope's implementation. Doing so would
require re-implementing something with essentially the same semantics elsewhere,
which introduces complexity.

## Workflows

### `CREATE CLUSTER`

1. On `clusterd` boot, creates Webhook GRPC server, which includes a handle to
   send it requests to register new collections.
1. Handle given to Storage server, which is, in turn, cloned on each Worker, so
   they can communicate with the cluster's webhook server.

### DDL

1. `envd` receives `CREATE WEBHOOK`, communicates with storage controller, which
   in turn communicates with storage server.
1. One worker is elected the recipient of the webhook requests. It uses its
   webhook gRPC handle to establish a bidirectional channel.
1. Starts dataflow listening to webhook channel.

## Request graph high-level

![high-level diagram](https://i.imgur.com/18TE0GP.png)

1. Webhook HTTP API receives request, forwards to `envd`
1. `envd` passes request to "Webhook handler."
1. "Webhook handler"...
   1. Looks up webhook address for `GlobalId`?
   1. Authenticates request?
   1. Extracts idempotency token
   1. Plugs `NowFn` value into row, so value becomes:
      ```
      {
        "key": <idempotency token>
        "value": <values>.push(NowFn())
      }
      ```
   1. Sends gRPC to webhook.
   1. Stashes response channel in `BTreeMap<IdempotencyToken, Channel>`.
1. Webhook gRPC server forwards request for `GlobalId` to appropriate channel?
1. Webhook gRPC listens for responses and forwards them to the "Webhook handler"
   in `envd`.
1. "Webhook handler" streams in idempotency tokens that have been committed, and
   looks up the client channel to which it should respond.

## Request graph low-level (worker-level)

![worker-level diagram](https://i.imgur.com/MwjWhsU.png)

1. Worker_n receive webhook data, which it buffers until it reaches the
   appropriate threshold, at which point it gives the data to the data output at
   some time equal to the number of batches it's already given + 1.

   When giving this data, we keep an in-memory map of all of the idempotency
   tokens associated with this batch.
1. Remapping/reclocking, envelope processing occurs.
1. At the end of the upsert logic being applied, we offer the upsert state the
   option to perform a reduction based on some configurable parameter, assuming
   it's determinable from the individual data in the state + some other external
   state, such as access to a `NowFn`.
1. As the webhook dataflow on the worker receives feedback from the committed
   uppers edge, it determines which idempotency tokens were committed in the
   batch, and send them as responses to the webhook gRPC server.

## Open questions

- Is a self-truncating operator possible to wedge into the upsert state in a way
  that a human being can write?

### Platform v2 and beyond

There are features being developed at some point in the somewhat near future
that we want to ensure idempotent webhooks offer compatibility with:

- Horizontally scaled servicing layers (`envd`)
- Replicated clusters

#### > 1 `envd`

Assuming there is still only one Webhook gRPC server running, each request now
needs to include its environment ID, as well as its `GlobalId` to ensure the
response makes it back to the client who's awaiting it.

The webhook gRPC server then pulls requests off of a `mpsc` channel, and stores
the request in a `BTreeMap<IdempotencyToken, EnvironmentId>`.

This scheme has the advantage that it lets the source itself still control
batching and doesn't need to be aware that multiple clients are providing its
writes.

#### > 1 `clusterd` + > 1 `envd`

_preface: this is an exploration to demonstrate that idempotent webhook sources
could be viable in this world; the following is not meant to be a proposal._

The architecture of replicated ingestions with horizontally scaled `envd`s looks
something like this:

```
[envd_0]       [envd_1]       [envd_2]

                  ???

[clusterd_a]   [clusterd_b]   [clusterd_c]
```

where `???` is some means of coordinating writes from all of the workers and
ensuring they're linearizably distributed to all of the clusters.

Having _n_ writers distributed messages to _m_ readers in a way that requires
little coordination between the services sounds like a message bus to me and
using Kafka/Redpanda provides a turnkey solution to the problem of writes. This
means we could drop the dataflow code that ingests from these channels and
instead render these as Kafka sources.

For determining when we can return `200` statuses to the clients, each `envd`
could maintain a subscribe to the output of the persist source and peel off
clients when the idempotency key it's tracking comes across. With our plan to
update the timestamp to the last-seen request, we can also support "double
writes" for a idempotency key, even across clients because every write is
guaranteed to occur unless it was entered at exactly the same timestamp as some
other write--but that write having occurred elsewhere is sufficient to trigger
the response to the client.

In the corner case of some timing madness, the write would have still occurred,
but the worst thing that happens is that the writer times out. This is bad for
our p99.9+ latency, surely, but doesn't seem likely to occur or that insane.
