# Volatile sources

## Summary

Our users want to connect Materialize to upstream sources that do not provide
guarantees about what data they will present. These sources are in tension with
the goal of providing a deterministic, replayable system.

This design doc proposes to:

  * Introduce "volatile source" as a term of art that describes a source that
    does not meet a specific standard of reliability.

  * Use the terminology consistently in the docs site and error messages to
    educate our users about volatile sources.

  * Restrict the use of volatile sources in contexts where they could impede
    future improvements to the system's determinism and correctness.

## Goals

The only goal is to **stabilize the [PubNub] and [SSE] sources**.

Both of these are volatile sources with some worrying correctness properties,
but their existence will massively improve the "wow factor" for demos of
Materialize Cloud.

[PubNub]: https://github.com/MaterializeInc/materialize/pull/6189
[SSE]: https://github.com/MaterializeInc/materialize/pull/6208

## Description

### Terminology

A "volatile source" is any source that cannot reliabily present the same data
with the same timestamps across different instantiations.

A "instantiation" of a source occurs whenever:

  * A new index is created on the source or on an unmaterialized view that
    references the source.
  * An ad-hoc query or `TAIL` references the source directly.

Restarting Materialize will cause all sources to be reinstantiated. Referencing
a materialized view that in turn depends on a source does not create a new
instantiation of a source.

Examples of volatile sources:

  * Any Kinesis source. Since Kinesis streams have mandatory retention periods
    (by default 24 hours), a new instantiation of the source will not see
    any data that has expired.
  * A PubNub demo source, since new instantiations always start from the tail
    of the stream.
  * An append-only Kafka source with a non-infinite retention policy.
  * A file source, where the file is periodically overwritten.

Examples of nonvolatile sources:

  * A PubNub source with [retention](https://www.pubnub.com/docs/messages/storage),
    though we don't presently support this.
  * An append-only Kafka source, when the topic has infinite retention and no
    compaction policy.
  * An upsert Kafka source, when the topic has infinite retention but *may*
    have a key compaction policy.
  * A file source, when the file is treated as append only.

It's important to note that volatility is a property of how a source is
*configured*, rather than fundamental to a source type. Kinesis is the rare
exception. All other sources can be either volatile and non-volatile modes,
depending on how the source is configured.

### Discussion

Volatile sources impede Materialize's goal of providing users with correct,
deterministic results. If you restart Materialize, we want to guarantee that you
will see exactly the same data at exactly the same timestamps, and there is
ongoing work to facilitate that. If you've created a volatile source, there is
simply no possibility that we'll be able to provide that guarantee, because the
upstream source may have garbage collected the data that you saw last time you
ran Materialize.

Several forthcoming initiatives depend on this determinism guarantee, however:

  * Exactly-once sinks
  * Active-active replication
  * Dataflow transformations that consider source instances to be logically
    equivalent

My contention is that it is wholly unrealistic to expect our users to only use
nonvolatile sources with Materialize. The streaming world runs on inconsistency
and nondurability. To play in this space, we need to interoperate seamlessly
with volatile sources, and then offer a large, inviting off ramp into the
Materialize world of determinism and consistency.

The proposal here, then, is to bake the notion of volatility into the system. We
start by marking volatile sources as such. Any view that depends on such a
source becomes volatile. Any sink created from a volatile source or view becomes
volatile. The optimizer will need to be careful not to treat different
instantiations of a volatile source or view as logically equivalent. Exactly
once sinks and other features that only work on nonvolatile sources can look
for and reject volatile sources.

### Design

To start, I propose that we observe a simple rule:

  * All PubNub, SSE, and Kinesis sources are marked as volatile.
  * All other sources are marked as having unknown volatility (`NULL`).

In the future, we'll want to allow users to mark Kafka, S3, and file sources as
volatile at their option, to capture when their *configuration* of the source is
volatile. See [Future work](#future-work) below.

The only user-visible change at the moment will be an additional column in
`SHOW SOURCES` that indicates whether a source is volatile

```
> SHOW SOURCES;
 name        | type | volatile | materialized
-------------+------+----------+-------------
 pubnub_src  | user | true     | false
 kafka_src   | user | NULL     | false
```

and similarly for `SHOW VIEWS`:

```
> SHOW VIEWS;
 name         | type | volatile | materialized
--------------+------+----------+-------------
 pubnub_view  | user | true     | true
 kafka_view   | user | NULL     | true
```

In the docs, we'll want to add a page about "Volatile sources" that explains
what they are, why they are good for demos, and why they are bad for production
applications that care about correctness. We can add a warning callout on
PubNub, SSE, and Kinesis source pages that links to the volatile source page.

The big "gotcha" with volatile sources is that different instantiations can have
different data within the same `materialized` process. Consider a sequence like
the following:

```sql
CREATE SOURCE market_orders_raw FROM PUBNUB
SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
CHANNEL 'pubnub-market-orders';

CREATE MATERIALIZED VIEW market_orders_1 AS
  SELECT text::jsonb AS val FROM market_orders_raw;

-- Imagine the user makes a cup of coffee now.

CREATE MATERIALIZED VIEW market_orders_2 AS
  SELECT text::jsonb AS val FROM market_orders_raw;
```

The data in `market_orders_1` and `market_orders_2` will not be equivalent,
because they'll each have instantiated separate copies of the source at
separate times. `market_orders_1` will contain some historical data that
`market_orders_2` does not.

The more I think about it, the more this seems like reasonable, even desirable
behavior from a volatile source, provided we can educate users sufficiently.

### Implications

#### Exactly-once sinks

Exactly-once sinks will need to error if they are created on a volatile source
or view. This will be easy to check in the coordinator. The error message might
read:

```
ERROR:  exactly-once sinks cannot be created from a volatile {source|view}
DETAIL: Data in volatile {sources|views} is not consistent across restarts,
        which is at odds with exactly-once semantics. To learn more, visit
        https://materialize.com/s/volatility.
```

#### Active-active replication

Active-active replication will rely on the determinism of the dataflow
computation to compute identical results across two separate instances of
Materialize. That is at odds with volatile sources, which will provide the
two copies of Materialize with two different sets of data.

One easy solution is to simply ban volatile sources when using Materialize
in this hypothetical distributed mode.

Alternatively, we could say "you got what you asked for", and message the
deficiencies accordingly.

#### Optimizer

The optimizer will need to be careful to treat different instances of volatile
sources and views as logically different views. This is a burden for the
optimizer team, but hopefully a small one. The check to see whether two
sources or views is roughly:

```rust
fn are_objects_equivalent(metadata: &Metadata, id1: GlobalId, id2: GlobalId) -> bool {
    id1 == id2 && !metadata.is_volatile(id1)
}
```

#### Other to-be-developed features

There is no obligation for new features to play well with volatile sources. In
other words, it is acceptable for engineers to treat volatile sources as
second-class citizens in Materialize.

For example, if you add a new sink type, there is no obligation to make it work
with volatile sources if doing so would be difficult. If you add a new
optimization that would be substantially complicated by volatile sources, there
is no obligation to do that hard work; you can just disable the optimization for
volatile sources instead.

That said, our backwards compatibility still applies to volatile sources. It is
not okay to break existing, working features involving volatile sources (modulo
extenuating circumstances).

## Future work

### Toggleable volatility

In the future, I believe we'll want to allow users to explicitly mark a source
as nonvolatile or volatile at their option, e.g. via a flag to `CREATE SOURCE`:

```sql
CREATE SOURCE [NONVOLATILE | VOLATILE] src
FROM KAFKA BROKER ...
```

Correctly choosing the defaults here will require a good deal of thought,
though, so I'd like to hold off on this discussion for now. The proposal above
of treating all Kafka, S3, and file sources as nonvolatile is equivalent to the
system's current behavior.

Another benefit of deferring this toggleability is that computing the
`volatility` bit as proposed in this doc does not require storing any additional
state, as it is a pure function of the source type. If we decide we don't like
the concept of volatility, we can excise it without breaking backwards
compatibility.

### Volatility removal

Source persistence may one day permit the conversion of a volatile source
into a nonvolatile one. If we can write down the data we see as it flows into
Materialize, then it doesn't matter if the upstream source garbage collects it.

### Expanded volatility bits

We might discover that a single volatility bit is too coarse. For example,
exactly-once sinks could learn to support volatile sources that provide the
guarantee that you will never see a message more than once.

This proposal leaves the door open for slicing "volatility" into multiple
dimensions. This is another good reason to punt on
[toggleable volatility](#toggleable-volatility). Once volatility specifications
are part of `CREATE SOURCE`, we'll have to worry about backwards compatibility;
until then, we have a lot of latitude to refine our notion of volatility.

### Automated volatility violation detection

Relying on users to properly declare their sources as volatile or nonvolatile is
error prone. Unfortunately this problem is inherent to interfacing with external
systems; our users are responsibile for configuring and operating those systems
according to some specification, and then telling us what that specification is.
Given a nonvolatile Kafka source, for example, there is simply no way for
Materialize to prevent a user from adding a short retention policy to the
underlying Kafka topic that will make the source volatile.

Still, in many cases, Materialize could assist in detecting volatility
violations. We could spot-check the data across instances of the same source
occasionally, for example, and report any errors that we notice. We could
periodically query the Kafka broker for its retention settings and surface any
misconfigurations. We could remember a file's inode and size, and complain if
either has changed after Materialize restarts.

## Alternatives

### Forced materialization

One considered alternative was forcing all PubNub and SSE sources to be
materialized. This neatly sidesteps the volatility within a single
`materialized` binary, because it guarantees there is only one instantiation
of each volatile source.

This _severely_ limits Materialize's expressivity, though. One use case we
expect to be very common in demos is to only materialize the last _n_ seconds
of data, which requires both an unmaterialized source and a materialized view:

```sql
CREATE SOURCE market_orders_raw FROM PUBNUB SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe' CHANNEL 'pubnub-market-orders';

CREATE MATERIALIZED VIEW market_orders AS
  SELECT *
  FROM (
      SELECT
        (text::jsonb)->>'bid_price' AS bid_price,
        (text::jsonb)->>'order_quantity' AS order_quantity,
        (text::jsonb)->>'symbol' AS symbol,
        (text::jsonb)->>'trade_type' AS trade_type,
        ((text::jsonb)->'timestamp')::bigint * 1000 AS timestamp_ms
      FROM market_orders_raw
    )
  WHERE mz_now() BETWEEN timestamp_ms AND timestamp_ms + 30000
```

### One-shot sources

One other discussed alternative was to introduce a "one-shot" source, which
could be used in exactly one materialized view:

```
CREATE MATERIALIZED VIEW market_orders AS
 WITH market_orders_raw AS SOURCE (
     PUBNUB SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe' CHANNEL 'pubnub-market-orders';
  )
  SELECT *
  FROM (
      SELECT
        (text::jsonb)->>'bid_price' AS bid_price,
        (text::jsonb)->>'order_quantity' AS order_quantity,
        (text::jsonb)->>'symbol' AS symbol,
        (text::jsonb)->>'trade_type' AS trade_type,
        ((text::jsonb)->'timestamp')::bigint * 1000 AS timestamp_ms
      FROM market_orders_raw
    )
  WHERE mz_now() BETWEEN timestamp_ms AND timestamp_ms + 30000
```

The idea here is to force you to type out the `SOURCE` definition every time you
want to use it, to make it more clear that you will get different data in
different views that use that source. But this design does not fit well into the
existing catalog infrastructure, which maintains a very clear separation between
sources and views. And even with this design, I _still_ think we'd want to
introduce the notion of volatility, because we'll want to exactly-once sinks and
active-active replication to know that `market_orders` does not uphold the
usual consistency properties.

### Wait for volatility removal

We could, in principle, reject the idea of volatile sources, and wait for source
persistence to unlock [volatility removal](#volatility-removal). Then the story
for using volatile sources with Materialize would be to _require_ using them
with source persistence, so that they become nonvolatile.

Source persistence is likely 1-2 years away, though, especially an
implementation that works seamlessly with compaction. It's also not clear to
me whether every user who wants a volatile source would be ok with requiring
persistence for those sources.
