# Include Kafka Metadata

## Summary

Users have regularly requested access to certain pieces of Kafka (and other)
per-message metadata within views. The most common requests have been for the
timestamp and offset fields.

The proposal here is to extend our syntax to allow users to opt-in to including
message headers on a per-source basis.

## Goals

Give users easy access to commonly-requested metadata without any backwards
compatibility concerns.

## Non-goals

Generic support for Headers. The Kafka API allows users to specify [arbitrary
per-message headers][headers-javadoc]. As far as I know we haven't received
requests for this yet, and the Kafka API does not provide enough information for
us to automatically decode header values (it's [just bytes][headers-rustdoc]),
so the exact shape of what we would give to users requires more design work.

[headers-javadoc]: https://kafka.apache.org/27/javadoc/org/apache/kafka/connect/header/Headers.html
[headers-rustdoc]: https://docs.rs/rdkafka/0.26.0/rdkafka/message/trait.Headers.html

## Description

We will extend the existing `INCLUDE` syntax to take additionaloptional metadata
field specifiers: `timestamp`, `offset`, `partition`, and `topic` clause. These
are all the remaining fields in a Kafka message that are not `headers`. Each
field inside the field list can take an optional `AS <name>` clause, in case of
conflicts with the key or value names.

With this, the full syntax for the `INCLUDE` clause becomes:

```
INCLUDE (
    KEY (AS <name>)? |
    TIMESTAMP (AS <name>)? |
    OFFSET (AS <name>)? |
    PARTITION (AS <name>)? |
    TOPIC (AS <name>)?
)+
```

With an example of just the new syntax looking like:

`INCLUDE TIMESTAMP`

And an example of it being combined with existing uses of `INCLUDE KEY` full
syntax:

`INCLUDE KEY, TIMESTAMP, OFFSET, PARTITION, TOPIC`

and with the renaming syntax:

`INCLUDE KEY AS mykey, TIMESTAMP AS ts, OFFSET AS "Off"`

### Column Types

Each of the new fields will become a column in the dataflow, with an appropriate
type:

* `TIMESTAMP` will become a nullable `TIMESTAMP WITH TIME ZONE` ([javadoc][ts])
* `OFFSET` will become a nullable `BIGINT` ([javadoc][offset])
* `PARTITION` will become an `INTEGER` ([javadoc][partition])
* `TOPIC` will become a `STRING` ([javadoc][topic])

[ts]: https://kafka.apache.org/28/javadoc//org/apache/kafka/clients/producer/RecordMetadata.html#timestamp()
[offset]: https://kafka.apache.org/28/javadoc//org/apache/kafka/clients/producer/RecordMetadata.html#offset()
[partition]: https://kafka.apache.org/28/javadoc//org/apache/kafka/clients/producer/RecordMetadata.html#partition()
[topic]: https://kafka.apache.org/28/javadoc//org/apache/kafka/clients/producer/RecordMetadata.html#topic()

### Envelope Support

#### Background

Envelopes will have varying support for metadata fields, due variously to their
inherent properties and engineering effort. The key constraint is that any
envelope that knows how to do its own retractions (i.e. the Debezium and
Materialize envelopes) require that the full retraction be present in the data.
Essentially, retraction-aware envelopes provide `materialized` with information
that is morally equivalent to:

```javascript
// "insert" event
{
    "retract": null,
    "insert": [1, "a"]
}
// "update" event
{
    "retract": [1, "a"],
    "insert": [1, "b"]
}
```

The retraction-aware envelopes understand the semantics of these events and will
correctly provide retraction/insertion messages to their dataflows.

The problem comes if we add metadata, it is guaranteed that each message will be
unique:

```javascript
// "insert" event
{
    "retract": null,
    "insert": [1, "a"],
    "offset": 100
}
// "update" event
{
    "retract": [1, "a"],
    "insert": [1, "b"],
    "offset": 704
}
```

If we insert the offset from the insert event then the row in Materialize
becomes `[1, "a", 100]`. When we see the update event, the event itself does not
have enough information to provide a retraction, the naive retraction would be
to try and `DELETE [1, "a", 704]`, but that does not exist and will therefore
cause errors in dataflows.

This can be worked around by maintaining UPSERT-like state for each envelope,
but it is unclear how needed this work is, and so the next section describes the
intended initial state and future work possible or required.

#### Trivially supported

`ENVELOPE NONE`: works inherently, no additional work required


#### Small amount of work to support

Both of `UPSERT` and `DEBEZIUM UPSERT` require the approximately the same amount
of effort as `ENVELOPE NONE` and should be part of the initial implementation.

#### Future work

Envelopes `DEBEZIUM` and `MATERIALIZE` on data streams that have primary-key
semantics (i.e. where rows are guaranteed to be unique according to some key
which is a subset of the data) can both be made to work by creating a shadow
upsert-like map from previously-seen entry keys to the metadata that they were
originally inserted with. For `ENVELOPE DEBEZIUM` this would just allow a memory
saving over `DEBEZIUM UPSERT` since Materialize would not need to store all the
data from each row, just the metadata.

It is unclear what the use case for the combination of these envelopes with
metadata is, so we are not expecting this to be worked on until we receive use
cases that need them.

#### Significant design and work to support

Self-retracting formats that operate on streams of data where rows do not have
primary-key semantics cannot be made to work without input from the user -- if
the same row was inserted three times, with three different offsets, which one
do you retract?

## Alternatives

### We could not include the topic as a possible field

It seems silly to include it since each source comes from exactly one topic.
Since this proposal is opt-in, though, it costs us and the users nothing to
include it unless it is used, and I could imagine users wanting it in JOINs or
UNIONs between sources.

### We could include everything by default

We have discussed including metadata fields by default and relying on projection
pushdown to eliminate unused fields.

This has several drawbacks though:

* It does not have any way of handling conflicts, so if the message has a field
  named `timestamp` then sources would immediately become unusable (with an
  "ambiguous column" error).
* Since it's opt-out, it would meaningfully change the semantics and memory
  usage of all existing sources that don't have a view with an allowlist of
  columns. That is, anything that is `SELECT *` all the way from source to final
  materialization.
* It would make choosing which fields to include more important -- adding the
  topic by default seems like it would be kind of ridiculous.
* The overall ergonomics would be meaningfully worse -- it would require all
  users to write a projecting view if they want access to just their data. Based
  on the fact that all our current users are getting by with none of these
  fields, that will be a majority of users.
