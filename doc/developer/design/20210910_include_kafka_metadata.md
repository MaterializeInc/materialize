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

We will extend the existing `INCLUDE` syntax to take an optional `METADATA
(timestamp | offset | partition | topic)+` clause. These are all the remaining
fields in a Kafka message that are not `headers`. Each field inside the field
list can take an optional `AS <name>` clause, in case of conflicts with the key
or value names.

With this, the full syntax for the `INCLUDE` clause becomes:

```
INCLUDE (
    KEY (AS <name>)?
    | METADATA ((timestamp | offset | partition | topic) (AS <name>)?)+
)
```

With an example of just the new syntax looking like:

`INCLUDE METADATA (timestamp, offset)`

And an example of it being combined with existing uses of `INCLUDE KEY` full
syntax:

`INCLUDE KEY, METADATA (timestamp, offset, partition, topic)`

and with the renaming syntax:

`INCLUDE KEY AS mykey, METADATA (timestamp AS ts, offset as "Off")`

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

### Alternative syntax

We could write this without the `METADATA` prefix, just field names as keywords:

`INCLUDE KEY, TIMESTAMP, OFFSET, PARTITION, TOPIC`.

Aesthetically this looks a little better to me. Which one we use depends I think
primarily on whether we want to consider these as the same category of item as
user data, or if we want to more clearly signify that these properties are
always set by Kafka itself.

With our likely eventual support for user-set headers (which I believe will
require a user-specified list of fields) we will end up with the full `INCLUDE`
syntax looking like one of the two following examples:

```
INCLUDE
    KEY,
    METADATA (timestamp, offset),
    HEADERS (encryption_key_id)
```

vs

```
INCLUDE
    KEY,
    TIMESTAMP,
    OFFSET,
    HEADERS (encryption_key_id)
```

## Open questions

### Debezium data cannot be retracted with metadata columns

The initial implementation will just prevent use of `INCLUDE METADATA` with
`ENVELOPE DEBEZIUM`, in the same way that we prevent `INCLUDE KEY` with Debezium
data.

The problem is that the Debezium envelope self-retracts data, but the data that
it knows how to retract does not include any Kafka metadata, so all retractions
(that is, all UPDATEs and DELETEs in upstream databases) will fail.
