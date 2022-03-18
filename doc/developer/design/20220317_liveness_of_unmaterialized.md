# Supporting liveness of weak reads on "unmaterialized" sources

## Summary

[This design document](https://github.com/MaterializeInc/materialize/pull/11262) documents
how we want to achieve _strict serializability_ as an option for users querying sources
in `materialized`. In addition, it mentions that currently, "materialized" sources are _serializable_ and "live".

This document describes how we will add this liveness property to "unmaterialized" sources.

## Goals

Offer an eventually-consistent, live experience for "unmaterialized" sources, to match the semantics of
"materialized" sources.

## Non-Goals

Alter the default experience of using `materialized` (yet).

## Description

### Background

This is in contrast to the current default for reads against "materialized" sources,
which is `serializable` (we endeavor for completely consistent reads as any given timestamp),
but not neccesarily _current_, where _current_ means "completely up-to-date with upstream.
We do, however, offer "liveness", which means
we make sure that we are always making progress through the source's upstream.

This is not a property we currently have for "unmaterialized" sources, which have no
mechanism for ensuring liveness.

We _do_ have a mechanism called _timestamp bindings_, which associated offsets and timestamps, which ensure _serializability_ for `materialized` sources, which have
a default _index_ that continuously produces new bindings.

### Solution

Instead of creating a default _index_ on "unmaterialized" source creation, we can instead create a special source _instance_, that produces messages (with offsets),
for which we store no actual data. This _instance_ will cause _timestamp bindings_ for these offets. When reads (also called "peeks") happen involving these
sources, the pre-existing timestamp-selection logic will select compatible timestamps, causing us to present _live data_ (that is, data that is up-to-date with the
timestamp bindings).

This will not affect any other query, even queries that force _strict serializability_. However, the alternative is true, and _strict serializable_ reads, as discussed
the other design document, may cause these weaker reads to wait.

### Source-specific considerations

We can control what we offer based on the guarantees that the upstream source can give us:

The _level_ of liveness we can offer for unmaterialized sources is dependent on the source type:

- Kafka: We durably write down timestamp bindings for kafka, so we can present _live_ unmaterialized sources, that behave across
restarts.
- Kinesis and Pubnub: We can present _liveness_, but not across restarts, as we don't (and can't (TODO: is this true?)) durably store timestamp bindings.
- Tables: Tables are always _live_ and _serializable_.
- Local files: we can probably present liveness across restarts, but it may be expensive.
- S3/Postgres: (TODO: investigate)

In the beginning of implementation, we will focus on Kafka.

### Platform considerations

This plan should be compatible with being entirely contained within `STORAGE`, especially considering [this pr](https://github.com/MaterializeInc/materialize/pull/11223)

In addition, this plan, while important to improve the experience of `materialized` users, may not be important in the Platform world, where the offsets and data from
all sources will be durably written down, always. In that world, _liveness_ will be a default guarantee of "unmaterialized" sources.

## Alternatives

- Change the default experience of using `materialized` to be `LINEARIZABLE`
- Don't offer this experience, and force _strict serializability_ on uses of "unmaterialized" sources

## Open questions

- More investigation in the exact guarantees of different source-types
- Confirmation of future of Platform
