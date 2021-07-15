# Reclocking

## Summary

"Reclocking" a stream of data is (for the purposes of this document) translating its events from one gauge of progress to another.
One example is taking Kafka topics (whose records are stamped with progressing `(partition, offset)` pairs) into Materialize's system timeline (whose records are stamped with milliseconds since the unix epoch).

Many of our sources of data come with their own understanding of forward progress.
Often, these are incompatible with each other, and with native notions of time within Materialize.
For example,
    Kafka topics contain multiple "parts", each of which advances through "offsets".
    Postgres sources come from a log where entries have a "log sequence number" (LSN).
    File sources have a line number.
    Materialize CDC sources have explicit timestamps.

The source-specific "gauges" of forward progress are useful, but they are not directly comparable.
This document describes one framework for converting between gauges including in to and potentially out of Materialize's system timeline.

## Goals

The primary goal of this document is to describe a formal way to translate collections that evolve with one gauge into collections that evolve with another gauge.
That formalism should have the properties that:
1. There is an explicit representation of the translation (rather than a behavioral description).
2. The translation should be sidecar metadata, rather than a rewriting of the source data.
3. The combination of durable source data and durable metadata should result in a durable reclocked stream.
4. The sidecar metadata should be compactable whenever the reclocked stream is compactable.

The formalism should support the goals of
A. Permitting the use of streams from one timeline in another timeline.
B. Describing durable state that can make sources like Kafka, Postgres, and files exactly replayable in the system timeline.
C. A BYO format for users who want to provide explicit timestamp information for sources that are presented not in that timeline.
D. A way to describe in one timeline progress made through another timeline (e.g. reporting Kafka read progress in MZ system timestamps).

## Non-Goals

This document has no opinions on the physical representation of the reclocking data.
This document has no opinions on the implementation of the reclocking actions.
This document has no opinions on the policies of which streams should be reclocked into which domains.
This document has no opinions on the correct interpretation of the terms `VOLATILE` and `NONVOLATILE`.

## Description

Streams of data often provide a gauge of progress: stamping records with values and indicating which values will not be seen again.
This information describes "progress" through a stream, and allows consumers of the stream to rely on the presence of certain contents.
Multiple records may share the same gauge value, indicating that they should occur at the same moment.
Records may not be in order of the gauge, but once a value is indicated to have passed it should not appear on a subsequent record.

### `remap`

We are going to use a `remap` collection, which is a map from a target gauge `IntoTime` to an antichain of timestamps from the source gauge `FromTime`.
The easiest way for me to represent this is as a differential dataflow collection,
```rust
// G's timestamp should be `IntoTime`.
// The `from` values at any time should form an antichain.
remap: Collection<G, FromTime>
```
We can also think of this as a function, or as a hash map, but would need to figure out how to express progress through that stream as well.

The only constraint on `remap` is monotonicity: if `into1 <= into2` then `remap[into1] <= remap[into2]`, where the second inequality is the partial order on antichains.
Neither `FromTime` nor `IntoTime` need to be totally ordered.

### `reclocked`

From the `source` collection and the `remap` collection, we can define a new collection `reclocked` as:
    the collection that contains at `into` all messages from `source` whose gauge value is not greater than or equal to some element of `remap[into]`.

The `reclocked` collection is in the `IntoTime` gauge, and contains messages from `source` but no longer in the `FromTime` gauge (perhaps the gauge values can be preserved as data).
All source events at a given `FromTime` either all occur or all do not occur at each gauge value of `IntoTime`.

### Mechanics / implementation

One natural implementation of reclocking could determine the `remap` relation by observing simultaneously:
1. Forward progress in the source `FromTime`,
2. Some external ticker of progress through the target `IntoTime`.
Whenever `IntoTime` advances, we record the current values of `FromTime` as the new contents of `remap`.
The intended behavior is that the reclocked collection contains all data from gauge values that are known to be complete.

To produce the reclocked stream from the source stream and the `remap` collection,
1. We can start with the `remap` collection which is in the `IntoTime` gauge.
2. Given access to the source stream, for each "delta" in `remap` we can determine which `FromTime` values we must await from the source stream, and emit them.
3. The source stream's gauge is not helpful in dataflows based on `IntoTime`, and likely needs a non-stream representation (e.g. the capture/replay representation, an arrangement, or some other way to externalize progress information).

## Alternatives

I'm not aware of other designs that fit the same requirements.
We could of course remove any of the requirements.

Our current implementation in support of exactly-once Kafka sinks does something similar, though I believe "less intentionally".
I'm not sure what I mean by that, other than that this document would is meant to be prescriptive about what you can do, rather than descriptive about what does happen.

## Open questions

I think we still need to discuss fitness for use for the enumerated use cases.

There is not yet a prototype implementation, so there may be unforeseen issues.
For example, I haven't thought through either stream completing.

The use of antichains is potentially confusing for sources.
For example, Kakfa's (partition, offset) pairs would be frustrating with dynamically arriving parts if the part identifiers are not both ordered and starting from small identifiers.
Otherwise, recording "these parts have not yet started" is non-trivial.
One could use a different meaning of the antichain, for example "all records less than an antichain element", though this seems likely to be at odds with timely and differential.
