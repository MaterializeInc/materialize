# Reclocking

## Summary

"Reclocking" a stream of data is (for the purposes of this document) translating its events from one time domain to another time domain.

Many of our sources of data come with their own understanding of forward progress.
Often, these are incompatible with each other, and with native notions of time within Materialize.
For example,
    Kafka topics contain multiple "parts", each of which advances through "offsets".
    Postgres sources come from a log where entries have a "log sequence number" (LSN).
    File sources have a line number.
    Materialize CDC sources have explicit timestamps.

The source-specific "timestamps" of forward progress are useful, but they are not directly comparable.
This document describes one framework for converting between the timestamps including in to and potentially out of Materialize's system time.

## Goals

The primary goal of this document is to describe a formal way to translate collections that evolve with one gauge into collections that evolve with another gauge.
That formalism should have the properties that:
1. There is an explicit representation of the translation (rather than a behavioral description).
2. The translation should be sidecar metadata, rather than a rewriting of the source data.
3. The combination of durable source data and durable metadata should result in a durable reclocked stream.
4. The sidecar metadata should be compactable whenever the reclocked stream is compactable.

The formalism should support the goals of
A. Permitting the use of streams from one timeline in another timeline.
B. Describing durable state that can make sources like Kafka, Postgres, and files NONVOLATILE in the strong sense that their contents can be replayed exactly.
C. A BYO format for users who want to provide explicit timestamp information for sources that are presented without timestamps (or with timestamps from another time domain).
D. A way to describe in one time domain progress made through another time domain (e.g. reporting Kafka read progress in MZ system timestamps).

## Non-Goals

This document has no opinions on the physical representation of the reclocking data.
This document has no opinions on the implementation of the reclocking actions.
This document has no opinions on the policies of which streams should be reclocked into which domains.

## Description

Streams of data may come with timestamps attached to each record, and a mechanism for determining when timestamps are complete.
This information describes "progress" through a stream, and allows consumers of the stream to rely on the presence of certain contents.
Multiple events may share the same timestamp, indicating that they should occur at the same moment.
Messages may not be in timestamp order, but the stream should not claim that a time is complete until all messages with lesser timestamps have been received.

### `remap`

We are going to use a `remap` collection, which is a map from a target time domain `Into` to an antichain of timestamps from the source time domain `From`.
The easiest way for me to represent this is as a differential dataflow collection,
```rust
// G's timestamp should be `Into`.
remap: Collection<G, Antichain<From>>
```
We can also think of this as a function, or as a hash map, but would need to figure out how to express progress through that stream as well.

The only constraint on `remap` is monotonicity: if `into1 <= into2` then `remap[into1] <= remap[into2]`, where the second inequality is the partial order on antichains.
Neither `From` nor `Into` need to be totally ordered.

### `reclocked`

From the `source` collection and the `remap` collection, we can define a new collection `reclocked` as:
    the collection that contains at `into` all messages from `source` whose timestamp is not greater than or equal to some element of `remap[into]`.

The `reclocked` collection is in the `Into` time domain, and contains messages from `source` but no longer in the `From` time domain (perhaps the timestamps can be preserved as data).
All source events at a given `From` time either all occur or all do not occur at each time of `Into`.

### Mechanics / implementation

One natural implementation of reclocking could determine the `remap` relation by observing simultaneously:
1. Forward progress in the source timestamp `From`,
2. Some external ticker of progress through the target timestamp `Into`.
Whenever the `Into` timestamp advances, we record the current values of `From` as the new contents of `remap`.
The intended behavior is that the reclocked collection contains all data from times that are known to be complete.

To produce the reclocked stream from the source stream and the `remap` collection,
1. We can start with the `remap` collection which is in the correct time domain.
2. Given access to the source stream, for each "delta" in `remap` we can determine which times we must await from the source stream, and emit them.
3. The source stream's timestamps are not helpful in dataflows based on the `Into` timestamp, and likely need a non-stream representation (e.g. the capture/replay representation, an arrangement, or some other way to externalize progress information).

## Alternatives

I'm not aware of other designs that fit the same requirements.
We could of course remove any of the requirements.

Our current implementation does something similar, though I believe "less intentionally".
I'm not sure what I mean by that, other than that this document would is meant to be prescriptive about what you can do, rather than descriptive about what does happen.

## Open questions

I think we still need to discuss fitness for use for the enumerated use cases.

There is not yet a prototype implementation, so there may be unforeseen issues.
For example, I haven't thought through either stream completing.

The use of antichains is potentially confusing for sources.
For example, Kakfa's (partition, offset) pairs would be frustrating with dynamically arriving parts if the part identifiers are not both ordered and starting from small identifiers.
Otherwise, recording "these parts have not yet started" is non-trivial.
One could use a different meaning of the antichain, for example "all records less than an antichain element", though this seems likely to be at odds with timely and differential.
