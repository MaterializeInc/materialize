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

We are going to use a `remap` collection, which is a map from a target gauge `IntoTime` to values from the source gauge `FromTime` that must themselves form an antichain.
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

## Practical Addendum

The goal of reclocking is to translate potentially incomparable sources into a common reckoning.

One primary *motivation* is to allow us to record consistent moments between the input data and persistent data we might record ourselves.
Reclocking provides the basis by which we can recover from a failure, read durable `remap` information and persistent collection data, and resume operation at input offsets that allow us to continue correct operation.

Let's detail this for a simple case, where the input data are independent records.
We will reclock input data, durably recording associations between `FromTime` and `IntoTime` in `remap`.
Additionally, we will likely record a collection of the data itself, also in a collection that changes as a function of `IntoTime`.
Imagine we ensure that we commit an output collection `IntoTime` only after that time is committed to `remap`, containing exactly those records up through its `FromTime` at the same `IntoTime`.
At any moment, say just after a crash, we can choose the greatest `IntoTime` available in the output collection, and resume processing from the `FromTime` present in `remap` at that `IntoTime`.

This is an example of how `remap`, durably recorded, introduces the basis for consistent recovery.

Let's detail this for a more complicated case, where the input data are independently data and transaction metadata, the latter describing which data are in a transaction.
We will reclock both of the inputs, although there are now multiple offsets across independent topics.
The `remap` collection must contain all of this information: `FromTime` for each involved input.
We would like to record `remap` and an output collection containing at each time the data up through the offsets of the reclocking collection at that time.
However, there is additional state that we must record to recover correctly: the data and transaction metadata that are not yet complete.

We can instead produce *two* output collections:
    * A TEMP collection containing at each `IntoTime` the data and transaction records up through `remap`'s `FromTime` at this `IntoTime`, excluding any records that form a complete transaction.
    * An output collection, containing at each `IntoTime` the data from transactions that are complete on records up through `remap`'s `FromTime` at this `IntoTime`.

These *two* collections are sufficient to summarize at each `IntoTime` all of the records up through `remap`'s `FromTime` at this `IntoTime`.
However, unlike the simpler case, the summary is not exactly the output collection, but the output collection plus some side information; "state" that we must hold on to.

The above approach actually fails to provide certain appealing end-to-end consistency properties we might want of "timestamp assignment" (I intentionally avoided "reclocking").
Above, a transaction is available at the first `IntoTime` for which it and its constituent data are available through `remap`'s `FromTime`.
However, the transaction may occur strictly after another transaction that is not yet complete.
We may want to change the definition of the two output collections, to respect the transaction order, to be:
    * A TEMP collection containing at each `IntoTime` the data and transaction records up through `remap`'s `FromTime` for this `IntoTime`, excluding records accepted in to the output.
    * An output collection, containing at each `IntoTime` the data from transactions for which all less-or-equal transactions are complete on records up through `remap`'s `FromTime` for this `IntoTime`.
Notice how we restricted the emission of transactions to await all prior transactions completing.
This will result in a larger TEMP collection, but an association of timestamps to transactions that respects the input order.

Our recipe for "practical reclocking" is therefore:
1. Maintain a durable collection `remap` containing `FromTime` at each `IntoTime`.
2. For each `IntoTime` that is durable in `remap`, summarize all input records up through `remap`'s `FromTime` at that `IntoTime`.
    a. The summary should contain an output collection of published records, varying as a function of `IntoTime`.
    b. The summary may optionally contain auxiliary information ("state"), varying as a funtion of `IntoTime`.

The summary should have the property that for any `remap`, all `it` in `IntoTime` produce identical summaries when restarted with their inputs at `remap[it]`, and their summary at `it`.
