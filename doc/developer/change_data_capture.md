
Materialize CDC ingestion.

## Core informational requirements

The Materialize computational layer takes as inputs streams of "updates", which are things that look like

```
    (data, time, diff)
```

where `data` is something like a row in a table, `time` is a logical timestamp with some semantic meaning (e.g. "ms since 1970" or "transaction number"), and `diff` is an integer that tells us how many copies of `data` we should add or remove (often +1 or -1).

In addition, the streams need to supply statements about the advancing lower bound on `time` values that might be seen in the future; these statements are what unblock computation and allow Materialize to determine that it can produce a "correct answer" in its output for a particular time.

If you are able to supply this information for your CDC sources, Materialize will provide exactly the correct answers for each logical timestamp. These answers are provided in the same format: a stream of updates and statements about the advancing times that will no longer be updated.

## Materialize can improvise for you

Materialize is able to interpret and in some cases make up this information for CDC sources that do not provide it. For example:

### Debezium

The Debezium CDC format contains `before` and `after` records. These can be interpreted as a subtraction (the `before`) and an insertion (the `after`). We can automatically translate

```
    {
        before: <data1>,
        after:  <data2>,
    }
```

into updates

```
    (data1, ?time?, -1)
    (data2, ?time?, +1)
```

This holds if either of `before` or `after` are null, which correspond respectively to insertions and deletions (rather than updates). This translation works correctly as long as the inputs are correct, but requires further care if input updates are duplicated or lost (e.g. due to casual compaction of the CDC log).

We have some initial work performing deduplication in the ingestion itself, but this only works if the CDC format makes deduplication possible. For example, it is generally difficult to deduplicate an otherwise unadorned "insert row <foo>" into a relation without a primary key; it could be a duplicate insertion, or the intent could be to have a second row. Ideal CDC formats would allow deduplication with minimal overhead (e.g. not require maintaining a second copy of the table).

### Upserts

One popular format provides keyed events with optional values,

```
    (key, time, opt_val)
```

where the intent is that each event should update the value associated with the key: if not yet present it inserts the value, if present it updates the value, and if `opt_val` is empty it removes the key and value.

Materialize is able to absorb updates in this representation, but must perform additional work to do so. In essence, we need to maintain a copy of the key-value mapping, so that we can correctly interpret the events and determine if they are insertions, updates, or removals, and which records are removed in the latter two cases.

Materialize is able to push filters and projections through these sources, but generally must maintain some materialization. This cost may come in exchange for savings upstream, and it is not unreasonable to make the OLTP source of truth more efficient, but it is a cost.

### Real-time timestamping

If updates do not arrive with semantically meaningful timestamps, Materialize can assign them using a system-local monotonic clock, persist these decisions locally, and close out times as the clock advances.

This provides an "interactive, real-time" experience, and introduces consistency within the system (updates are ingested at specific times, and all queries reflect consistent views based on these times; the specific times may not be semantically meaningful externally). These sources and views can interact with other more specifically timestamped inputs as long as the share the same reckoning (currently: milliseconds since 1970).

## Summary: Must haves, nice to haves, don't cares

Materialize is able to function against generally unstructured streams of events, but to cause it to do specific things certain properties are required of the inputs.

### Transaction boundaries

If Materialize needs to produce outputs that are consistent with transaction boundaries, it needs to assign timestamps to updates that align with these transactions. To do this, Materialize both needs to know transaction identifiers for each update, and guarantees about when it will no longer see a transaction identifier (so that it can advance the timestamps and unblock computation on those identifiers).

Transaction identifiers and completeness statements are must haves for transactional output, and don't cares if the output doesn't need to be transactional.

### Deduplicated updates

Materialize can deduplicate updates for CDC formats that make this possible. It becomes much more efficient if this is very easily done; sequence numbers or other monotonically increasing characteristics mean that the ingestion can just drop duplicates. In general, Materialize can accept duplicate updates and upsert style updates, though there is a cost associated with this.

Deduplicatable updates are a must have for Materialize to produce correct output, and it is increasingly nice to have clear and efficient protocols for deduplication.

### Primary Keys

Materialize can ingest data for arbitrary collections, without requiring primary key information. This information can be helpful for tasks like deduplication and upsert ingestion, and it does provide our query planner with additional plan options.

Primary keys are a nice to have, unless they are required for correctly ingesting duplicated or upsert input data.
