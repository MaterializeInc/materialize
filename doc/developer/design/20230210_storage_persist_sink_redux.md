- Feature name: storage `persist_sink` upgrade
- Associated: <https://github.com/MaterializeInc/materialize/issues/17413>

# Summary
[summary]: #summary

This design document describes how the _storage_ `persist_sink` implementation should work going forward,
in particular, how it can be upgraded to utilize all the timely workers available in a replica.
This design is specifically chosen to consider the constraints we have for storage sources.

# Motivation
[motivation]: #motivation

Unblock the "Multipurpose Clusters" epic, by effectively utilizing large replicas
when writing source data to persist. This design is also chosen to be easy to reason about and
implement. An alternative (discussed below) is to attempt to merge the compute and storage `persist_sink`s,
which should happen eventually. In that sense, this design is not a _normative_ description of the storage
`persist_sink`, but rather, a pragmatic evolution.

# Explanation
[explanation]: #explanation

The design detailed in the [reference explanation](#reference-explanation) proposes changing the storage
`persist_sink` from operating on a single timely worker, to operating on all available timely workers. Like
the compute `persist_sink` (which is also described below), it writes persist batches of data from all the
workers, and then forwards references to those batches to a single worker that performs the `compare_and_append`.

# Reference explanation
[reference-explanation]: #reference-explanation


## Current state

Currently, there are 2 implementations for sinking a differential `Collection` to persist, both called `persist_sink`.

The compute implementation, described here: <https://www.notion.so/materialize/distributed-self-correcting-persist_sink-d3d59834ed9d47d397143c738e9d6c9d>
is used to persist `MATERIALIZED VIEW`. It writes data from all available timely workers, and is _self-correcting_, which means:
_it adjusts what the persist shard accumulates to if the persisted `Collection` has changed_. That is, if the
`Collection` produced by a compute dataflow is somehow _different_ in a different version of the code, this sink ensures that
the data in the persist shard is adjusted to reflect this. The compute `persist_sink` also expects to be run
from many cluster replicas, and therefore gracefully handles contention, at the cost of write amplification.

The storage implementation is much simpler. It never attempts to correct the underlying persist shard, instead only
writing data past that shard's upper, and only writes data from a single timely worker thread. This implementation
also irrecoverably fails if it detects contention writing to the persist shard.

## Proposal

Replace the storage `persist_sink` implementation with an implementation that maximizes throughput, by writing data
from all available timely workers. This implementation will be _similar to the compute one_, but will be
customized for the constraints storage has. These constraints are:

- Uses bounded-memory.
  - Storage `Collection`s can produce large amounts of data, over time, or within a single timestamp. We want to be
  able to persist `Collection`s of arbitrary sizes, without needing to vertically scale.
- Minimal overhead. While compute's `persist_sink` also has this constraint, it must also perform additional work
(mostly update consolidation) to perform self-correction.
  - Persisting a storage `Collection` should be bounded only by persist's write throughput.
- Functions correctly if the upstream source compacts data that occurs before the upper of the `Collection`'s persist
shard.
- Required to retry on contention.
  - Currently this means panicking, shutting down, and restarting, but in the future could be made more graceful.
  - Compute assumes contention meant the some other replica won the `compare_and_append`. In storage, there
  is never more than 1 replica persisting a collection, which must always attempt to make real progress.

The current, single-worker `persist_sink` operator will be replaced with a set of operators that look like this:

```
                               ,------------.
                               | source     |
                               | Collection |
                               +---+--------+
                               /   |
                              /    |
                             /     |
                            /      |
                           /       |
                          /        |
                         /         |
                        /          |
                       /     ,-+-----------------------.
                      /      | mint_batch_descriptions |
                     /       | one arbitrary worker    |
                    |        +-,--,--------+----+------+
                   ,----------´.-´         |     \
               _.-´ |       .-´            |      \
           _.-´     |    .-´               |       \
        .-´  .------+----|-------+---------|--------\-----.
       /    /            |       |         |         \     \
,--------------.   ,-----------------.     |     ,-----------------.
| write_batches|   |  write_batches  |     |     |  write_batches  |
| worker 0     |   | worker 1        |     |     | worker N        |
+-----+--------+   +-+---------------+     |     +--+--------------+
       \              \                    |        /
        `-.            `,                  |       /
           `-._          `-.               |      /
               `-._         `-.            |     /
                   `---------. `-.         |    /
                             +`---`---+-------------,
                             | append_batches       |
                             | one arbitrary worker |
                             +------+---------------+
```

The different operators work like this:

1. `mint_batch_descriptions` emits new batch descriptions whenever the frontier of source `Collection` advances.
A batch description is a pair of `(previous_upper, upper)` that tells write operators which updates to write and
in the end tells the append operator what frontiers to use for the `compare_and_append`. This is a single-worker operator.
2. `write_batches` writes the source `Collection` to persist. It does this by creating a persist `BatchBuilder` for each
timestamp, and writing every value that occurs at each timestamp to the corresponding `BatchBuilder`. This uses a bounded
amount of memory. This does not yet append the batches to the persist shard, the update are only uploaded/prepared to be
appended to a shard. We only emit finalized batches to the `append_batches` operator when that batch is contained within
a batch descriptions that we learned about from `mint_batch_descriptions`.
3. `append_batches` takes as input the minted batch descriptions and written batches. Whenever the frontiers sufficiently advance,
we take a batch description and all the batches that belong to it and append it to the persist shard.
This also updates the shared frontier information in `storage_state` to match the persist frontier.
This way, the controller eventually learns of the upper frontier of data written to the persist shard.
Note that failing to `compare_and_append` must be retried, as we current expect only 1 replica to be
running this sink operation for each source `Collection`.

### Notes on the implementation

- It is needlessly expensive to create a `BatchBuilder` for each timestamp.
  - A change to this implementation is to avoid spilling data to persist for timestamps
  whose values use a small amount of memory. These can be consolidated into large batches in `append_batches`,
  similar to how <https://github.com/MaterializeInc/materialize/pull/17526> works.
  - We do this in the current implementation, so doing it from many workers, instead of 1,
  which has strictly more throughput (though does cause additional `SET` and `GET` load on AWS S3).
  - Its undecided whether or not this should be required for the reference implementation.
- `append_batches` and `write_batches` will be required to track statistics to expose to users.
  - See <https://www.notion.so/materialize/User-facing-metrics-for-Sources-9603b61224a04a749969702e04662ed3> for more details


# Rollout
[rollout]: #rollout

## Testing and observability
[testing-and-observability]: #testing-and-observability

This new implementation can be tested entirely using the existing CI, nightly, and release-qualification pipelines.
However its important that the performance of the new implementation is carefully monitored as it is released into
staging and production. Because of this, we will gate it behind a feature flag.

## Lifecycle
[lifecycle]: #lifecycle

This new implementation will be feature-gated. The rollout will start in staging, which will be carefully monitored,
particularly:
- Unacceptable regressions in the S3 PUT rate for persist (see [Drawbacks](#drawbacks)).
- Regressions in S3 read throughput

These will also be monitored as we roll out to production. This feature gate can only be applied when restarting a
storage dataflow, and its undetermined whether or not we will support this without manually cycling `clusterd` pods.

# Drawbacks
[drawbacks]: #drawbacks

The primary drawback of this design, as alluded to above, is that it does not represent the final implementation we want the
storage `persist_sink` to have. Ideally, we would be able to use the exact same implementation between compute and storage, but
after a _large_ amount of brainstorming within the storage team, this is not feasible without significant engineering effort
(the technical details here are mentioned in [Conclusion and alternatives](#conclusion-and-alternatives)), which we are unable
to invest at this time.

An additional drawback is that the multi-worker persist sink can now write more, and smaller part files because we write those
part files from each worker instead of only one worker, for each timestamp. As mentioned above, the compute `persist_sink`
suffers from a similar problem but there is at least one proposed solution in
<https://github.com/MaterializeInc/materialize/pull/17526>. This will be carefully monitored as part of the
rollout of this design, which will help us determine if additional optimizations are required before we go to
production.

## Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

Some rejected alternatives are:

- Eliminate the storage `persist_sink` and replace with it with the compute one. Rejected for the following reasons:
  - Requires skipping self-correction on startup, which is deemed too complex to maintain.
  - No clear way to keep the self-correction mechanism while also using bounded amounts of memory.
    - A core design principle of storage is that the ingested collections ("sources") can be
    arbitrarily sized, and its memory usage should only scale with the size of the largest
    transaction. The `perist_sink` implementation must conform to this requirement.
    - Its possible we re-evaluate if the compute sink can use bounded memory in the future,
    particularly when we have out-of-band consolidation of batches.
  - Self-correction does additional work that uses compute resources, whereas storage wants to optimize for the
  raw performance of storage ingestion.
- Share more of the implementation with the compute `persist_sink`, in particular the `append_batches` operator. Its also
  possible that `mint_batch_descriptions` could be shared, but the `persist_collection` input would need to be fabricated.
  Rejected for the following reasons:
  - Making operators generic in these ways was deemed too onerous for the increase in code-sharing. The most plausibly-shared
  operator is `append_batches`, which we will not share in the initial implementation, as storage instruments additional touchpoints.
  - This can be revisited as an independent refactoring project after the initial implementation.
- Use an even simpler implementation, skipping the `mint_batch_descriptions` operators. Rejected for the following reasons:
  - We want to ensure that the storage and compute implementations are similar enough that we can reason about them as a unit.

We have chosen the above design because it 1. solves the stated requirements and 2. is feasibly implementable in a short time frame.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- Is it worth implementing other proposed optimizations (like only spilling to `BatchBuilder`s when enough memory has been used)
as part of the reference implementation, or can those be left for later?

# Future work
[future-work]: #future-work

The storage `persist_sink` should not be re-implemented in the future, unless we are merging it with the compute `persist_sink`.
Note that _the design proposed is an overt derivative of the compute `persist_sink`, and therefore, brings us closer to doing
this, as opposed to using an orthogonal technique_.
