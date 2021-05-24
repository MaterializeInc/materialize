Note: this was written prior to the design doc process, so only loosely follows the template. It still has some use as an example, but ideally would contain more high-level overviews/descriptions of the changes to be made.

## Summary:

Today, Kafka sinks are a major user pain point because any Kafka sinks that transitively depend on realtime (RT) sources write to a new Kafka topic after every restart. This shifts the burden of "detect when Materialize crashed, discover the new topic, and update all downstream services" to the user. This behavior exists largely because RT sources do not remember what timestamps were assigned to previously ingested data.

## Goals:

* Kafka sinks that depend on realtime (RT) sources can crash or be manually restarted and 'pick up where they left off'
* Minimal changes to consumers (ie. no requirement on CDCv2 in the sink topic)
* The assigments of timestamps to RT source data (aka the timestamp bindings) are 'replayable' in the sense that 'assuming the upstream data still exists, we will assign the same data the same, or greater timestamp on subsequent reads'.
* RT source timestamp bindings use a bounded amount of persistent memory when compaction is enabled.

## Non-goals:

* Supporting sinks other than Kafka. Eventually we will want to do this, but it would likely mean requiring CDCv2. Given the prevalence of Kafka, there's value in doing a non-CDCv2 version specific to that sink type.
* Multipartition sinks. We don't currently support them, this work continues not supporting them.
* Reingesting our own sinks as a source.
* Spinning up a clean host with no accumulated state and having it continue appending to an existing topic. For now, persistent local storage like a reused EBS volume will be required for host migrations.

## Description

### Mz changes (high level):

1. Minted timestamps must match for all instantiations of a given RT source. This change is potentially mandatory, but we decided to make this part of the exactly-once sinks (EOS) work because:
   a. There wasn't a stable identifier for source instances.
   b. Having separate timestamp bindings across source instances is another UX wart that leads to confusing outputs.
2. Minted timestamps should be persisted and reused across restarts.
3. Sinks can now only write data at times that we know how to replay after a restart, or in other words, those times whose bindings are known to be durably persisted. Similarly, timestamp bindings cannot be compacted for a source until all sinks reading from that source have written data at that timestamp.
4. Writing to sinks should be idempotent. This is a small configuration change and needs to be done regardless.
5. Sink should update the consistency topic in lockstep with the result topic and only after a timestamp has been closed. Requires batching completed timestamps into a Kafka transaction.
6. On startup, sink should read the latest complete timestamp from the consistency topic and drop any writes coming through the dataflow until caught up.

### Mz changes (but with even more detail!)

1. Minting timestamps consistently across all instances of a RT source is likely the hardest part of this project. Currently, RT sources:
   a. Don't communicate any `upper` or `since` information back to the Coordinator.
   b. Handle their timestamping logic in source operator local state. Each source operator, when invoked, checks the last time it updated the RT timestamp, and if that update happened before `now() - timestamp_frequency` it updates the RT timestamp. All records it receives from now until the end of the current invocation get assigned that timestamp. Rinse and repeat.

This protocol is simple, and requires that each source instance hold a constant amount of data (the current RT timestamp) in memory. Unfortunately, it also means that queries like:

```sql

# Create RT Kafka source
CREATE SOURCE foo from KAFKA BROKER ...

# Create two source instances
CREATE MATERIALIZED VIEW test1 AS SELECT * FROM foo;
CREATE MATERIALIZED VIEW test2 AS SELECT * FROM foo;

SELECT * FROM test1 EXCEPT ALL SELECT * FROM test2;
```

can return data, when logically they shouldn't.

We will change this behavior and instead force all instances consuming from a given `(source_id, partition_id)` to read from the same worker, and then keep state about "what data have beeen assigned to what timestamps" in worker-local state, rather than operator local state.

The worker will track, for each partition, a list of timestamp bindings, that are conceptually a mapping from `timestamp` to intervals `[start_offset, end_offset)`. When a source operator needs to timestamp a record for which there is no existing binding, it will propose a new binding from `current_timestamp` -> `[start_offset, end_offset')`. Periodically, the dataflow worker will finalize proposed bindings, and update the current timestamp and introduce a new timestamp binding for `new_timestamp -> [greatest_read_offset, greatest_read_offset)` that source operators can then propose new additions to.

Since all the source operators are also invoked by the same worker, only one thing can happen at a time and there are no race conditions to worry about.

Concretely, every time a source operator is invoked, it needs to:
 - Check the worker's timestamp bindings for the next potential offset that data might appear at, so that the operator can respect the timestamp assignments of its peers
 - Downgrade its capability based on the lowest available timestamp across all of the partitions that operator has been assigned to. If the operator has not been assigned to any timestamps it still needs to downgrade its capability to the `current_timestamp`. We cannot close the source operator in this case because the worker may be assigned to a new partition in the future.
 - Read from its assigned partitions if any, and assign incoming records the appropriate timestamps. If an appropriate binding does not exist the operator needs to extend the timestamp binding for the current timestamp.
- If the operator minted new timestamp bindings it needs to commit them back to worker-local state before exiting, so that those bindings can be shared with its peers.

Whew. This is a complex (at least for me) protocol that involves some performance changes (now every time we need to timestamp a record we may need to binary search a list of timestamp bindings) to source ingestion code. The only alternative approach which made sense to me for this was to be a Kafka read replica, and write down `(data, timestamp)` for all incoming data, and have only one reader read the topic, and have every other reader read the written down state. This approach was scrapped because of the dependence on source persistence, which is another large project and currently a work in progress.

Once we do the above, we also need a mechanism to compact the list of timestamp bindings in memory, as otherwise we risk running out of memory over time. We can do that rebinding all intervals assigned to a timestamp <= `compaction_frontier` to the `compaction_frontier`. At this stage, we also changed the dataflow worker to communicate source timestamp binding `upper` information back to the `Coordinator` so that the `Coordinator` can direct the dataflow worker on what frontier to compact up to based on the `logical_compaction_window`. After that, the `Coordinator` needs to also be modified to be aware of each source's `upper` and `since` frontiers and to disallow queries outside of the interval `[since, upper)` the way it does for indexes.

2. We chose to persist timestamp binding information in a table in SQLite mostly for convenience because SQLite is already used as a storage layer for the catalog. Dataflow workers will send newly minted timestamp bindings to the `Coordinator`, which will persist them, and maintain a `durability_frontier` and send `durability_frontier` updates back to all the dataflow workers. Periodically, the `Coordinator` will also compact the persisted bindings that are at times less than or equal to the current `since` frontier.

This approach is kind of like a "speculative execution" approach because updates can flow through the dataflow layer at times that have not yet been persisted to disk. We chose this approach rather than having the dataflow worker itself persist timestamps before downgrading capabilities because that puts the latency of persisting on the fast path to returning results, even when there are no sinks, and SQLite doesn't love multithreaded writes (this is a less good reason). The downside of this approach is that now we need to communicate a `durability_frontier` in addition to the standard `input_frontier` for operators that care about persistence.

3. On restart, exactly-once sinks resume the dataflow `as_of` the maximum timestamp fully written out to the Kafka topic (the write frontier). This means that all timestamps > the sink write frontier cannot be compacted away. Otherwise, the sink may not write to some of those timestamps. We can achieve this by having each sink publish its write frontier back to the `Coordinator` and having the `Coordinator` hold back the `since` frontier for a source until all of its dependent sinks have advanced. The `Coordinator` already does something very similar with index `since`s and transactions.

### Customer changes:

This will require customers' kafka consumers to be in read-committed mode. This is the default on many clients (such as all those based on rdkafka), but not for primary main Java client.

## Alternatives

The best alternative is a similar scheme but using CDCv2 to remove the need for Kafka transactions. This is likely the best route when supporting other sinks that don't offer transactional support, but it does require us building out a small CDCv2 client in a few languages for customer ease of use. While a good option and something we will probably do in the future, given Kafka's prevalence it's worthwhile doing something specific for that sink type that imposes minimal format restrictions.

Most other alternatives are merely refinements or iterative improvements of this proposal.

## Testing

Specific areas that will need careful testing:
* Coordination between the timestamper and sink: the timestamper must be in charge of deciding whether or not we reuse an existing topic. If the timestamper has no persisted state but the topic exists and the sink is in reuse mode, then we'll end up writing bogus data. This should instead either be an error on startup or result in a fallback to the nonce write path.
* Need to understand interaction with Kafka source topic compression
* Kafka transactions will incur some write amplification and should be sized 'as big as possible without incurring unnecessary latency.' That plus the fact that sinking timestamps only once completed will result in more buffering and less precise control over batching semantics means that we will need to revisit sink tuning.
