Note: this was written prior to the design doc process, so only loosely follows the template. It still has some use as an example, but ideally would contain more high-level overviews/descriptions of the changes to be made.

## Goals:

* A host configured with realtime consistency and Kafka sinks can crash or be manually restarted and 'pick up where it left off' with the same topic name.
* Minimal changes to consumers (ie. no requirement on CDCv2 in the sink topic)

## Non-goals:

* Supporting sinks other than Kafka. Eventually we will want to do this, but it would likely mean requiring CDCv2. Given the prevalence of Kafka, there's value in doing a non-CDCv2 version specific to that sink type.
* Multipartition sinks. We don't currently support them, this work continues not supporting them.
* BYO consistency is entirely out of scope.
* Reingesting our own sinks as a source.
* Minimizing state accumulation. O(n) growth of the catalog is acceptable for v1.
* Spinning up a clean host with no accumulated state and having it continue appending to an existing topic. For now, persistent local storage like a reused EBS volume will be required for host migrations.

## Description

### Mz changes:

1. Minted timestamps must match for all instantiations of a given source. Requires dusting off the dead code in timestamp.rs and changing sources to get their timestamps from the coord instead of locally.
2. Minted timestamps should be persisted and reused across restarts. Requires adding persistence, detection of persisted state, and reuse to timestamp.rs. Timestamper must also signal the sink that we're in reuse mode.
3. Writing to sinks should be idempotent. This is a small configuration change and needs to be done regardless.
4. Sink should update the consistency topic in lockstep with the result topic and only after a timestamp has been closed. Requires batching completed timestamps into a Kafka transaction.
5. On startup, sink should read the latest complete timestamp from the consistency topic and drop any writes coming through the dataflow until caught up.

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
