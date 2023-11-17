# Deterministic Sink Progress Topics

-   Associated:
    -   [panic: some element of the Sink as_of frontier is too far advanced for our output-gating time... #18628](https://github.com/MaterializeInc/materialize/issues/18628)

## The Problem

We have seen instances where a broker to which we are writing a connection's
sink's progress topic indicates that it successfully committed a write. Wowever,
when the sink later restarts, we do not see the most recent values we've
written, but instead some earlier progress value.

In the best case of this occurring, the sink simply panics because the persist
handle's since is beyond the latest progress value we read. (In these cases, we
have seen that restarting ends up returning a "better value.)

However, we cannot rule out receiving a "stale" progress value that is beyond
the persist handle's since, in which can we would duplicate values written to
the sink.

## Success Criteria

I do not yet have a reliable reproduction of this issue, and we only see it in
CI sporadically. I suggest that merging the proposed design be considered
"successful" since it should prevent the issue of duplicating data from
occurring.

We cannot reasonably guarantee the behavior of any broker, so this defensive
posture should be enough.

## Out of Scope

-   Preventing sinks from panicking/re-rerendering during startup

## Solution Proposal

To improve the likelihood of reading the most recent value for a key, we can
read values from the progress topic until we read a value at a higher offset
than the last progress message.

Without adding something like a remap shard to sinks (which would let us durably
record the progress topic's offset whenever we commit new values), the simplest
means to do this is writing a new value to the progress topic. If we read the
value that we wrote, we can guarantee that the progress value we read with the
highest offset must have been the most recent value.

If we do not read the value we wrote, we can panic the sink knowing that we
cannot guarantee that we read the most recent progress key and risk duplicating
data in the topic.

We must always be prepared to handle the situation where the most recent value
is not returned because, at least according to [Redpanda’s official Jepsen
report](https://redpanda.com/blog/redpanda-official-jepsen-report-and-analysis):

> ...consumer.poll() in Kafka and Redpanda is allowed to fall arbitrarily
> far behind producers, it was not possible to tell whether these missing
> messages were permanently lost or simply delayed

### Parallelism

If we have multiple sinks writing to the progress topic, each can use the same
sentinel key, and can write a hash that it retains in memory as the value--this
ensures that each sink is looking only for its own sentinel value. We do not
expect Kafka topics to have a compaction window short enough to cause a problem
here.

## Minimal Viable Prototype

See code changes alongside this commit

## Alternatives

### Watermarks

We could perform a similar operation without necessarily reading and comparing
the values, and only examining the topic's offsets, e.g.

1. Get topic high-water mark.
2. Write a value.
3. Get topic high-water mark.

We could then read until we got the first message beyond the high-water mark.
However, if e.g. the higher water marks were stale when we queried them
initially, it's unclear how far into the topic we need to read to ensure we get
the last value we wrote.

This also only saves us a nominal amount of work--decoding the payloads is
something we do only during startup.

## Open questions
