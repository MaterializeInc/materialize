---
title: "Volatility"
description: "Understand volatility in Materialize."
menu:
  main:
    parent: advanced
    weight: 5
---

Materialize strives to be a correct, deterministic system. But because
Materialize does not itself store data, it can only provide correctness and
determinism if the sources it connects to provide the same guarantee. In
particular, a given source must be capable of producing the same data
repeatedly. In other words, if Materialize restarts, it must be able to replay
the source from the beginning of time and receive exactly the same data.

We call a source that cannot uphold this guarantee a *volatile* source. Many
common sources of streaming data are volatile. For example, [Kafka](/sql/create-source/kafka) streams not configured with infinite retention are volatile, because (by
default) data is only retained for 7 days. If Materialize restarts after
reading a Kafka stream for longer than this retention period, it might be unable to recover part of the data.

While it is possible to connect to volatile sources in Materialize, the system
internally tracks the volatility.

## Rules

The following sections describe Materialize's rules for determining the
volatility of objects in the system.

{{< warning >}}
These rules will evolve in future versions of Materialize.
{{< /warning >}}

### Sources

At present, the following source types are always considered volatile:

  * [PubNub sources](/sql/create-source/text-pubnub/)

The following source types are always considered to be of unknown volatility:

  * [Kafka sources](/sql/create-source/avro-kafka/)

In the future, Materialize will let you configure whether a given source is considered volatile or nonvolatile, as their volatility depends
on their configuration. A common way to introduce volatility is, for example, to configure
a retention policy on a Kafka topic used in a Kafka source.

### Views, indexes, and sinks

The volatility of a view, index, or sink is determined by applying each of
the following rules in turn:

  1. If the object depends on at least one volatile source, it is volatile.
  2. If the object depends on at least one source of unknown volatility, it has
     unknown volatility.
  3. Otherwise the object is nonvolatile.

An important implication of these rules is that a view that has no dependencies
(e.g., `CREATE VIEW v AS SELECT 1`) is nonvolatile.

### Tables

Tables are always volatile because they do not currently retain state between
restarts.
