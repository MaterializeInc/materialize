# Dynamic resource sharing for sources and sinks

## Summary

In Materialize today, each source and sink instance is independent. For example:

  * Each instance of a Kafka source creates its own Kafka consumer.
  * Each instance of a file source creates its own file handle and filesystem
    event listener.
  * Each instance of a `TAIL` sink performs its own consolidation of the input
    stream.

The result is a system that can correctly backfill data when a source is
instantiated multiple times, but is inefficient in the steady state. In an ideal
system, once two instances of a source or sink had caught up to the same point,
they would share resources. To continue the example from above:

  * Multiple instances of a Kafka source would share the underlying Kafka
    consumer ([#3791]).
  * Multiple instances of a file source would share the underlying file handler
    and filesystem event listener.
  * Multiple instances of a `TAIL` sink would share the underlying consolidation
    operator.

The proposal here is to teach Materialize to perform this resource sharing
automatically and dynamically as the catalog evolves.

## Goals

There are three goals here:

  * To raise awareness of the general problem of steady-state inefficiency.
  * To build consensus that dynamic resource sharing is the general solution to
    the problem.
  * To commit to using dynamic resource sharing to permit unmaterialized
    PostgreSQL sources.

The last goal is in fact the motivating goal. Taken in isolation, supporting
unmaterialized PostgreSQL sources does not clearly justify the complexities of
dynamic resource sharing. But taken altogether, I think it becomes clear that
there is a general problem here that warrants a general solution.

## Non-goals

The design here does not even begin to address the implementation of dynamic
resource sharing. Suffice it to say that the implementations will be nontrivial.
We'll need separate implementation design docs for each source and sink type.

## Description

### Background

Consider the following DDL statements in Materialize today:

```sql
CREATE SOURCE kafka_src FROM KAFKA BROKER '...' TOPIC 'top' FORMAT AVRO ... ;
CREATE MATERIALIZED VIEW view1 AS SELECT col1 FROM kafka_src;
CREATE MATERIALIZED VIEW view2 AS SELECT col2 FROM kafka_src;
```

`view1` and `view2` will *each* instantiate `kafka_src`, resulting in
two separate Kafka consumers that read from the `top` topic:

```
       kafka_src/1                    kafka_src/2
  +---------------------+        +---------------------+
  | librdkafka consumer |        | librdkafka consumer |
  +---------------------+        +---------------------+
  |    Avro decoding    |        |    Avro decoding    |
  +---------------------+        +---------------------+
           |                               |
         view1                           view2
```

In the steady state, this is quite wasteful. We're streaming the records from
`top` over the network twice and decoding the Avro in each record twice. We'd
prefer to run just one Kafka consumer that feeds both views:

```
               +---------------------+
               | librdkafka consumer |
               +---------------------+
               |    Avro decoding    |
               +---------------------+
             /                        \
            /                           \
      kafka_src/1                     kafka_src/2
          |                                |
        view1                            view2
```

### Today's workaround

If you know the complete set of columns you care about ahead of time, you can
coax an efficient implementation out of Materialize today with an intermediate
materialized view:

```sql
CREATE SOURCE kafka_src FROM KAFKA BROKER '...' TOPIC 'top' FORMAT AVRO ... ;
CREATE MATERIALIZED VIEW kafka_view AS SELECT col1, col2 FROM kafka_src;
CREATE VIEW view1 AS SELECT col1 FROM kafka_view;
CREATE VIEW view2 AS SELECT col2 FROM kafka_view;
```

This creates only one librdkafka consumer by ensuring that `kafka_src` is
only instantiated once.

```
       kafka_src/1
  +---------------------+
  | librdkafka consumer |
  +---------------------+
  |    Avro decoding    |
  +---------------------+
            |
        kafka_view
         /      \
      view1    view2
```

This approach has two notable deficiencies.

First, it requires that you commit ahead of time to the columns you care about.
What if, down the road, you realize you need a `view3` that extracts `col3`? You
have two unappealing options:

  1. Delete `kafka_view`, `view1`, and `view2`. Recreate `kafka_view` with
    `col3` as well, then create `view1`, `view2`, and `view3` atop the new
    `kafka_view`.

     This is both annoying to type and will interrupt the availability of
     `view1` and `view2`.

  2. Create another instantiation of `kafka_src` by materializing `view3`
     atop `kafka_src` directly.

     This is ergonomic but results in a suboptimal steady state.

Second, this workaround is not fully generalizable. Consider the following
source and views:

```sql
CREATE SOURCE kafka_src FROM KAFKA BROKER '...' TOPIC 'top' FORMAT AVRO ... ;
CREATE MATERIALIZED VIEW v1 AS SELECT sum(col1) FROM src;
CREATE MATERIALIZED VIEW v2 AS SELECT col2 FROM src;
```

There is no suitable intermediate materialized view that can be extracted
here. (There are some gross hacks, but none that we'd feel comfortable
recommending to customers.)

### Dynamic resource sharing

With dynamic resource sharing, you get all the ergonomics of creating views
as you please, but a system that converges on the same efficience that you'd
get if you'd pre-committed to those views. The key insight is that we can
create a temporary backfill consumer when a new instance is instantiated:

```
          steady-state primary                    temporary backfill
         +---------------------+                 +---------------------+
         | librdkafka consumer |                 | librdkafka consumer |
         +---------------------+                 +---------------------+
         |    Avro decoding    |                 |    Avro decoding    |
         +---------------------+                 +---------------------+
       /                        \                          |
      /                           \                        |
kafka_src/1                     kafka_src/2           kafka_src/3
    |                                |                     |
  view1                            view2                 view3
```

Once that backfill consumer has caught up to the steady-state primary
consumer—i.e., once the consumer offsets are identical—we can cut `kafka_src/3`
over to the primary consumer, and jettison the backfill consumer:

```
          steady-state primary
         +---------------------+
         | librdkafka consumer |
         +---------------------+
         |    Avro decoding    |
         +---------------------+
       /                        \------------------------+
      /                           \                       \
kafka_src/1                     kafka_src/2           kafka_src/3
    |                                |                     |
  view1                            view2                 view3
```

Note that this example is provided for illustrative purposes only. The
actual implementation might choose to jettison the existing consumer and
switch to the new consumer instead. See [this discussion][jettison-discussion]
for details.

### Unmaterialized PostgreSQL sources

The MVP for PostgreSQL sources will not support unmaterialized sources. It's not
fundamentally impossible to have multiple instances of a PostgreSQL source, but
each instance of a PostgreSQL source is prohibitively expensive. Forcing
PostgreSQL sources to be materialized is an easy way to ensure that only one
instance of a PostgreSQL source is ever created.

Roughly speaking, PostgreSQL's replication protocol works per database, while
a PostgreSQL source in Materialize corresponds to just one table in that
database. If you were to instantiate a PostgreSQL source twice in Materialize,
you'd wind up streaming two separate copies of the entire database to
Materialize, which is certain to be surprising to users.

With dynamic resource sharing, creating a new instance of a PostgreSQL source
would instead backfill the missing data for just the one table, then cut over to
the already existing replication stream. This will be cheap enough that we'll
be comfortable allowing PostgreSQL sources to be unmaterialized.

### Discussion

The major downside of this approach is the implementation complexity. Cutting
source instances over from a backfill resource to the steady-state resource will
require a lot of fiddly logic and testing.

My contention is that this complexity is well worth it for the user experience
it provides. Expecting users to predict exactly which fields they'll need from a
source is unrealistic. Asking them to recreate an entire stack of views, as
described in the [workaround](#todays-workaround) is acceptable in the short
term but unfriendly in the long term. But letting users interactively create the
sources and views they want, while converging on the optimal implementation
nonetheless? That's the magic experience we strive for.

Also, the lessons learned here will be directly applicable to our future plans
for multi-query optimization. Dynamically sharing resources in sources is a
simplified version of dynamically sharing resources (i.e., arrangements) between
views.

## Alternatives

### Bulk creation

We'd previously talked about using SQL transactions to permit bulk creation
of set of sources and views, like so:

```sql
BEGIN;
CREATE SOURCE kafka_src FROM KAFKA BROKER '...' TOPIC 'top' FORMAT AVRO ...
CREATE MATERIALIZED VIEW view1 AS SELECT col1 FROM kafka_src
CREATE MATERIALIZED VIEW view2 AS SELECT col2 FROM kafka_src
COMMIT;
```

When the transaction commits, Materialize could look holistically at the
objects within, and realize that only one new source instance is necessary.

But this turns out not to be any more flexible than the [current
workaround](#todays-workaround). You need to be willing to declare all of your
sources and views in one shot, and that's not always feasible.

We might, however, want to support this feature for savvy users though as a
minor optimization for dynamic resource sharing. Dynamic resource sharing only
guarantees that the system converges to an optimal state. Creating sources one
by one will result in a period of inefficiency, as several backfill resources
are created and then merged together. But if the user is willing to provide a
batch of create statements simultaneously, we can create the optimal number of
instances from the start.

### Schema changes

In theory, arbitrary schema changes on live views would avoid the need for
dynamic resource sharing in some cases. In practice we are at least several
years away from supporting this, but I mention it here for posterity.

Suppose you've set things up using the [workaround](#todays-workaround)
described above:


```
CREATE SOURCE kafka_src FROM KAFKA BROKER '...' TOPIC 'top' FORMAT AVRO ... ;
CREATE MATERIALIZED VIEW kafka_view AS SELECT col1, col2 FROM kafka_src;
CREATE VIEW view1 AS SELECT col1 FROM kafka_view;
CREATE VIEW view2 AS SELECT col2 FROM kafka_view;
```

Now suppose you want to add a new view that references `col3` without
reinstantiating the source. You could just replace the definition of
`kafka_view` with a query that includes `col3`:

```
CREATE OR REPLACE VIEW kafka_view AS SELECT col1, col2, col3 FROM kafka_src;
CREATE VIEW view3 AS SELECT col3 FROM kafka_view;
```

[#3791]: https://github.com/MaterializeInc/database-issues/issues/1182
[jettison-discussion]: https://github.com/MaterializeInc/materialize/pull/6450/files#r612735779
