# Jepsen Test Plan

When Materialize is ready, we'd like to test it with Jepsen. What properties
should we try to verify? Where should they hold? How should we measure them?

From conversations with Frank, it sounds like Materialize is aiming to offer
serializability, and possibly up to strong session serializability or strong
serializability. Niftily, this property should hold not only within Materialize
but also together with the upstream systems which feed Materialize data. We
should expect that the union of the histories against (e.g.) Postgres *and*
Materialize is still serializable. If Materialize winds up building
strong-session or strong serializability, we can verify those real-time
properties as well.

The natural choice for testing a transactional system like this with Jepsen is
to use [Elle](https://github.com/jepsen-io/elle), and most likely the
list-append workload. This workload is efficient, supports the strongest forms
of anomaly inference, and comes with helpful visualizations. It should also, I
think, be easy to implement on top of Materialize.

## Writes

Unlike most databases Jepsen tests, where all writes go to the database itself,
Materialize has (at least) two distinct write paths. One is values written via
user DML sent through the adapter layer. The other is values written to some
upstream system (e.g. Postgres, Kafka, etc), which then make their way into
Materialize. We should test both of these paths, via, e.g. `[:append [:pg 1] 5]`
to append 5 to key 1 in Postgres, and `[append [:mz 2] 6]` to append 6 to key
`2` in Materialize itself. If time permits, we can add additional write paths
for Kafka and other systems---each helps us test a different form of ingestion.

Postgres already supports a natural form of list-append: we'll store each
key/value pair in a separate row, and use `UPDATE lists SET value =
CONCAT(value, ",5") WHERE id = 3` to append 5 to key 3. This comma-separate
string approach worked well in the [previous Jepsen Postgres
tests](https://jepsen.io/analyses/postgresql-12.3), and should work here too.
We can develop alternate embeddings (e.g. across columns, across tables) later
on.

This approach should, I think, allow us to append values to both an external
Postgres server and *also* Materialize directly, since Materialize speaks the
Postgres wire protocol. This makes Postgres the natural first step for
implementing writes.

For Kafka writes (if we wind up doing them), we'll treat each topic-partition
as a separate list, and append values using `producer.send`.

## Reads

All reads will go through the Materialize adapter layer. As a simple first
approach, we'll look at the key being read and use that to determine which
table in Materialize to read from: `[:r [:pg 1] nil]` tells us to query
Materialize's local copy of the Postgres table `lists` using something like
`SELECT * FROM pg-lists WHERE id = 1`.

To start with, we'll use very simple sources which just maintain a literal copy
of some table in Postgres or queue in Kafka. Later, we can introduce more
complex materialized views, so long as those views preserve queryability.

## Transactions

We can't express transactions which cross Postgres and Materialize, or Kafka
and Postgres, etc--we'll disallow those at the generator level. We *can*
express transactions constrained to a single system though--so we can do a
transaction which constrains itself entirely to Postgres, Kafka, or
Materialize. A few constraints on these transactions:

For Materialize transactions, we must ensure that all reads occur before all
writes. This is a Materialize design limitation.

For Postgres and Kafka, I think we actually do want to do at least some reads.
The reason is that we might imagine a case where Postgres decides on one
serialization order, and for whatever reason Materialize decides on a different
order, and both orders are consistent with the write order. If we don't read
from Postgres, we won't detect that inconsistency. I don't think this is a huge
deal given our entire-list-in-one-row embedding, but it might matter more if we
start doing alternate embeddings, like splitting lists across multiple
tables/rows.

## Analysis

The neat thing about Materialize is that it ought to provide serializability
across *all* of these transactions, which means we shouldn't have to do any
special processing around deriving separate orders for Postgres, Kafka,
Materialize, etc. We can, I think, shovel the entire history of transactions
across all n services right into Elle, and it'll tell us whether or not strict
serializability actually held!
