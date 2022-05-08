---
title: "Architecture Overview"
description: "Understand Materialize's architecture."
menu:
  main:
    parent: advanced
    weight: 2
---

Everything in Materialize is currently handled by the `materialized` process
(pronounced _materialize-dee_; the "`d`" is for daemon), which interacts with
the outside world by interfacing with:

- **SQL shells** for interacting with clients, including defining sources,
  creating views, and querying data.
- **Sources** to ingest data, i.e. writes. We'll focus on streaming sources like
  Kafka, though Materialize also supports file sources.

## Diagrams

{{<
    figure src="/images/architecture_deployment.png"
    alt="Materialize deployment diagram"
    width="700"
>}}

_Above: Materialize deployed with multiple Kafka feeds as sources._

_Below: Zooming in on Materialize's internal structure in the above deployment._

{{<
    figure src="/images/architecture_internals.png"
    alt="Materialize internal diagram"
    width="700"
>}}

## SQL shell: interacting with clients

Right now, Materialize provides its interactive interface through `psql` running
locally on a client machine; this uses the PostgreSQL wire protocol (`pgwire`)
to communicate with `materialized`.

Because this is a SQL shell, Materialize lets you interact with your server
through SQL statements sent over `pgwire` to an internal `queue`, where they are
dequeued by a `sql` thread that parses the statement.

Broadly, there are three classes of statements in Materialize:

- **Creating sources** to ingest data from an external system, which is how data gets
  inserted into Materialize
- **Reading data** from sources
- **Creating views** to maintain the output of some query

### Creating sources

When Materialize receives a `CREATE SOURCE...` statement, it connects to some
external system to read data. You can find more information about how that works in the
[Sources](#sources-ingesting-data) section.

### Reading data

As with any SQL API, you read data from Materialize using `SELECT` statements. When
the `sql` thread parses a `SELECT` statement, it generates a
plan. Plans in Materialize are dataflows which can be executed by Differential.
The plan gets passed to the `dataflow` package, which works as the glue between
Materialize and its internal Differential engine.

Differential then passes this new dataflow to all of its workers, which begin
processing. Once Differential determines that the computation is complete, the
results are passed back to the client, and the dataflow is terminated.

Unfortunately, if the user passes the same query to Materialize again, it must
repeat the entire process––creating a new dataflow, waiting for its execution,
etc. The inefficiency of this is actually Materialize's _raison d'être_, and
leads us to the thing you actually want to do with the software: creating views.

### Creating views

If you know that you are routinely interested in knowing the answer to a
specific query (_how many widgets were sold in Oklahoma today?_), you can do
something much smarter than repeatedly ask Materialize to tabulate the answer
from a blank slate. Instead, you can create a materialized view of the query, which
Materialize will persist and continually keep up to date.

When users define views (that is, `CREATE MATERIALIZED VIEW some_view AS
SELECT...`), the internal `SELECT` statement is parsed––just as it is for ad hoc
queries––but instead of only executing a single time, the generated dataflow
persists. Then, as data comes in from Kafka, Differential workers collaborate to
maintain the dataflow and its attendant view.

To read data from views (as opposed to ad hoc queries), users target the view
with `SELECT * FROM some_view`; from here, Materialize can simply return the
result from the already-up-to-date view. No substantive processing necessary.

**Reading data vs. creating views**

The difference between simply reading data and creating a
view is how long the generated dataflow persists.

- When you perform an ad hoc `SELECT`, the dataflow only sticks around
  long enough to generate an answer once before being terminated.
- For views, the dataflows persist indefinitely and are kept up to date. This
  means you can get an answer an indefinite number of times from the generated
  dataflow. Though getting these answers isn't "free" because of the incremental
  work required to maintain the dataflow, it is dramatically faster to perform a
  read on this data at any given time than it is to create an answer from
  scratch.

In some cases, when the queries are relatively simple, Materialize can avoid creating a new dataflow and instead serves the result from an
existing dataflow.

## Sources: Ingesting data

For Materialize to ingest data, it must read it from a source or a table. Sources come in two varieties:

- Streaming sources, like Kafka
- File sources, like `.csv` or generic log files

File sources are more straightforward, so we'll focus on streaming sources.

When using a streaming source, Materialize subscribes to Kafka topics and
monitors the stream for data it should ingest.

As this data streams in, all Differential workers receive updates and determine
which––if any––of their dataflows should process this new data. This works
because each Differential worker determines the partitions it's responsible for;
the isolation that this self-election process provides prevents contention. Phrased
another way, you don't have to worry about two workers both trying to process
the same piece of data.

The actual processing of Differential workers maintaining materialized views is
also very interesting and one day we hope to explain it to you here. In the
meantime, more curious readers can [take the first step towards enlightenment
themselves](https://timelydataflow.github.io/differential-dataflow/).

Implicit in this design are a few key points:

- State is all in totally volatile memory; if `materialized` dies, so does
  all of the data.
- Streaming sources must receive all of their data from the stream itself; there
  is no way to "seed" a streaming source with static data. However, you can
  union streaming and file sources in views, which accomplishes a similar
  outcome.

## Learn more

Check out:

- [Key Concepts](/overview/key-concepts/) to better understand Materialize's API
- [Get started](/get-started) to try out Materialize

[1]:
https://paper.dropbox.com/doc/Materialize-Product--AbHSqqXlN5YNKHiYEXm3EKyNAg-eMbfh2QTOCPrU7drExDCm
[Naiad paper]: http://sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf
[Timely Dataflow]: https://github.com/TimelyDataflow/timely-dataflow
[Differential Dataflow]: https://github.com/TimelyDataflow/differential-dataflow
