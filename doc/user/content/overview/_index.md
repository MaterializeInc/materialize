---
title: "What Is Materialize?"
description: "Learn more about Materialize"
aliases:
  - /overview/what-is-materialize/
menu:
  main:
    identifier: overview
    weight: 10
    name: "Overview"
---

Materialize is a streaming SQL materialized view engine.

If that jargon-filled sentence doesn't answer all of your questions, we'll cover some important conceptual topics related to Materialize:

- What it helps teams accomplish
- What it actually is
- How it works

## What does Materialize do?

Materialize lets you ask questions about your data, and then get low-latency,
correct answers, even as the underlying data changes.

Why not just use your database's built-in functionality to perform these same
computations? Because your database often acts as if it's never been asked that
question before, which means it can take a _long_ time to come up with an
answer, each and every time you pose the query.

Materialize instead keeps the results of the queries and incrementally updates
them as new data comes in. So, rather than recalculating the answer each time
it's asked, Materialize continually updates the answer and gives you the
answer's current state from memory.

Importantly, Materialize supports incrementally updating a much broader set of
views than is common in traditional databases (e.g. views over multi-way joins
with complex aggregations), and can do incremental updates in the presence of
arbitrary inserts, updates, and deletes in the input streams.

## Why should I use Materialize?

If you perform any OLAP queries over relational data and want to reduce the time
it takes to refresh answers to common queries, Materialize can make that happen.

For a sense of scale, it can take queries that most teams would run once-per-day
and instead provide sub-second or single-digit second answers.

## Materialize's design objective

Materialize lets teams very quickly get answers to questions they routinely ask.

It's useful to think of these questions as the kind of queries you might use to power business intelligence dashboards. You might have a ton of data that comes in all day, and each team in your org wants to answer some questions using that data.

Traditionally, to get answers, some batch processing system performs a query over all of the data every night. This approach makes intuitive sense: the working set is often larger than any machine's memory and you need to process all of it. These kind of computations are therefore relatively expensive, so it's not feasible to execute them on the fly. Instead, you want to compute the answer once and then cache it until you derive a new answer.

This once-a-day update cycle works, but it's inopportune to continually recompute an answer that you once knew. It's also slow and doesn't allow teams to nimbly make decisions.

In contrast to the traditional approach, Materialize let teams continually update the answers to their queries as new data comes in. This means that you no longer need to rely on day-old data to inform your decisions. You can now simply look at a real-time answer.

## Understanding the jargon

"A streaming SQL materialized view engine" is difficult to wade through, so let's take the terms piece-by-piece.

### SQL & views

One way to consider the answer to one of the aforementioned analytical queries is as a "view" of your data. There is some defined query that you want to execute, that results in giving you an answer that contains some subsets and computations of your data.

In the language of **SQL**, a **view** is a pre-defined query and its results; every time you query a view, the underlying query is executed and you get the results back.

However, continually re-executing queries is expensive, so another implementation of views instead stores the view's results as a physical table, i.e. it caches them. Because this involves storing the results, they're referred to as **materialized views**.

Materialized views are often maintained with some periodicity and don't reflect the underlying data with total fidelity. This essentially makes them snapshots of the results, meaning any updates that have happened since the snapshot was generated will not be reflected in the materialized view. It's easy to liken this to the batch processing strategy discussed above; you occasionally perform some expensive computation and cache the results.

However, it's also possible that if you were to describe the changes that happened in the underlying database that you would be able to reflect those changes in materialized views without needing to periodically recompute the answer to the query in total. Instead, each successful operation could be passed to an engine that could then parse the operation and, if necessary, update the materialized view. This is known as an **incrementally updated materialized view**.

### Streaming

If we understand the implementation of incrementally updated materialized views as "the need to see a stream of updates that are occurring to the underlying database", it could be modeled simply as "change data capture" (CDC). Using CDC, whenever a change occurs to the underlying database, a structure describing the change is propagated to some destination.

One such destination you can use for CDC is a **stream** like Apache Kafka. As your database changes, you can describe those changes in a published feed, and services that care about those changes can subscribe to it.

### Engine

To maintain materialized views using a stream of data, you need an engine to subscribe to the CDC streams, and then perform the computations for and maintenance of the materialized view.

One approach to this is using a dataflow **engine**, which is a set of computations that work over streams of data; they take a stream as their input, transform it, and output their own stream. If you think of dataflows in similar terms as functional programming, you can see that they can complete arbitrarily complex tasks.

By modeling a SQL query as a dataflow, you can take in a CDC stream, apply a set of transformations to it, and then observe the results as final operator's output set.

### Putting it all together

To put all of this together, Materialize lets you:

- Describe queries you want to know the answer to as **materialized views**, which is a concept implemented in **SQL**.
- Use CDC **streams** to feed our dataflow **engine** to create and maintain the answers to those queries.

Ultimately, this lets you refresh answers to your queries very quickly because Materialize continually updates their answers as new data streams in.

## How does Materialize work?

While we've covered the high-level details above, this section gives some of the more specific details about Materialize itself.

Materialize ingests streams of data from external systems like Kafka and PostgreSQL that you
declare as "sources". These sources monitor changes in the upstream data and
automatically pull new records into your Materialize instance. You can interact
with this data in Materialize as if it were in a SQL table.

Using these sources, you can define "views". Views represent those queries to which
you continually want an up-to-date answer. Your view definitions are transformed by
our query engine to create [Differential dataflows](https://github.com/frankmcsherry/differential-dataflow).

A dataflow is a set of connected operators that each
both consume and produce streams of data, creating a network of computation that
can respond to streams of input data from sources. Differential dataflows
are special, though, in that they can easily perform incremental updates at each
step of the dataflow.

As data streams in from your sources, your dataflows determine which data is
relevant and update their result sets only if they need to, for example, when there are new
rows or the values used in dataflow computations have changed.

When you query one of your views, Materialize can simply return the dataflow's
result set from memory, which should always be faster than computing the answer
from scratch.

## Materialize vs. other methodologies

### Batch processing

Batch processing relies on perform large, expensive computations infrequently. This means that you can never achieve real-time results, and it often does not build upon its prior computations.

In contrast, Materialize continually updates queries as data comes in, which means it performs small, efficient computations constantly. This enables real-time results and builds upon all of your prior computations.

### Materialized views in relational databases

To maintain materialized views, most RDBMSes occasionally re-run the view's underlying query. This results in a potential impact to the database's performance while updating the view, as well as data that is only infrequently up-to-date.

## Learn more

- [Architecture overview](/overview/architecture) to understand Materialize's internal architecture
- [API overview](/overview/key-concepts) to understand what Materialize's SQL API expresses
