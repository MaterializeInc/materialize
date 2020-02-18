---
title: "Materialize Docs"
disable_toc: true
disable_list: true
weight: 1
---

Materialize is a streaming data warehouse. Materialize accepts input data from a
variety of streaming sources (e.g. Kafka) and files (e.g. CSVs), and lets you
query them using the PostgreSQL dialect of SQL.

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

## Learn more

- [**What is Materialize?**](./overview/what-is-materialize) to learn more about what Materialize does and how it works.
- [**Architecture documentation**](./overview/architecture) for a deeper dive into the `materialized` binary's components and deployment.
- [**Getting started guide**](./get-started) to get hands-on experience with Materialize.
