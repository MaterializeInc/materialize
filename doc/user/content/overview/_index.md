---
title: "What is Materialize?"
description: "Learn more about Materialize"
disable_list: true
aliases:
  - /overview/what-is-materialize/
menu:
  main:
    parent: overview
    weight: 5
---

Materialize is a **streaming database** that incrementally updates query results as it
receives new data. Unlike a traditional database, Materialize continually
performs complex operations and computes dataflow operations as they
stream in from your data sources.

## Why should you use Materialize?

Teams responsible for processing data understand the challenges
associated with traditional databases. The need for immediate answers to business-essential questions requires a significant investment in data resources and can be time/cost
prohibitive at scale.

Materialize can solve data challenges in your organization in several ways.

### Efficient dataflow management

If your team performs OLAP queries over relational data,
Materialize can reduce the time it takes to receive your query results.

Traditionally, teams use a batch processing system to query large data
sets every night. These computations are resource expensive and time consuming
meaning you cannot run queries on demand. The once-a-day cycle caches the
query result until the next run.

Materialize continually updates the results of your queries as it receives new
data. You no longer need to rely on day-old data and have a real time answer to
your queries.

### SQL API management

Materialize allows you to interact with your data through the built-in SQL API.
You can manage your data and perform administrative tasks in Materialize with 
the SQL queries your team already uses.

### PostgreSQL wire compatibility 

Materialize supports PostgreSQL wire-compatibility by default. PostgreSQL
wire-compatibility allows Materialize to communicate with PostgreSQL databases
and tools without third-party integrations.

## How does Materialize work?

Materialize is a SQL streaming engine that manages materialized views. This section defines
these terms and explains what Materialize does.

### What is a SQL view?

In SQL, a **view** is a pre-defined query and its results. Every time you query
a view, SQL executes your query and returns your results. Views contain your
results and compute your data as you define.

Because executing queries is resource expensive, a **materialized view** stores
the results in a table. Materialized views are typically executed on a schedule
and create a snapshot of the data at runtime, which cannot detect changes until
the next scheduled run.

**Incrementally updated materialized views** can detect changes to your data,
pass that data to an engine, parse the operation, and update your materialized
views. Incremental updates do not take the same resource expenses as a full
query operation and allow you to quickly receive materialized views based on the
most up-to-date information.

### What is a streaming engine?

Incrementally updated materialized views require a stream of data in the
underlying database. The **stream** is a destination or source of data, like
Apache Kafka. Those changes flow from a published feed and other services can
consume that data. 

To maintain materialized views from a stream of data, you need an engine to
subscribe to the stream, compute, and maintain the materialized view. The
**dataflow engine** in Materialize is a set of computations that keep your data
up to date automatically.

## What is the Materialized workflow?

The streaming materialized view engine described above allows you to:

- Describe queries as **materialized views**, which is a concept implemented in
**SQL**.
- Use change data capture **streams** to feed a dataflow **engine** to create
and maintain the answers to those queries.

The basic stages of the Materialize workflow are:

* Ingesting data
* Querying data and creating views
* Incremental updates
* Data output

### Ingesting data

Materialize relies on data from outside sources to perform the complex queries
you need. You can connect Materialize to external data sources like
Kafka, Redpanda, and Confluent.

The first step in the Materialize workflow is to create a secure connection to
a {{< glossary_tooltip text="source" term_id="source" >}}.
SQL syntax is the primary user interface for Materialize so you
need to use a `CREATE CONNECTION` statement to your specified data sources. To
perform SQL queries in Materialize, log in to your Materialize cloud account
with a `psql` client.

Below is an example of the `CREATE` statement using Kafka:

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER '{yourKafkaBrokerIP:Port}',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```

After you create the connection, the `CREATE SOURCE` statement provisions the
dedicated resources to process data from your Kafka connection. Your source is
captured in **clusters** which are resource isolated tenants within Materialize
where all data operations take place. The example below uses the `default`
cluster available in every environment

```sql
CREATE SOURCE kafka_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  WITH (SIZE = '3xsmall');
```

With a connection and a data source, Materialize will process your incoming data
and transform queries you define. The next step in the workflow is creating a
query.

### Creating queries and views

Materialize can perform complex queries on your data including joins,
subqueries, and data aggregation.

To determine what queries to create in Materialize, consider how you use your
query results and what operational questions you want to answer with that data.

After you determine what data you need from your sources, you can create a
materialized view from your query. The example below creates a materialized view
of a `winning_bids` view and selects columns based on high bids and
sorts sequentially:

```sql
CREATE MATERIALIZED VIEW winning_bids AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

### Incremental updates

Materialize continually checks your sources for new data and updates the
materialized views you create.

Materialize creates dataflows of your query output. As the Materialize engine
receives new data from sources, Materialize compares the changed data and
operation type to determine what and how to update your data. Materialized
views persist in durable storage and reduces the operational burden on your
resources when accessing query results.

Timely Dataflow and Differential Dataflow are the foundation of this
internal process. For more information on Timely and Differential
Dataflows, review the documentation and other references.

### Emitting data

After the Materialize engine computes and processes data and changes from a
data source, you need a way to access those results that will also process
incremental updates as your data changes.

A **sink** allows Materialize to stream results to an outside system. Like a
source, you need to create a connection to your data recipient with the a new
`CREATE CONNECTION` statement.

A sink requires the same properties as an source, with one important
consideration. Although Materialize processes data from outside sources
continually with little resource impact, systems receiving the data may not
have the same resource freedom. The `SIZE` parameter determines the amount
of CPU and memory available to the sink process in Materialize and can reduce
bottleneck in your receiving systems.

```sql
CREATE SINK json_sink
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_json_topic')
  FORMAT JSON
  WITH (SIZE = '3xsmall');
```

~> Some statements are truncated for clarity and formatting. Review the documentation for
Envelopes and Formats for more information.

## Learn more

Now that you understand what Materialize is and how it can help your
organization, review the following resources to learn more:

- [Key concepts](/overview/key-concepts)
- [Get started](/get-started)
