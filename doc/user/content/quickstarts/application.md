---
title: "Build a real-time application"
description: "  "
menu:
    main:
        parent: quickstarts
weight: 20
aliases:
  - /demos/real-time-application
---

<!-- **tl;dr** Materialize can enable real-time monitoring within business
intelligence tools, and we have a [demo](#run-the-demo) showing you that it's
feasible. -->
**tl;dr** Materialize unleashes real-time powers for your applications. Process, serve, and display information quickly as a blink.


<!-- Traditionally, business intelligence tools rely on reading day-old answers from
data warehouses. Given the substantial size of the data they're required to
process, it makes sense that you wouldn't want to recompute the answer every
time someone wants to view it.

Materialize, though, offers a totally different paradigm for powering BI tools
that offers complex, real-time analysis over massive datasets. The fundamental
idea is that Materialize persists the results of your queries, and then
incrementally updates them as new data comes in. Unlike the traditional RDBMS
model, which doesn't rely on previously completed work, Materialize minimizes
the time required to provide fresh answers. -->

## Overview

An infrastructure working safe and healthy is critical. We, developers, know this very well. In other businesses, there are different vital infrastructures, such as mobile antennas (4G, 5G) in telecommunications companies. If there is an issue, then addressing and fixing it quickly is a must; otherwise, customers will complain or, even worse, churn.

Telecommunications companies use [antenna manufacturers’ key performance indicators (KPIs)](https://www.ericsson.com/en/reports-and-papers/white-papers/performance-verification-for-5g-nr-deployments) to run analytics. Let’s use these indicators as “performance” and use them to calculate different performance health statuses.

•	If the <strong> last-half-minute average performance</strong> is greater than 5, it is <span style='border-bottom:1px solid green;'>healthy</span>.

•	If it is more than 4.75 but less than 5, it is <span style='border-bottom:1px solid #FFBA00;'>semi-healthy</span>.

•	If it is less than 4.75, it is <span style='border-bottom:1px solid red;'>unhealthy</span>.

Every time the operational analytics detects an unhealthy case during a brief period, it will deploy a whole set of helper antennas in the area. Once the area improves and becomes healthy, these antennas are deactivated to save money.

Each antenna has a known fixed range capable of serving customers. Operations teams watch a map of green, yellow, or red circles (antenna’s range area and health status) to understand where the issues are.

A traditional approach like batch processing demands extra time to detect any issue, exposing the clients to a bad experience. A real-time application should be able to keep customers, businesses, and operational people happy.

## Conceptual Overview

### Sources

First, a source needs to spin up. It could be Postgres, Redpanda, Kafka, S3, or other alternatives. It will define how to store, insert, and process in Materialize the performance events and antenna’s position on the map.

{{< figure
    src="https://materialize.com/wp-content/uploads/2022/04/Inner-Frame.png"
    alt="Materialize sources"
>}}

### Parse

The source defines how to parse the events schema.

•	Postgres has tables with a defined schema.

•	Kafka/Redpanda can have topics without a schema.

For a quick start, let’s focus only on Postgres. If you want to know how it would look using Kafka without a schema, refer to (this repository)[https://github.com/MaterializeInc/demos/tree/main/antennas-kafka].

The first step while building an application using Postgres is making available the tables to consume from Materialize.
It’s done through a process known as Change Data Capture (CDC). As it refers to in our [integration guide](https://materialize.com/docs/integrations/cdc-postgres/), <i>“it allows you to track and propagate changes in a Postgres database to downstream consumers based on its Write-Ahead Log (WAL).”</i>

After setting up, Materialize will consume the current data and any future changes.
Speeding up development can be done through commands like [CREATE VIEWS](https://materialize.com/docs/sql/create-views/). It will create a view for each table on the Postgres source that Materialize is consuming.

### Aggregate

Asking the performance metric could be done running a query over the available data but why rather than asking Materialize to calculate the same information running the same frequent query, when Materialized View can compute and update the value incrementally with each new event? In the end, a simple lookup is enough to retrieve the results.

```
```

Aggregated information about the antenna’s performance is now ready to consume. The stack is ready to win back every customer and wants to react in a blink; Updates happen fast, and a suitable back-end needs to maintain the pace.

### Application Stack

A suitable back-end is whatever you are comfortable with, and even better if it has support for sockets, SSE, or any sort of streaming transport.  The idea is to use `TAIL` to distribute events as soon as they happen in a performant way.

GraphQL with sockets is one of the many options available. [Graphql-ws](https://github.com/enisdenjo/graphql-ws) is a framework that will leverage the transport and communication between the front-end application and the TAIL updates.

React front-end communicates using one of the most known clients for the framework, the [Apollo client](https://www.apollographql.com/docs/react/), with a custom [Link](https://github.com/MaterializeInc/demos/blob/main/antennas-postgres/frontend/src/link.ts) to support the subscriptions.

<!-- ### Dataset & load generator

For this demo, Materialize uses a custom benchmark called chBench that is really
just a concatenation of two well-known database benchmarking tools, TPC-C and
TPC-H.

[TPC-C](http://www.tpc.org/tpcc/detail.asp) is an industry-standard benchmark
for transactional workloads, meant to test a database's scalability.

[TPC-H](http://www.tpc.org/tpch/) is from the same group that developed TPC-C,
and is meant to test the capabilities of online analytic platforms by performing
complex analytic style queries. This includes large aggregations, many
groupings, and complex multi-way joins.

[CH-benCHmark](https://db.in.tum.de/research/projects/CHbenCHmark/?lang=en)
brings together a TPC-C-like dataset with TPC-H's analytical queries. This
is a great approximation for how many businesses perform OLAP queries over OLTP
data.

### Database (MySQL)

This demo relies on MySQL, which is a stable, well-supported platform with good
performance.

### Change Data Capture & Streaming (Debezium & Kafka)

As the chBench load generator writes data to your database, we need to propagate
those changes to Materialize so it can update the answers to your queries.

With MySQL, the easiest way to do this is through a change data capture (CDC)
tool, which can describe changes to your data. This demo relies on Debezium as
its CDC tool.

To ferry the CDC data to Materialize, we stream the data using Kafka. This
requires a suite of tools from Confluent, e.g. ZooKeeper, Confluent Schema
Registry, etc, which we've also included in our demo's Docker deployment.

### Materialize

Materialize ingests the CDC data from MySQL, representing the chBench load
generator's continued activity.

With that data available, you can perform complex queries, and Materialize
maintains their results, even as the underlying data changes. This means that
you can get answers to your queries in real time, rather than relying on day-old
answers from your data warehouse.

### BI Tool (Metabase)

Metabase is an open-source tool to create visualizations of SQL queries'
results, and then group them into dashboards. For instance, teams might use
Metabase to monitor geographic purchasing patterns from their stores.

In our demo, we'll use Metabase to visualize the results of some TPC-H-like
queries, and watch the visualizations update as quickly as Metabase allows.

In thinking of this deployment in terms of client-server relationships, this BI
tool represents a client, and Materialize represents a server.

### Diagram

Putting this all together, our deployment looks like this:

{{< figure
    src="/images/demos/bi_architecture_diagram.png"
    alt="Materialize deployment diagram with Metabase"
>}} -->

## Run the demo

### Preparing the environment

1. [Set up Docker and Docker compose](/integrations/docker), if you haven't
   already.

   **Note for macOS users:** Be sure to [increase Docker
   resources](/integrations/docker/#increase-docker-resources) to at least 2 CPUs
   and 8GB memory. Running Docker for Mac with less resources may cause the demo
   to fail.

1. Clone the Materialize demos repository:

    ```shell
    git clone https://github.com/MaterializeInc/demos.git
    ```

   You can also view the demo's code on
   [GitHub](https://github.com/MaterializeInc/demos/tree/main/chbench).

1. Download and start all of the components we've listed above by running:

   ```shell
   cd demos/chbench
   docker-compose up -d
   ```

   Note that downloading the Docker images necessary for the demo can take quite
   a bit of time (upwards of 10 minutes, even on fast connections).

   When the script exits successfully, all components from the diagram above are
   active in Docker containers, and the chBench client is rapidly pushing data
   and transactions to the MySQL database.

### Define sources & views

Now that our deployment is running (and looks like the diagram shown above), we
can get Materialize to read data from Kafka and define the views we want
Materialize to maintain for us.

1. Launch the `psql` shell against Materialize by running:

    ```shell
    docker-compose run cli
    ```

1. Within the CLI, ensure you have all of the necessary sources, which represent
   all of the tables from MySQL. It may take a minute or two for these sources
   to appear.

    ```sql
    SHOW SOURCES;
    ```

    ```nofmt
    debezium_tpcch_customer
    debezium_tpcch_district
    debezium_tpcch_item
    debezium_tpcch_nation
    debezium_tpcch_neworder
    debezium_tpcch_order
    debezium_tpcch_orderline
    debezium_tpcch_region
    debezium_tpcch_stock
    debezium_tpcch_supplier
    debezium_tpcch_warehouse
    ```

1. Create a straightforward view of the underlying data.
   <a name="define-query01"></a>

    ```sql
    CREATE MATERIALIZED VIEW query01 AS
        SELECT
            ol_number,
            sum(ol_quantity) as sum_qty,
            sum(ol_amount) as sum_amount,
            avg(ol_quantity) as avg_qty,
            avg(ol_amount) as avg_amount,
            count(*) as count_order
        FROM debezium_tpcch_orderline
        WHERE ol_delivery_d > date '1998-12-01'
        GROUP BY ol_number;
    ```

    This is used to repesent "Query 01" in chBench, which tracks statistics
    about the TPC-C `debezium_tpcch_orderline` table.

1. Check the results of the view:

    ```sql
    SELECT * FROM query01;
    ```

    If you run this query multiple times, you should see the results change,
    and the answers should come back pretty quickly. (How quickly depends on
    the speed of your computer, but sub-second responses are typical.)

1. Define another view for "Query 07", which involves a complex 11-way `JOIN`
   across 6 tables:
   <a name="define-query07"></a>

    ```sql
    CREATE MATERIALIZED VIEW query07 AS
        SELECT
            su_nationkey AS supp_nation,
            substr(c_state, 1, 1) AS cust_nation,
            extract('year' FROM o_entry_d) AS l_year,
            sum(ol_amount) AS revenue
        FROM
            debezium_tpcch_supplier,
            debezium_tpcch_stock,
            debezium_tpcch_orderline,
            debezium_tpcch_order,
            debezium_tpcch_customer,
            debezium_tpcch_nation AS n1,
            debezium_tpcch_nation AS n2
        WHERE
            ol_supply_w_id = s_w_id
            AND ol_i_id = s_i_id
            AND s_su_suppkey = su_suppkey
            AND ol_w_id = o_w_id
            AND ol_d_id = o_d_id
            AND ol_o_id = o_id
            AND c_id = o_c_id
            AND c_w_id = o_w_id
            AND c_d_id = o_d_id
            AND su_nationkey = n1.n_nationkey
            AND c_n_nationkey = n2.n_nationkey
        GROUP BY
            su_nationkey,
            substr(c_state, 1, 1),
            extract('year' FROM o_entry_d)
        ORDER BY su_nationkey, cust_nation, l_year;
    ```
    This query sums revenue (`ol_amount`) from the fast-changing
    `orderline` table and joins it with several other tables to show
    the total revenue between any two nations in a given year.

1. Check the results of this query:

    ```sql
    SELECT * FROM query07;
    ```

    It can take a few minutes to begin producing answers. Running
    the query while it is initializing will produce an empty set of
    results.

    Once initialized, re-running the query should show updated values in
    the `revenue` column and the response times should be quick!

### Set up Metabase

1. In a browser, navigate to `localhost:3030`.

1. Click **Let's get started**.

1. Complete the first set of fields asking for your email address. This
   information isn't crucial for anything but does have to be filled in.

1. On the **Add your data** page, fill in the following information:

    Field             | Enter...
    ----------------- | ----------------
    Database type     | **PostgreSQL**
    Name              | **tpcch**
    Host              | **materialized**
    Port              | **6875**
    Database name     | **materialize**
    Database username | **materialize**
    Database password | Leave empty.

1. Proceed past the screens until you reach your primary dashboard.

## Create dashboards

1. Click **Ask a question**.

1. Click **Native query**.

1. From **Select a database**, select **tpcch**.

1. In the query editor, enter:

    ```sql
    SELECT * FROM query01;
    ```

    Materialize relies on you already having created a materialized view with
    this name, which you did the Materialize CLI [a few steps
    back](#define-query01).

    In a production setting, you might want to let users find views' underlying
    queries. For example, you might store the underlying queries in a GitHub
    repository, and provide a link to the repository as a comment. Those with
    direct access to the `materialized` process through a SQL client can find
    the underlying queries using [`SHOW CREATE
    VIEW`](/sql/show-create-view).

1. Click the large **>** button.

    If you receive an error stating `One or more sources has no closed timestamps`, wait a few seconds, press enter in the query editor, and then
    repeat this process.

1. Once you see the results similar to those below, click **Save**, enter
   **query01** as the query's name, and then click **Save**.

1. When asked if you want to save this to a dashboard, click **Yes please!**.

1. Click **My personal collection**, and then click **Create a new dashboard**.

1. Enter **tpcch** as the **Name**, and then click **Create**.

1. Modify the size of the **query01** card, and then click **Save**.

1. Click **Auto-refresh**, and then select **1 minute.**

   60 seconds is the fastest refresh rate selectable in the UI, but if you
   copy the URL, open a new tab and edit the end of the url to change the
   `refresh=60` anchor to `refresh=1` you can force metabase to update
   every second.

{{< figure
    src="https://user-images.githubusercontent.com/11527560/107248709-8f339a00-6a00-11eb-81b5-beb01a95156c.gif"
    alt="Materialize real-time business intelligence dashboard in metabase"
>}}

## Recap

In this demo, we saw:

-   How to define sources and views within Materialize
-   How to query defined views in a BI tool
-   Materialize's ability to serve results for views with the fastest refresh
    rates the tool offers

## Details

### Typical operation

In this demo, you "played" the role of both infrastructure engineer and
business analyst; that is to say, you both deployed Materialize, as well
as ran queries within your BI tool, Metabase.

To give you a sense of what this experience would look like if you were two
different people, let's highlight what the expected workflow is.

-   **Business analysts** perform _ad hoc_ queries in Materialize to determine
    what queries are meaningful to them.
-   **Business analysts** then provide those queries to **DBAs**.
-   **DBAs** turn those queries into materialized views, and then provide the
    **business analysts** with the views' names, e.g. `some_view`. **Business
    analysts** then query those views from within their BI tools, e.g. `SELECT * FROM some_view`.

Of course, it's possible to change this workflow to make it more self-service,
etc. but this is a reasonable workflow.

## Related pages

-   [Replace a microservice with a SQL query](/quickstarts/microservice/)
-   [Run SQL on streaming logs](/quickstarts/log-parsing/)
-   [`CREATE SOURCE`](/sql/create-source)
