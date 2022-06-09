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

**tl;dr** Materialize unleashes real-time powers for your applications. Process, serve, and display information quickly as a blink.

{{< figure
    src="https://user-images.githubusercontent.com/11491779/166932582-e5a9fd47-e397-4419-b221-e8f38c6f06f5.mp4"
    alt="Materialize real-time application"
>}}

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

First, a [source](https://materialize.com/docs/sql/create-source/) needs to spin up. It could be Postgres, Redpanda, Kafka, S3, or other alternatives. It will define how to store, insert, and process in Materialize the performance events and antenna’s position on the map.

{{< figure
    src="https://materialize.com/wp-content/uploads/2022/04/Inner-Frame.png"
    alt="Materialize sources"
>}}

### Parse

The source defines how to parse the events schema.

•	Postgres has tables with a defined schema.

•	Kafka/Redpanda can have topics with or without a schema.

For a quick start, let’s focus only on Postgres. If you want to know how it would look using Kafka without a schema, refer to [this repository](https://github.com/MaterializeInc/demos/tree/main/antennas-kafka).

The first step while building an application using Postgres is making available the tables to consume from Materialize.
It’s done through a process known as Change Data Capture (CDC). As it refers to in the [integration guide](https://materialize.com/docs/integrations/cdc-postgres/), <i>“it allows you to track and propagate changes in a Postgres database to downstream consumers based on its Write-Ahead Log (WAL).”</i>

From Materialize standpoint it requires create a source:

```sql
    CREATE MATERIALIZED SOURCE IF NOT EXISTS antennas_publication_source
    FROM POSTGRES
    CONNECTION 'host=postgres port=5432 user=materialize password=materialize dbname=postgres'
    PUBLICATION 'antennas_publication_source';
```

After setting up, Materialize will consume the current data and any future changes.
Speeding up development can be done through commands like [CREATE VIEWS](https://materialize.com/docs/sql/create-views/). It will create a view for each table on the Postgres source that Materialize is consuming.

```sql
    -- Materialized views is also an option
    CREATE MATERIALIZED VIEWS FROM SOURCE antennas_publication_source;
```

Otherwise, it is possible to filter the rows for each table using the `oid` in the `antennas_publication_source`.

### Aggregate

The antenna's health and performance metrics could be asked by running a query over the available data in the source. But why, rather than calculating the same information running the same query when a materialized view can compute and update the value incrementally with each new event? In the end, a simple lookup over the materialized view is enough and the fastest way to retrieve the results.

```sql
    CREATE MATERIALIZED VIEW IF NOT EXISTS last_half_minute_performance_per_antenna AS
    SELECT A.antenna_id, A.geojson, AVG(AP.performance) as performance
    -- Join
    FROM antennas A JOIN antennas_performance AP ON (A.antenna_id = AP.antenna_id)
    -- Filter
    WHERE ((CAST(EXTRACT( epoch from AP.updated_at) AS NUMERIC) * 1000) + 30000) > mz_logical_timestamp()
    -- Aggregate
    GROUP BY A.antenna_id, A.geojson;
```

Aggregated information in the last half minute is available to consume in the materialized view `last_half_minute_performance_per_antenna`. The stack can now react in a blink and win back every affected customer; Updates happen fast, and a suitable application needs to maintain the pace.

### Application Stack

A suitable back-end is whatever you are comfortable with, and even better if it has support for sockets, SSE, or any sort of streaming transport.  The idea is to use a [`TAIL`](https://materialize.com/docs/sql/tail/) subscription to distribute events as soon as they happen in a performant way and keep the stack reacting as a chain of events.

GraphQL with sockets is one of the many options available. [Graphql-ws](https://github.com/enisdenjo/graphql-ws) is a framework that will leverage the transport and communication between the front-end application and the `TAIL` updates.

The React front-end communicates using one of the most known clients for the framework, the [Apollo client](https://www.apollographql.com/docs/react/), with a custom [Link](https://github.com/MaterializeInc/demos/blob/main/antennas-postgres/frontend/src/link.ts) to support the subscriptions.

A microservice communicates to the GraphQL back-end using `graphql-ws` client with the NodeJS `ws` socket library.

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
   [GitHub](https://github.com/MaterializeInc/demos/tree/main/antennas-postgres).

1. Download and start all of the components we've listed above by running:

   ```shell
   cd demos/antennas-postgres
   docker-compose up -d
   ```

   Note that downloading the Docker images necessary for the demo can take quite
   a bit of time (upwards of 10 minutes, even on fast connections).

### Check the environment

Now all the components should be up and running.

1. Launch the `psql` shell against Materialize by running:

    ```shell
    psql postgresql://materialize:materialize@localhost:6875/materialize
    ```

1. Within the CLI, check how data flows for an antenna using `TAIL`

    ```sql
    COPY ( TAIL ( SELECT * FROM last_half_minute_performance_per_antenna WHERE antenna_id = 1 )) TO STDOUT;
    ```

1. Check the most unhealthy antenna

    ```sql
        SELECT *
        FROM ...
    ```

1. Turn the query into a Materialized View:

    ```sql
        CREATE MATERIALIZED VIEW most_unhealthy AS
        SELECT * FROM query01;
    ```

1. Check the results:

    ```sql
    SELECT * FROM most_unhealthy;
    ```

## Check how fast it reacts

1. Launch the `psql` shell against the Postgres source by running:

    ```shell
        psql postgresql://postgres:pg_password@localhost:5432/postgres
    ```

1. Check all the available antennas

    ```sql
    SELECT * FROM query01;
    ```

1. Delete the most unhealthy antenna

    ```sql
    DELETE FROM antennas WHERE antenna_id = 4;
    ```

1. Check in the front how it dissapeared

### Check the infrastructure

1. Head to `localhost:3001`

1. Check memory utilization

1. Check consumption

[Grafana picture]

## Recap

In this demo, we saw:

-   How to define sources and views within Materialize
-   How to query and tail Materialized Views
-   Materialize's ability to process and serve results for applications

## Related pages

-   [Replace a microservice with a SQL query](/quickstarts/microservice/)
-   [Run SQL on streaming logs](/quickstarts/log-parsing/)
-   [`CREATE SOURCE`](/sql/create-source)
