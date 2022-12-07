---
title: "Build a real-time application"
description: "  "
draft: true
menu:
    main:
        parent: quickstarts
weight: 20
aliases:
  - /demos/real-time-application
---

{{< quickstarts-tldr >}}
With the power of materialized views and subscriptions (`TAIL`), you can build performant real-time applications without ever thinking of cache invalidation, and using common tech in the web development stack, like GraphQL.
{{</ quickstarts-tldr >}}

[//]: # "TODO(morsapaes) Add introduction motivating the use of Materialize to build real-time applications."

## Overview

{{< figure
    src="https://user-images.githubusercontent.com/11491779/155920578-7984244a-6382-4628-a87b-00e1f6ad1acd.png"
    alt="Materialize real-time application"
>}}


In this quickstart, we'll

An infrastructure working safe and healthy is critical. We, developers, know this very well. In other businesses, there are different vital infrastructures, such as mobile antennas (4G, 5G) in telecommunications companies. If there is an issue, then addressing and fixing it quickly is a must; otherwise, customers will complain or, even worse, churn.

Telecommunications companies monitor  [Key Performance Indicators (KPIs)](https://www.ericsson.com/en/reports-and-papers/white-papers/performance-verification-for-5g-nr-deployments) to run analytics. Let's use these indicators as "performance" and use them to calculate different performance health statuses over an antenna.

To assess the performance health of each antenna, we'll use the following scale:

*   If the <strong> last-half-minute average performance</strong> is greater than 5, it is <span style='border-bottom:1px solid green;'>healthy</span>.

*   If it is more than 4.75 but less than 5, it is <span style='border-bottom:1px solid #FFBA00;'>semi-healthy</span>.

*   If it is less than 4.75, it is <span style='border-bottom:1px solid red;'>unhealthy</span>.

Whenever an unhealthy antenna is detected, the team will deploy a whole set of helper antennas in the area. Once the area improves and becomes healthy, these antennas are deactivated to save money.

Each antenna has a known fixed range capable of serving customers. The antenna's range and health status is visualized on a map of green, yellow, and red circles.

A traditional approach like batch processing demands extra time to detect any issue, exposing clients to a bad experience. A real-time application will keep customers, businesses, and operational people happy.



In this quickstart, you will learn how to:

- Handle Change Data Capture (CDC) data from PostgreSQL
- Use `TAIL` to subscribe to changes in a materialized view
- Use [Grafana](https://grafana.com/) to monitor a Materialize deployment


In this demo, we saw:

-   How to define sources and views within Materialize
-   How to query and tail Materialized Views
-   How to monitor Materialize using Grafana
-   Materialize's ability to process and serve results for applications


## Setup

1. The quickstart is containerized and should run end-to-end with no modifications. To get started, make sure you have [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.

    **Note:** We recommend running Docker with at least **2 CPUs** and **8GB of memory**, so double check your resource preferences before moving on.

1. Clone the Materialize demos GitHub repository and checkout the `lts` branch:

    ```shell
    git clone https://github.com/MaterializeInc/demos.git

    git checkout lts
    ```

   , and navigate to the [`antennas-postgres`](https://github.com/MaterializeInc/demos/tree/lts/antennas-postgres) directory:

   ```shell
   cd demos/antennas-postgres
   ```

1. Spin up the setup:

   ```shell
   docker-compose up -d
   ```

   **Note:** Downloading all the Docker images needed for the quickstart can take a few minutes. ☕

1. Once you're done exploring, remember to tear down the setup:

   ```shell
   docker-compose down -v
   ```

## Connect

```bash
psql -h localhost -p 6875 materialize materialize
```

In the background, a [load generator](https://github.com/MaterializeInc/demos/blob/main/antennas-postgres/helper/src/app.ts) is writing antenna performance data to the source PostgreSQL database. As this happens, we want to propagate those changes to Materialize and keep our monitoring materialized views up-to-date with the latest performance indicators.

### Creating a source

To listen to changes in the source database, we'll use the [PostgreSQL source](/sql/create-source/postgres/). This source uses PostgreSQL’s native replication protocol to keep track of any CRUD operations upstream, and

We'll gloss over configuring the source database for replication (since that's taken care of in the setup), but you can refer to the [Change Data Capture (PostgreSQL) guide](/integrations/cdc-postgres/#direct-postgres-source) for the required configuration steps.

1. In the SQL client, run the `CREATE SOURCE` statement:

    ```sql
        CREATE SOURCE antennas_publication_source
        FROM POSTGRES
        CONNECTION 'host=postgres port=5432 user=materialize password=materialize dbname=postgres'
        PUBLICATION 'antennas_publication_source';
    ```


1. for all tables in the publication,

    ```sql
        CREATE VIEWS FROM SOURCE antennas_publication_source;
    ```

    Under the hood, Materialize parses this statement into view definitions for each table (so you don’t have to!).

    Once you’ve created the Postgres source, you can create views that filter the replication stream and take care of converting its elements to the original data types:

    Each Materialize replication slot can be used to source data for a single materialized view. You can create multiple non-materialized views for the same replication slot using the CREATE VIEWS statement.

### Check CDC in action

1. Open your browser and go to the application `localhost:3000`

1. Launch the `psql` shell against the Postgres source by running:

    ```shell
    psql postgresql://postgres:pg_password@localhost:5432/postgres
    ```

1. Check the main antennas data

    ```sql
    SELECT * FROM antennas LIMIT 7;
    ```

1. Delete one antenna while keeping an eye on the application and see how it disappears

    ```sql
    DELETE FROM antennas WHERE antenna_id = 1;
    ```


First, a [source](https://materialize.com/docs/sql/create-source/) needs to spin up. It could be Postgres, Redpanda, Kafka, S3, or other alternatives. It will define how to store, insert, and process in Materialize the performance events and antenna’s position on the map.

{{< figure
    src="https://materialize.com/wp-content/uploads/2022/04/Inner-Frame.png"
    alt="Materialize sources"
>}}


The source defines how to parse the events schema.

•   Postgres has tables with a defined schema.

•   Kafka/Redpanda can have topics with or without a schema.

Let's focus only on the Postgres case, where the source already defines a schema. Refer to [this repository](https://github.com/MaterializeInc/demos/tree/main/antennas-kafka). if you want to know how it would look using Kafka/Redpanda without a schema.

The first step while building an application using Postgres is making available the tables to consume from Materialize.
It’s done through a process known as [Change Data Capture (CDC)](https://materialize.com/docs/sql/create-source/materialize-cdc/). As it refers to in the [integration guide](https://materialize.com/docs/integrations/cdc-postgres/), <i>“it allows you to track and propagate changes in a Postgres database to downstream consumers based on its Write-Ahead Log (WAL).”</i>

From Materialize standpoint it requires creating a source:



After setting up, Materialize will consume the current data and any future changes. In this quickstart, it will be the antenna's performance events and their static data like position and name. Speeding up development can be done through commands like [CREATE VIEWS](https://materialize.com/docs/sql/create-views/). It will create a view for each table available on the Postgres publication.





## Model and transform

### Views


### Materialized views

The antenna's health and performance metrics could be asked by running a query over the available data in the source. But why, rather than calculating the same information running the same query when a materialized view can compute and update the value incrementally with each new event? In the end, a simple lookup over the materialized view is enough and the fastest way to retrieve the results.

```sql
    CREATE MATERIALIZED VIEW IF NOT EXISTS last_half_minute_performance_per_antenna AS
    SELECT A.antenna_id,
           A.geojson,
           AVG(AP.performance) AS performance
    -- Join
    FROM antennas A
    JOIN antennas_performance AP ON (A.antenna_id = AP.antenna_id)
    -- Filter
    WHERE ((CAST(EXTRACT( epoch from AP.updated_at) AS NUMERIC) * 1000) + 30000) > mz_logical_timestamp()
    -- Aggregate
    GROUP BY A.antenna_id, A.geojson;
```

Antenna's performance and their derivated health status in the last half-minute are available to consume in the materialized view `last_half_minute_performance_per_antenna`. Using Materialize, the stack can now react in a blink and win back every affected customer; Updates happen fast, and a suitable back-end and front-end needs to maintain the pace.

### Check Materialize in action

Now all the components should be up and running.

1. Launch the `psql` shell against Materialize by running:

    ```shell
    psql postgresql://materialize:materialize@localhost:6875/materialize
    ```

1. Within the CLI, check how an antenna's performance is doing using `TAIL`

    ```sql
    COPY ( TAIL ( SELECT * FROM last_half_minute_performance_per_antenna WHERE antenna_id = 1 )) TO STDOUT;
    ```

1. Check the average performance per antenna since they are on:

    ```sql
    SELECT antenna_id, AVG(performance) as average_performance
    FROM antennas_performance
    GROUP BY antenna_id;
    ```

1. Turn the query into a Materialized View:

    ```sql
    CREATE MATERIALIZED VIEW historical_performance AS
    SELECT antenna_id, AVG(performance) as average_performance
    FROM antennas_performance
    GROUP BY antenna_id;
    ```

1. Check the results:

    ```sql
    SELECT * FROM historical_performance ORDER BY average_performance LIMIT 5;
    ```

## Subscribe to changes

A suitable back-end is whatever you are comfortable with, and even better if it has support for sockets, SSE, or any sort of streaming transport.  The idea is to use a [`TAIL`](https://materialize.com/docs/sql/tail/) subscription to distribute events as soon as they happen in a performant way and keep the stack reacting as a chain of events.

GraphQL with sockets is one of the many options available. [Graphql-ws](https://github.com/enisdenjo/graphql-ws) is a framework that will leverage the transport and communication between the front-end application and the `TAIL` updates.

The React front-end communicates using one of the most known clients for the framework, the [Apollo client](https://www.apollographql.com/docs/react/), with a custom [Link](https://github.com/MaterializeInc/demos/blob/lts/antennas-postgres/frontend/src/link.ts) to support the subscriptions.

Operation teams got very happy when they knew that a [microservice](https://github.com/MaterializeInc/demos/blob/lts/antennas-postgres/microservice/src/app.ts) communicates to the GraphQL back-end using `graphql-ws` client with the NodeJS `ws` socket library. When it detects
an unhealthy antenna will deploy a set of helper antennas.

### `TAIL`

### GraphQL

1. ddd

1. In a browser, navigate to `localhost:3000`. There, you can find the React front-end consuming the GraphQL subscriptions.

{{< figure
    src="https://materialize.com/wp-content/uploads/2022/06/Jun-16-2022-18-38-42.gif"
    alt="Materialize real-time application"
>}}

## Learn more

-   [Replace a microservice with a SQL query](/quickstarts/microservice/)
-   [Run SQL on streaming logs](/quickstarts/log-parsing/)
-   [`CREATE SOURCE`](/sql/create-source)
