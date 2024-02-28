---
title: "Cube"
description: "How to connect a Cube deployment to Materialize"
aliases:
  /third-party/cube/
---

[Cube](https://cube.dev/) is a headless BI platform that makes data accessible and consistent across every application. It provides data modeling, access control, caching, and a variety of APIs (SQL, GraphQL, and REST) on top of any data warehouse, database, or query engine, including Materialize.

In this guide, we’ll cover how to connect and configure a [Cube Cloud](https://cube.dev/product/why-cube-cloud) deployment to Materialize.

## Steps

### Create and configure Cube

1. #### Login to Cube and create a deployment
    Match the regions to improve latency.

1. #### Set up the Cube project
    You will have the following options to build the project:<br/>
        • CLI<br/>
        • Github repository<br/>
        • Create from scratch <br/>

    Click on the **create** from scratch option to be align with this guide.

1. #### Configure the connection to Materialize
    Use Cube's connector for [Materialize](https://cube.dev/docs/config/databases/materialize#materialize) and fill in the fields.
    Field             | Value
    ----------------- | ----------------
    Host              | Materialize host name.
    Port              | **6875**
    Database username | Materialize user.
    Database password | App-specific password.
    Cluster           | Default cluster for the connection.
    SSL               | ✅

1. #### Configure the default database
    Configure the database name as follows: <br/>
      i. Access **Settings** <br/>
      ii. Click on **Configuration** <br/>
      iii. Add `CUBEJS_DB_NAME` as an environment variable with `materialize` as a value. <br/>

1. #### Configure the cluster name
    Configure the cluster name as follows: <br/>
      i. Access **Settings** <br/>
      ii. Click on **Configuration** <br/>
      iii. Add `CUBEJS_DB_MATERIALIZE_CLUSTER` as an environment variable with the cluster name as a value. <br/>

### Set and use the schema

1. #### Build the schema
    You can rely on Cube to infer the schema or do it manually. Cube will figure out Materialize's schema and try to obtain the dimensions, measures, primary keys, and joins.

1. #### You are all set!
    Head to the **Playground** and start interacting with Materialize.

## Related pages

* [Shared blog post](https://materialize.com/blog/materialize-cube-integration/) with additional information and examples.
* [Cube’s connector details](https://cube.dev/docs/config/databases/materialize) for Materialize.
