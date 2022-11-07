---
title: "Cube"
description: "How to connect a Cube deployment to Materialize"
aliases:
  /third-party/cube/
---

[Cube](https://www.cubejs.com/) is a headless BI platform that makes data accessible and consistent across every application. It provides data modeling, access control, caching, and a variety of APIs (SQL, GraphQL, and REST) on top of any data warehouse, database, or query engine, including Materialize.

In this guide, we’ll cover how to connect a [Cube Cloud](https://cube.dev/docs/cloud) deployment to Materialize.

## Pre-requisite

Before following the steps, make sure to meet the next items:

- Have a Cube account. Create [one here](https://cubecloud.dev/auth/signup).
- A Materialize's region enabled.

## Steps

1. #### Login to Cube and create a deployment
    Match the regions to improve latency.

1. #### Set up the Cube project
    You will have the following options to build the project:<br/>
        • CLI<br/>
        • Github repository<br/>
        • Create from scratch <br/>

    Click on the **create** from scratch option to be align with this guide.

1. #### Configure the connection to Materialize
    Use Cube's specific connector for [Materialize](https://cube.dev/docs/config/databases/materialize#materialize) and fill in the fields.
    Field             | Value
    ----------------- | ----------------
    Host              | Materialize host name.
    Port              | **6875**
    Database username | Materialize user.
    Database password | App-specific password.
    SSL               | ✅

1. #### Build the schema
    You can rely on Cube to infer the schema or skip the step to do it manually. Cube will figure out tables, sources, and materialized views and try to figure out their dimensions, measures, primary keys, and joins.

1. #### Set the default database
    Before running queries on Materialize, configure the database name as follows: <br/>
      i. Access **Settings** <br/>
      ii. Click on **Configuration** <br/>
      iii. Add `CUBEJS_DB_NAME` as an environment variable with `materialize` as a value. <br/>

1. #### You are all set!

For more information and examples, check out our [shared blog post](https://materialize.com/blog/materialize-cube-integration/).
