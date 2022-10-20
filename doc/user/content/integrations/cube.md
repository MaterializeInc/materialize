---
title: "Cube"
description: "How to connect a Cube deployment to Materialize"
aliases:
  /third-party/cube/
menu:
  main:
    parent: "integration-guides"
    name: "Cube"
---

[Cube](https://www.cubejs.com/) is an open-source **Headless BI platform**. This integration guide will teach you how to connect Cube with Materialize in a few steps using the browser.

### Requirements

- Have a Cube and Materialize account.
    - Create a [Cube account here](https://cubecloud.dev/auth/signup).
    - Create a [Materialize account here](https://materialize.com/register/).
- A Materialize's region enabled.

## Steps

1. #### Login to Cube and create a deployment
    Be sure to match the regions to have the lowest latency.

1. #### Set up the Cube project
    You will have the following options to build the project:<br/>
        • CLI<br/>
        • Github repository<br/>
        • Create from scratch <br/>

    Choose **create from scratch** to proceed with this guide.

1. #### Set up the connection to Materialize
    Select Cube's specific connector for [Materialize](https://cube.dev/docs/config/databases/materialize#materialize) and fill in the fields.
    Field             | Value
    ----------------- | ----------------
    Host              | Materialize host name.
    Port              | **6875**
    Database username | Materialize user.
    Database password | App-specific password.
    SSL               | ✅

1. #### Set up the schema
    You can rely on Cube to infer the schema or skip the step to do it manually. Cube will figure out tables, sources, and materialized views and try to figure out their dimensions, measures, primary keys, and joins.

1. #### Set up the default database
    Before running queries on Materialize, configure the database name as follows: <br/>
      i. Access **Settings** <br/>
      ii. Click on **Configuration** <br/>
      iii. Add `CUBEJS_DB_NAME` as an environment variable with `materialize` as a value. <br/>

1. #### You are all set!

For more information and examples, check out our [shared blog post](https://materialize.com/blog/materialize-cube-integration/).
