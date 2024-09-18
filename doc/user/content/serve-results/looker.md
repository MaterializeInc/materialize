---
title: "Looker"
description: "How to create dashboards with Looker"
aliases:
  - /third-party/looker/
  - /integrations/looker/
menu:
  main:
    parent: "bi-tools"
    name: "Looker"
    weight: 20
---

You can use [Looker](https://cloud.google.com/looker-bi) to create dashboards
based on the data maintained in Materialize.

## Database connection details

To set up a connection from Looker to Materialize, use the native
[PostgreSQL 9.5+ database dialect](https://cloud.google.com/looker/docs/db-config-postgresql)
with the following parameters:

Field                  | Value
---------------------- | ----------------
Dialect                | **PostgreSQL 9.5+**.
Host                   | Materialize host name.
Port                   | **6875**
Database               | **materialize**
Schema                 | **public**
Database username      | Materialize user.
Database password      | App-specific password.

![Connect using the credentials provided in the Materialize console](https://github-production-user-asset-6210df.s3.amazonaws.com/21223421/272911799-2525c5ae-4594-4d33-bdfa-c20af835c7c5.png)

## Configure a custom cluster

To configure a custom Materialize [cluster](/sql/create-cluster), follow these steps:

* Edit the Materialize connection.

* Expand the 'Additional Settings' section.

* In the 'Additional JDBC parameters' section, input:

    ```
    options=--cluster%3D<cluster_name>
    ```

    Make sure to replace `<cluster_name>` with the actual name of your cluster.

## Known limitations

When you connect to Materialize from Looker and try to test the connection, you
might see the following error:

```
Test kill: Cannot cancel queries: Query could not be found in database.
```

This is a known issue with Looker. You can safely ignore this error.
