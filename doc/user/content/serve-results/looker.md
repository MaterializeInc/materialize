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

When using Looker with Materialize, be aware of the following limitations:

1. **Connection Test Error**: You might encounter this error when testing the connection to Materialize from Looker:

   ```
   Test kill: Cannot cancel queries: Query could not be found in database.
   ```

   This is a known issue with Looker and can be safely ignored and does not impact the functionality of Looker.

2. **Symmetric Aggregates**: Looker uses [symmetric aggregates](https://cloud.google.com/looker/docs/best-practices/understanding-symmetric-aggregates),a feature that relies on the `BIT` type with `SUM DISTINCT` in PostgreSQL connectors. While we haven't confirmed performance impacts in Materialize, consider the following:

   - You can disable symmetric aggregates in Looker if needed. For instructions on how to do this, refer to the [Looker documentation on disabling symmetric aggregates](https://cloud.google.com/looker/docs/reference/param-explore-symmetric-aggregates#not_all_database_dialects_support_median_and_percentile_measure_types_with_symmetric_aggregates).
   - Looker remains fully functional for visualizing Materialize data without this feature.

3. **Handling Symmetric Aggregates**:

   a. Test query performance with and without symmetric aggregates to determine the optimal configuration.

   b. If you encounter performance issues, disable symmetric aggregates in your Looker setup using the link provided above.

   c. For use cases requiring symmetric aggregates, contact Materialize support for optimization guidance.
