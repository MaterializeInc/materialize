---
title: "How to connect Azure Database for PostgreSQL to Materialize"
description: "How to connect Azure Database for PostgreSQL as a source to Materialize."
menu:
  main:
    parent: "integration-guides"
    name: "Azure DB for PostgreSQL"
---

Materialize can read data from Azure DB for PostgreSQL via the direct [Postgres Source](/sql/create-source/postgres/). Before you start, note that a database restart will be required after making the changes below.

1. The Materialize instance will need access to connect to the upstream database. This is usually controlled by IP address. In your Azure portal, go to the Azure Database for PostgreSQL instance and under the "Connections security" section add your Materialize instance's IP address to the allowed IP addresses list and click on the "Save" button.

1. In the Azure portal or using the Azure CLI, [enable logical replication](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#set-up-your-server) for the PostgreSQL instance.

1. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

     The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream.

For more information, see the [Azure Database for PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical#pre-requisites-for-logical-replication-and-logical-decoding) documentation.


## Related pages

- [Change Data Capture (Postgres)](../cdc-postgres/)
- [`CREATE SOURCE FROM POSTGRES`](/sql/create-source/postgres/)
