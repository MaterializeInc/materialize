---
title: "How to connect GCP Cloud SQL to Materialize"
description: "How to connect GCP Cloud SQL as a source to Materialize."
menu:
  main:
    parent: "integration-guides"
    name: "GCP Cloud SQL"
---


To connect GCP Cloud SQL to Materialize via the direct [Postgres Source](/sql/create-source/postgres/), as a user with the `cloudsqlsuperuser` role, make these changes to the upstream database:

1. In the Google Cloud Console, set the `cloudsql.logical_decoding` to `on`. This enables logical replication.

1. The Materialize instance will need access to connect to the upstream database. This is usually controlled by IP address. If you are hosting your own installation of Materialize, in your Google Cloud Console, enable access on the upstream database for the Materialize replica's IP address.

1. Restart the database to apply your changes.

1. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

    _For specific tables:_

    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

    _For all tables in Postgres:_

    ```sql
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

    The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream.

For more information, see the [Cloud SQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance) documentation.
