1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your PostgreSQL instance and start ingesting data from the publication you
   created [earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
   ```

    The command uses the `IN CLUSTER` clause to create the source in the
    `ingest_postgres` cluster. If omitted, the source is created in the current cluster.

    To ingest data from specific schemas or tables in your publication, use `FOR
    SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead
    of `FOR ALL TABLES`.

    After source creation, you can handle upstream [schema
    changes](/sql/create-source/postgres/#schema-changes) for specific
    replicated tables using the [`ALTER SOURCE...ADD
    SUBSOURCE`](/sql/alter-source/#context) and [`DROP
    SOURCE`](/sql/alter-source/#dropping-subsources) syntax.
