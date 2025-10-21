1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your MySQL instance and start ingesting data:

    ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_mysql
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    - The command uses the `IN CLUSTER` clause to create the source in the
      `ingest_postgres` cluster. If omitted, the source is created in the
      current cluster.

    - To ingest data from specific schemas or tables, use the `FOR SCHEMAS
      (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options
      instead of `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or `IGNORE
      COLUMNS` options. Check out the [reference
      documentation](/sql/create-source/mysql/#supported-types) for guidance.

    - After source creation, you can handle upstream [schema
    changes](/sql/create-source/mysql/#schema-changes) by dropping and
    recreating the source.
