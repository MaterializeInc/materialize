---
title: "Use dbt to manage Materialize"
description: "How to use dbt and Materialize to transform streaming data in real time."
aliases:
  - /guides/dbt/
  - /third-party/dbt/
  - /integrations/dbt/
menu:
  main:
    parent: manage
    name: "Use dbt to manage Materialize"
    weight: 10
---

{{< note >}}
The `dbt-materialize` adapter can only be used with dbt Core. We are working with the dbt community to bring native Materialize support to dbt Cloud!
{{</ note >}}

[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for data transformation ("the T in ELT"). It combines the accessibility of SQL with software engineering best practices, allowing you to not only build reliable data pipelines, but also document, test and version-control them.

In this guide, we’ll cover how to use dbt and Materialize to transform streaming data in real time — from model building to continuous testing.

## Setup

**Minimum requirements:** dbt v0.18.1+

Setting up a dbt project with Materialize is similar to setting it up with any other database that requires a non-native adapter. To get up and running, you need to:

1. Install the [`dbt-materialize` plugin](https://pypi.org/project/dbt-materialize/) (optionally using a virtual environment):

    ```bash
    python3 -m venv dbt-venv         # create the virtual environment
    source dbt-venv/bin/activate     # activate the virtual environment
    pip install dbt-materialize      # install the adapter
    ```

    The installation will include `dbt-core` and the `dbt-postgres` dependency. To check that the plugin was successfully installed, run:

    ```bash
    dbt --version
    ```

    `materialize` should be listed under "Plugins". If this is not the case, double-check that the virtual environment is activated!

1. To get started, make sure you have a Materialize account.

## Create and configure a dbt project

A [dbt project](https://docs.getdbt.com/docs/building-a-dbt-project/projects) is a directory that contains all dbt needs to run and keep track of your transformations. At a minimum, it must have a project file (`dbt_project.yml`) and at least one [model](#build-and-run-dbt-models) (`.sql`).

To create a new project, run:

```bash
dbt init <project_name>
```

This command will bootstrap a starter project with default configurations and create a `profiles.yml` file, if it doesn't exist.

### Connect to Materialize

dbt manages all your connection configurations (or, profiles) in a file called [`profiles.yml`](https://docs.getdbt.com/dbt-cli/configure-your-profile). By default, this file is located under `~/.dbt/`.

1. Locate the `profiles.yml` file in your machine:

    ```bash
    dbt debug --config-dir
    ```

    **Note:** If you started from an existing project but it's your first time setting up dbt, it's possible that this file doesn't exist yet. You can manually create it in the suggested location.

1. Open `profiles.yml` and adapt it to connect to Materialize using the reference [profile configuration](https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#connecting-to-materialize-with-dbt-materialize).

    As an example, the following profile would allow you to connect to Materialize in two different environments: a developer environment (`dev`) and a production environment (`prod`).

    ```yaml
    default:
      outputs:

        prod:
          type: materialize
          threads: 1
          host: <host>
          port: 6875
          user: <user@domain.com>
          pass: <password>
          database: materialize
          schema: public
          # optionally use the cluster connection
          # parameter to specify the default cluster
          # for the connection
          cluster: <prod_cluster>
          sslmode: require
        dev:
          type: materialize
          threads: 1
          host: <host>
          port: 6875
          user: <user@domain.com>
          pass: <password>
          database: <dev_database>
          schema: <dev_schema>
          cluster: <dev_cluster>
          sslmode: require

      target: dev
    ```

    The `target` parameter allows you to configure the [target environment](https://docs.getdbt.com/docs/guides/managing-environments#how-do-i-maintain-different-environments-with-dbt) that dbt will use to run your models.

1. To test the connection to Materialize, run:

    ```bash
    dbt debug
    ```

    If the output reads `All checks passed!`, you're good to go! The [dbt documentation](https://docs.getdbt.com/docs/guides/debugging-errors#types-of-errors) has some helpful pointers in case you run into errors.

## Build and run dbt models

For dbt to know how to persist (or not) a transformation, the model needs to be associated with a [materialization](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations) strategy.
Because Materialize is optimized for real-time transformations of streaming data and the core of dbt is built around batch, the `dbt-materialize` adapter implements a few custom materialization types:

{{% dbt-materializations %}}

Create a materialization for each SQL statement you're planning to deploy. Each individual materialization should be stored as a `.sql` file under the directory defined by `model-paths` in `dbt_project.yml`.

### Sources

In Materialize, a [source](/sql/create-source) describes an **external** system you want to read data from, and provides details about how to decode and interpret that data. You can instruct dbt to create a source using the custom `source` materialization. Once a source has been defined, it can be referenced from another model using the dbt [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source) function.

{{< note >}}
To connect to a Kafka broker or PostgreSQL database, you first need to [create a connection](/sql/create-connection) that specifies access and authentication parameters.
Once created, a connection is **reusable** across multiple source models.
{{</ note >}}

{{< tabs tabID="1" >}} {{< tab "Kafka">}}
Create a [Kafka source](/sql/create-source/kafka/).

**Filename:** sources/kafka_topic_a.sql
```sql
{{ config(materialized='source') }}

CREATE SOURCE {{ this }}
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'topic_a')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  WITH (SIZE = '3xsmall')
```

The source above would be compiled to:

```
database.schema.kafka_topic_a
```

{{< /tab >}} {{< tab "PostgreSQL">}}
Create a [PostgreSQL source](/sql/create-source/postgres/).

**Filename:** sources/pg.sql
```sql
{{ config(materialized='source') }}

CREATE SOURCE {{ this }}
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES
  WITH (SIZE = '3xsmall')
```

Materialize will automatically create a **subsource** for each table in the `mz_source` publication. Pulling subsources into the dbt context automatically isn't supported yet. Follow the discussion in [dbt-core #6104](https://github.com/dbt-labs/dbt-core/discussions/6104#discussioncomment-3957001) for updates!

A possible **workaround** is to define PostgreSQL sources as a [dbt source](https://docs.getdbt.com/docs/build/sources) in a `.yml` file, nested under a `sources:` key, and list each subsource under the `tables:` key.

```yaml
sources:
  - name: pg
    schema: "{{ target.schema }}"
    tables:
      - name: table_a
      - name: table_b
```

Once a subsource has been defined this way, it can be referenced from another model using the dbt [`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source) function. To ensure that dbt is able to determine the proper order to run the models in, you should additionally force a dependency on the parent source model (`pg`), as described in the [dbt documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies).

**Filename:** staging/dep_subsources.sql
```sql
-- depends_on: {{ ref('pg') }}
{{ config(materialized='view') }}

SELECT
    table_a.foo AS foo,
    table_b.bar AS bar
FROM {{ source('pg','table_a') }}
INNER JOIN
     {{ source('pg','table_b') }}
    ON table_a.id = table_b.foo_id
```

The source and subsources above would be compiled to:

```
database.schema.pg
database.schema.table_a
database.schema.table_b
```

{{< /tab >}} {{< /tabs >}}

* Use the `{{ this }}` [relation](https://docs.getdbt.com/reference/dbt-jinja-functions/this) to generate a fully-qualified name for the source from the base model name.

### Views and materialized views

In dbt, a [model](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#getting-started) is a `SELECT` statement that encapsulates a data transformation you want to run on top of your database.

When you use dbt with Materialize, **your models stay up-to-date** without manual or configured refreshes. This allows you to efficiently transform streaming data using the same thought process you'd use for batch transformations on top of any other database.

#### Views

dbt models are materialized as `views` by default, so to create a [view](/sql/create-view) in Materialize you can simply provide the SQL statement in the model (and skip the `materialized` configuration parameter).

**Filename:** models/view_a.sql
```sql
SELECT
    col_a, ...
FROM {{ ref('kafka_topic_a') }}
```

The model above would be compiled to `database.schema.view_a`.
One thing to note here is that the model depends on the Kafka source defined above. To express this dependency and track the **lineage** of your project, you can use the dbt [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) function.

#### Materialized views

This is where Materialize goes beyond dbt's incremental models (and traditional databases), with [materialized views](/sql/create-materialized-view) that **continuously update** as the underlying data changes:

**Filename:** models/materialized_view_a.sql
```sql
{{ config(materialized='materializedview') }}

SELECT
    col_a, ...
FROM {{ ref('view_a') }}
```

The model above would be compiled to `database.schema.materialized_view_a`.
Here, the model depends on the view defined above, and is referenced as such via the dbt [ref()](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) function.

### Sinks

In Materialize, a [sink](/sql/create-sink) describes an **external** system you want to write data to, and provides details about how to encode that data. You can instruct dbt to create a sink using the custom `sink` materialization.

{{< tabs tabID="1" >}} {{< tab "Kafka">}}
Create a [Kafka sink](/sql/create-sink).

**Filename:** sinks/kafka_topic_c.sql
```sql
{{ config(materialized='sink') }}

CREATE SINK {{ this }}
  FROM {{ ref('materialized_view_a') }}
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'topic_c')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM
  WITH (SIZE = '3xsmall')
```

The sink above would be compiled to:
```
database.schema.kafka_topic_c
```

{{< /tab >}} {{< /tabs >}}

* Use the `{{ this }}` [relation](https://docs.getdbt.com/reference/dbt-jinja-functions/this) to generate a fully-qualified name for the sink from the base model name.

### Configuration: clusters, databases and indexes {#configuration}

#### Clusters

Use the `cluster` option to specify the [cluster](/sql/create-cluster/) in which a `materialized view` is created. If unspecified, the default cluster for the connection is used.

```sql
{{ config(materialized='materializedview', cluster='cluster_a') }}
```

#### Databases

Use the `database` option to specify the [database](/sql/namespaces/#database-details) in which a `source`, `view`, `materialized view` or `sink` is created. If unspecified, the default database for the connection is used.

```sql
{{ config(materialized='materializedview', database='database_a') }}
```

#### Indexes

Use the `indexes` option to define a list of [indexes](/sql/create-index/) on `source`, `view`, or `materialized view` materializations. Each index option can have the following components:

Component                            | Value     | Description
-------------------------------------|-----------|--------------------------------------------------
`columns`                            | `list`    | One or more columns on which the index is defined. To create an index that uses _all_ columns, use the `default` component instead.
`name`                               | `string`  | The name for the index. If unspecified, Materialize will use the materialization name and column names provided.
`cluster`                            | `string`  | The cluster to use to create the index. If unspecified, indexes will be created in the cluster used to create the materialization.
`default`                            | `bool`    | Default: `False`. If set to `True`, creates a [default index](/sql/create-index/#syntax).

##### Creating a multi-column index

```sql
{{ config(materialized='view',
          indexes=[{'columns': ['col_a','col_b'], 'cluster': 'cluster_a'}]) }}
```

##### Creating a default index

```sql
{{ config(materialized='view',
    indexes=[{'default': True}]) }}
```

## Build and run dbt

1. [Run](https://docs.getdbt.com/reference/commands/run) the dbt models:

    ```
    dbt run
    ```

    This command generates **executable SQL code** from any model files under the specified directory and runs it in the target environment. You can find the compiled statements under `/target/run` and `target/compiled` in the dbt project folder.

1. Using a new terminal window, [connect](/integrations/psql/) to Materialize to double-check that all objects have been created:

    ```bash
    psql "postgres://<user>:<password>@<host>:6875/materialize"
    ```

    ```sql
    materialize=> SHOW SOURCES [FROM database.schema];
           name
    -------------------
     postgres_table_a
     postgres_table_b
     kafka_topic_a

     materialize=> SHOW VIEWS;
           name
    -------------------
     view_a

     materialize=> SHOW MATERIALIZED VIEWS;
           name
    -------------------
     materialized_view_a
    ```

That's it! From here on, Materialize makes sure that your models are **incrementally updated** as new data streams in, and that you get **fresh and correct results** with millisecond latency whenever you query your views.

## Test and document a dbt project

### Configure continuous testing

Using dbt in a streaming context means that you're able to run data quality and integrity [tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests) non-stop, and monitor failures as soon as they happen. This is useful for unit testing during the development of your dbt models, and later in production to trigger **real-time alerts** downstream.

1. To configure your project for continuous testing, add a `tests` property to `dbt_project.yml` with the `store_failures` configuration:

    ```yaml
    tests:
      dbt_project.name:
        models:
          +store_failures: true
          +schema: 'etl_failure'
    ```

    This will instruct dbt to create a materialized view for each configured test that can keep track of failures over time. By default, test views are created in a schema suffixed with `dbt_test__audit`. To specify a custom suffix, use the `schema` config.

    **Note:** As an alternative, you can specify the `--store-failures` flag when running `dbt test`.

1. Add tests to your models using the `tests` property in the model configuration `.yml` files:

    ```yaml
    models:
      - name: materialized_view_a
        description: 'materialized view a description'
        columns:
          - name: col_a
            description: 'column a description'
            tests:
              - not_null
              - unique
    ```

    The type of test and the columns being tested are used as a base for naming the test materialized views. For example, the configuration above would create views named `not_null_col_a` and `unique_col_a`.

1. Run the tests:

    ```bash
    dbt test
    ```

    When configured to `store_failures`, this command will create a materialized view for each test using the respective `SELECT` statements, instead of doing a one-off check for failures as part of its execution.

    This guarantees that your tests keep running in the background as views that are automatically updated as soon as an assertion fails.

1. Using a new terminal window, [connect](/integrations/psql/) to Materialize to double-check that the schema storing the tests has been created, as well as the test materialized views:

    ```bash
    psql "postgres://<user>:<password>@<host>:6875/materialize"
    ```

    ```sql
    materialize=> SHOW SCHEMAS;
           name
    -------------------
     public
     public_etl_failure

     materialize=> SHOW MATERIALIZED VIEWS FROM public_etl_failure;;
           name
    -------------------
     not_null_col_a
     unique_col_a
    ```

With continuous testing in place, you can then build alerts off of the test materialized views using any common PostgreSQL-compatible [client library](/integrations/#client-libraries-and-orms) and [`SUBSCRIBE`](/sql/subscribe/) (see the [Python cheatsheet](/integrations/python/#stream) for a reference implementation).

### Generate documentation

dbt can automatically generate [documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation) for your project as a shareable website. This brings **data governance** to your streaming pipelines, speeding up life-saving processes like data discovery (_where_ to find _what_ data) and lineage (the path data takes from source(s) to sink(s), as well as the transformations that happen along the way).

If you've already created `.yml` files with helpful [properties](https://docs.getdbt.com/reference/configs-and-properties) about your project resources (like model and column descriptions, or tests), you are all set.

1. To generate documentation for your project, run:

    ```bash
    dbt docs generate
    ```

    dbt will grab any additional project information and Materialize catalog metadata, then compile it into `.json` files (`manifest.json` and `catalog.json`, respectively) that can be used to feed the documentation website. You can find the compiled files under `/target`, in the dbt project folder.

1. Launch the documentation website. By default, this command starts a web server on port 8000:

    ```bash
    dbt docs serve #--port <port>
    ```

1. In a browser, navigate to `localhost:8000`. There, you can find an overview of your dbt project, browse existing models and metadata, and in general keep track of what's going on.

    If you click **View Lineage Graph** in the lower right corner, you can even inspect the lineage of your streaming pipelines!

    ![dbt lineage graph](https://user-images.githubusercontent.com/23521087/138125450-cf33284f-2a33-4c1e-8bce-35f22685213d.png)
