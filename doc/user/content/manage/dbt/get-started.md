---
title: "Get started with dbt and Materialize"
description: "How to use dbt and Materialize to transform streaming data in real time."
menu:
  main:
    parent: manage-dbt
    weight: 10
    identifier: "get-started-dbt"
    name: "Get started"
---

[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for
data transformation ("the T in ELT"). It combines the accessibility of SQL with
software engineering best practices, allowing you to not only build reliable
data pipelines, but also document, test and version-control them.

In this guide, we'll cover how to use dbt and Materialize to transform streaming
data in real time — from model building to continuous testing.

## Setup

Setting up a dbt project with Materialize is similar to setting it up with any
other database that requires a non-native adapter. To get up and running, you
need to:

1. Install the [`dbt-materialize` plugin](https://pypi.org/project/dbt-materialize/)
   (optionally using a virtual environment):

   {{< note >}}
    The `dbt-materialize` adapter can only be used with **dbt Core**. Making the
    adapter available in dbt Cloud depends on prioritization by dbt Labs. If you
    require dbt Cloud support, please [reach out to the dbt Labs team](https://www.getdbt.com/community/join-the-community/).
  {{</ note >}}

    ```bash
    python3 -m venv dbt-venv                  # create the virtual environment
    source dbt-venv/bin/activate              # activate the virtual environment
    pip install dbt-core dbt-materialize      # install dbt-core and the adapter
    ```

    The installation will include the `dbt-postgres` dependency. To check that
    the plugin was successfully installed, run:

    ```bash
    dbt --version
    ```

    `materialize` should be listed under "Plugins". If this is not the case,
    double-check that the virtual environment is activated!

1. To get started, make sure you have a Materialize account.

## Create and configure a dbt project

A [dbt project](https://docs.getdbt.com/docs/building-a-dbt-project/projects) is
a directory that contains all dbt needs to run and keep track of your
transformations. At a minimum, it must have a project file
(`dbt_project.yml`) and at least one [model](#build-and-run-dbt-models)
(`.sql`).

To create a new project, run:

```bash
dbt init <project_name>
```

This command will bootstrap a starter project with default configurations and
create a `profiles.yml` file, if it doesn't exist. To help you get started, the
`dbt init` project includes sample models to run the [Materialize quickstart](/get-started/quickstart/).

### Connect to Materialize

{{< note >}}

As a best practice, we strongly recommend using [service
accounts](/security/cloud/users-service-accounts/create-service-accounts) to
connect external applications, like dbt, to Materialize.

{{</ note >}}

dbt manages all your connection configurations (or, profiles) in a file called
[`profiles.yml`](https://docs.getdbt.com/dbt-cli/configure-your-profile). By
default, this file is located under `~/.dbt/`.

1. Locate the `profiles.yml` file in your machine:

    ```bash
    dbt debug --config-dir
    ```

    **Note:** If you started from an existing project but it's your first time
      setting up dbt, it's possible that this file doesn't exist yet. You can
      manually create it in the suggested location.

1. Open `profiles.yml` and adapt it to connect to Materialize using the
   reference [profile configuration](https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#connecting-to-materialize-with-dbt-materialize).

    As an example, the following profile would allow you to connect to
    Materialize in two different environments: a developer environment
    (`dev`) and a production environment (`prod`).

    ```yaml
    default:
      outputs:

        prod:
          type: materialize
          threads: 1
          host: <host>
          port: 6875
          # Materialize user or service account (recommended)
          # to connect as
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

    The `target` parameter allows you to configure the [target environment](https://docs.getdbt.com/docs/guides/managing-environments#how-do-i-maintain-different-environments-with-dbt)
    that dbt will use to run your models.

1. To test the connection to Materialize, run:

    ```bash
    dbt debug
    ```

    If the output reads `All checks passed!`, you're good to go! The
    [dbt documentation](https://docs.getdbt.com/docs/guides/debugging-errors#types-of-errors)
    has some helpful pointers in case you run into errors.

## Build and run dbt models

For dbt to know how to persist (or not) a transformation, the model needs to be
associated with a [materialization](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations)
strategy. Because Materialize is optimized for real-time transformations of
streaming data and the core of dbt is built around batch, the `dbt-materialize`
adapter implements a few custom materialization types:

{{% dbt-materializations %}}

Create a materialization for each SQL statement you're planning to deploy. Each
individual materialization should be stored as a `.sql` file under the
directory defined by `model-paths` in `dbt_project.yml`.

### Sources

In Materialize, a [source](/sql/create-source) describes an **external** system
you want to read data from, and provides details about how to decode and
interpret that data. You can instruct dbt to create a source using the custom
`source` materialization. Once a source has been defined, it can be referenced
from another model using the dbt [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
or [`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source) functions.

{{< note >}}
To create a source, you first need to [create a connection](/sql/create-connection)
that specifies access and authentication parameters. Connections are **not
exposed** in dbt, and need to exist before you run any `source` models.
{{</ note >}}

{{< tabs tabID="1" >}}
{{< tab "Kafka">}}
Create a [Kafka source](/sql/create-source/kafka/).

**Filename:** sources/kafka_topic_a.sql
```mzsql
{{ config(materialized='source') }}

FROM KAFKA CONNECTION kafka_connection (TOPIC 'topic_a')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
```

The source above would be compiled to:

```
database.schema.kafka_topic_a
```

{{< /tab >}}
{{< tab "PostgreSQL">}}
Create a [PostgreSQL source](/sql/create-source/postgres/).

**Filename:** sources/pg.sql
```mzsql
{{ config(materialized='source') }}

FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
FOR ALL TABLES
```

Materialize will automatically create a **subsource** for each table in the
`mz_source` publication. Pulling subsources into the dbt context automatically
isn't supported yet. Follow the discussion in [dbt-core #6104](https://github.com/dbt-labs/dbt-core/discussions/6104#discussioncomment-3957001)
for updates!

A possible **workaround** is to define PostgreSQL sources as a [dbt source](https://docs.getdbt.com/docs/build/sources)
in a `.yml` file, nested under a `sources:` key, and list each subsource under
the `tables:` key.

```yaml
sources:
  - name: pg
    schema: "{{ target.schema }}"
    tables:
      - name: table_a
      - name: table_b
```

Once a subsource has been defined this way, it can be referenced from another
model using the dbt [`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
function. To ensure that dbt is able to determine the proper order to run the
models in, you should additionally force a dependency on the parent source
model (`pg`), as described in the [dbt documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies).

**Filename:** staging/dep_subsources.sql
```mzsql
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

{{< /tab >}}
{{< tab "MySQL">}}
Create a [MySQL source](/sql/create-source/mysql/).

**Filename:** sources/mysql.sql
```mzsql
{{ config(materialized='source') }}

FROM MYSQL CONNECTION mysql_connection
FOR ALL TABLES;
```

Materialize will automatically create a **subsource** for each table in the
upstream database. Pulling subsources into the dbt context automatically
isn't supported yet. Follow the discussion in [dbt-core #6104](https://github.com/dbt-labs/dbt-core/discussions/6104#discussioncomment-3957001)
for updates!

A possible **workaround** is to define MySQL sources as a [dbt source](https://docs.getdbt.com/docs/build/sources)
in a `.yml` file, nested under a `sources:` key, and list each subsource under
the `tables:` key.

```yaml
sources:
  - name: mysql
    schema: "{{ target.schema }}"
    tables:
      - name: table_a
      - name: table_b
```

Once a subsource has been defined this way, it can be referenced from another
model using the dbt [`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
function. To ensure that dbt is able to determine the proper order to run the
models in, you should additionally force a dependency on the parent source
model (`mysql`), as described in the [dbt documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies).

**Filename:** staging/dep_subsources.sql
```mzsql
-- depends_on: {{ ref('mysql') }}
{{ config(materialized='view') }}

SELECT
    table_a.foo AS foo,
    table_b.bar AS bar
FROM {{ source('mysql','table_a') }}
INNER JOIN
     {{ source('mysql','table_b') }}
    ON table_a.id = table_b.foo_id
```

The source and subsources above would be compiled to:

```
database.schema.mysql
database.schema.table_a
database.schema.table_b
```

{{< /tab >}}
{{< tab "Webhooks">}}
Create a [webhook source](/sql/create-source/webhook/).

**Filename:** sources/webhook.sql
```mzsql
{{ config(materialized='source') }}

FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS request_body,
        -- Make sure to fully qualify the secret if it isn't in the same
        -- namespace as the source!
        SECRET basic_hook_auth
      )
      constant_time_eq(headers->'authorization', basic_hook_auth)
    );
```

The source above would be compiled to:

```
database.schema.webhook
```
{{< /tab >}}
{{< /tabs >}}

### Views and materialized views

In dbt, a [model](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#getting-started)
is a `SELECT` statement that encapsulates a data transformation you want to run
on top of your database. When you use dbt with Materialize, **your models stay
up-to-date** without manual or configured refreshes. This allows you to
efficiently transform streaming data using the same thought process you'd use
for batch transformations against any other database.

Depending on your usage patterns, you can transform data using [`view`](#views)
or [`materialized_view`](#materialized-views) models. For guidance and best
practices on when to use views and materialized views in Materialize, see
[Indexed views vs. materialized views](/concepts/views/#indexed-views-vs-materialized-views).

#### Views

dbt models are materialized as [views](/sql/create-view) by default. Although
this means you can skip the `materialized` configuration in the model
definition to create views in Materialize, we recommend explicitly setting the
materialization type for maintainability.

**Filename:** models/view_a.sql
```mzsql
{{ config(materialized='view') }}

SELECT
    col_a, ...
-- Reference model dependencies using the dbt ref() function
FROM {{ ref('kafka_topic_a') }}
```

The model above will be compiled to the following SQL statement:

```mzsql
CREATE VIEW database.schema.view_a AS
SELECT
    col_a, ...
FROM database.schema.kafka_topic_a;
```

The resulting view **will not** keep results incrementally updated without an
index (see [Creating an index on a view](#creating-an-index-on-a-view)). Once a
`view` model has been defined, it can be referenced from another model using
the dbt [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
function.

##### Creating an index on a view

{{< tip >}}
For guidance and best practices on how to use indexes in Materialize, see
[Indexes on views](/concepts/indexes/#indexes-on-views).
{{</ tip >}}

To keep results **up-to-date** in Materialize, you can create [indexes](/concepts/indexes/)
on view models using the [`index` configuration](#indexes). This
allows you to bypass the need for maintaining complex incremental logic or
re-running dbt to refresh your models.

**Filename:** models/view_a.sql
```mzsql
{{ config(materialized='view',
          indexes=[{'columns': ['col_a'], 'cluster': 'cluster_a'}]) }}

SELECT
    col_a, ...
FROM {{ ref('kafka_topic_a') }}
```

The model above will be compiled to the following SQL statements:

```mzsql
CREATE VIEW database.schema.view_a AS
SELECT
    col_a, ...
FROM database.schema.kafka_topic_a;

CREATE INDEX database.schema.view_a_idx IN CLUSTER cluster_a ON view_a (col_a);
```

As new data arrives, indexes keep view results **incrementally updated** in
memory within a [cluster](/concepts/clusters/). Indexes help optimize query
performance and make queries against views fast and computationally free.

#### Materialized views

To materialize a model as a [materialized view](/concepts/views/#materialized-views),
set the `materialized` configuration to `materialized_view`.

**Filename:** models/materialized_view_a.sql
```mzsql
{{ config(materialized='materialized_view') }}

SELECT
    col_a, ...
-- Reference model dependencies using the dbt ref() function
FROM {{ ref('view_a') }}
```

The model above will be compiled to the following SQL statement:

```mzsql
CREATE MATERIALIZED VIEW database.schema.materialized_view_a AS
SELECT
    col_a, ...
FROM database.schema.view_a;
```

The resulting materialized view will keep results **incrementally updated** in
durable storage as new data arrives. Once a `materialized_view` model has been
defined, it can be referenced from another model using the dbt [`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
function.

##### Creating an index on a materialized view

{{< tip >}}
For guidance and best practices on how to use indexes in Materialize, see
[Indexes on materialized views](/concepts/views/#indexes-on-materialized-views).
{{</ tip >}}

With a materialized view, your models are kept **up-to-date** in Materialize as
new data arrives. This allows you to bypass the need for maintaining complex
incremental logic or re-run dbt to refresh your models.

These results are **incrementally updated** in durable storage — which makes
them available across clusters — but aren't optimized for performance. To make
results also available in memory within a [cluster](/concepts/clusters/), you
can create [indexes](/concepts/indexes/) on materialized view models using the
[`index` configuration](#indexes).

**Filename:** models/materialized_view_a.sql
```mzsql
{{ config(materialized='materialized_view')
          indexes=[{'columns': ['col_a'], 'cluster': 'cluster_b'}]) }}

SELECT
    col_a, ...
FROM {{ ref('view_a') }}
```

The model above will be compiled to the following SQL statements:

```mzsql
CREATE MATERIALIZED VIEW database.schema.materialized_view_a AS
SELECT
    col_a, ...
FROM database.schema.view_a;

CREATE INDEX database.schema.materialized_view_a_idx IN CLUSTER cluster_b ON materialized_view_a (col_a);
```

As new data arrives, results are **incrementally updated** in durable storage
and also accessible in memory within the [cluster](/concepts/clusters/) the
index is created in. Indexes help optimize query performance and make queries
against materialized views faster.

##### Using refresh strategies

{{< tip >}}
For guidance and best practices on how to use refresh strategies in Materialize,
see [Refresh strategies](/sql/create-materialized-view/#refresh-strategies).
{{</ tip >}}

{{< private-preview />}}

For data that doesn't require up-to-the-second freshness, or that can be
accessed using different patterns to optimize for performance and cost
(e.g., hot vs. cold data), it might be appropriate to use a non-default
[refresh strategy](/sql/create-materialized-view/#refresh-strategies).

To configure a refresh strategy in a materialized view model, use the
[`refresh_interval` configuration](#configuration-refresh-strategies).
Materialized view models configured with a refresh strategy must be deployed in
a [scheduled cluster](/sql/create-cluster/#scheduling) for cost savings to be
significant — so you must also specify a valid scheduled `cluster` using the
[`cluster` configuration](#configuration).

**Filename:** models/materialized_view_refresh.sql
```mzsql
{{ config(materialized='materialized_view', cluster='my_scheduled_cluster', refresh_interval={'at_creation': True, 'every': '1 day', 'aligned_to': '2024-10-22T10:40:33+00:00'}) }}

SELECT
    col_a, ...
FROM {{ ref('view_a') }}
```

The model above will be compiled to the following SQL statement:

```mzsql
CREATE MATERIALIZED VIEW database.schema.materialized_view_refresh
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 10PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-10-22T10:40:33+00:00'
) AS
SELECT ...;
```

Materialized views configured with a refresh strategy are **not incrementally
maintained** and must recompute their results from scratch on every refresh.

##### Using retain history

{{< tip >}}
For guidance and best practices on how to use retain history in Materialize,
see [Retain history](/transform-data/patterns/durable-subscriptions/#set-history-retention-period).
{{</ tip >}}

To configure how long historical data is retained in a materialized view, use the
`retain_history` configuration. This is useful for maintaining a window of
historical data for time-based queries or for compliance requirements.

**Filename:** models/materialized_view_history.sql
```mzsql
{{ config(
    materialized='materialized_view',
    retain_history='1hr'
) }}

SELECT
    col_a,
    count(*) as count
FROM {{ ref('view_a') }}
GROUP BY col_a
```

The model above will be compiled to the following SQL statement:

```mzsql
CREATE MATERIALIZED VIEW database.schema.materialized_view_history
WITH (RETAIN HISTORY FOR '1hr')
AS
SELECT
    col_a,
    count(*) as count
FROM database.schema.view_a
GROUP BY col_a;
```

You can specify the retention period using common time units like:
- `'1hr'` for one hour
- `'1d'` for one day
- `'1w'` for one week

### Sinks

In Materialize, a [sink](/sql/create-sink) describes an **external** system you
want to write data to, and provides details about how to encode that data. You
can instruct dbt to create a sink using the custom `sink` materialization.

{{< tabs tabID="1" >}}
{{< tab "Kafka">}}
Create a [Kafka sink](/sql/create-sink).

**Filename:** sinks/kafka_topic_c.sql
```mzsql
{{ config(materialized='sink') }}

FROM {{ ref('materialized_view_a') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'topic_c')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
ENVELOPE DEBEZIUM
```

The sink above would be compiled to:
```
database.schema.kafka_topic_c
```

{{< /tab >}}
{{< /tabs >}}

### Configuration: clusters, databases and indexes {#configuration}

#### Clusters

Use the `cluster` option to specify the [cluster](/sql/create-cluster/ "pools of
compute resources (CPU, memory, and scratch disk space)") in which a
`materialized_view`, `source`, `sink` model, or `index` configuration is
created. If unspecified, the default cluster for the connection is used.

```mzsql
{{ config(materialized='materialized_view', cluster='cluster_a') }}
```

To dynamically generate the name of a cluster (e.g., based on the target
environment), you can override the `generate_cluster_name` macro with your
custom logic under the directory defined by [macro-paths](https://docs.getdbt.com/reference/project-configs/macro-paths)
in `dbt_project.yml`.

**Filename:** macros/generate_cluster_name.sql
```mzsql
{% macro generate_cluster_name(custom_cluster_name) -%}
    {%- if target.name == 'prod' -%}
        {{ custom_cluster_name }}
    {%- else -%}
        {{ target.name }}_{{ custom_cluster_name }}
    {%- endif -%}
{%- endmacro %}
```

#### Databases

Use the `database` option to specify the [database](/sql/namespaces/#database-details)
in which a `source`, `view`, `materialized_view` or `sink` is created. If
unspecified, the default database for the connection is used.

```mzsql
{{ config(materialized='materialized_view', database='database_a') }}
```

#### Indexes

Use the `indexes` configuration to define a list of [indexes](/concepts/indexes/) on
`source`, `view`, `table` or `materialized view` materializations. In
Materialize, [indexes](/concepts/indexes/) on a view maintain view results in
memory within a [cluster](/concepts/clusters/ "pools of compute resources (CPU,
memory, and scratch disk space)"). As the underlying data changes, indexes
**incrementally update** the view results in memory.

Each `index` configuration can have the following components:

Component                            | Value     | Description
-------------------------------------|-----------|--------------------------------------------------
`columns`                            | `list`    | One or more columns on which the index is defined. To create an index that uses _all_ columns, use the `default` component instead.
`name`                               | `string`  | The name for the index. If unspecified, Materialize will use the materialization name and column names provided.
`cluster`                            | `string`  | The cluster to use to create the index. If unspecified, indexes will be created in the cluster used to create the materialization.
`default`                            | `bool`    | Default: `False`. If set to `True`, creates a [default index](/sql/create-index/#syntax).

##### Creating a multi-column index

```mzsql
{{ config(materialized='view',
          indexes=[{'columns': ['col_a','col_b'], 'cluster': 'cluster_a'}]) }}
```

##### Creating a default index

```mzsql
{{ config(materialized='view',
    indexes=[{'default': True}]) }}
```

### Configuration: refresh strategies {#configuration-refresh-strategies}

{{< private-preview />}}

**Minimum requirements:** `dbt-materialize` v1.7.3+

Use the `refresh_interval` configuration to define [refresh strategies](#using-refresh-strategies)
for materialized view models.

The `refresh_interval` configuration can have the following components:

Component       | Value    | Description
----------------|----------|--------------------------------------------------
`at`            | `string` | The specific time to refresh the materialized view at, using the [refresh at](/sql/create-materialized-view/#refresh-at) strategy.
`at_creation`   | `bool`   | Default: `false`. Whether to trigger a first refresh when the materialized view is created.
`every`         | `string` | The regular interval to refresh the materialized view at, using the [refresh every](/sql/create-materialized-view/#refresh-every) strategy.
`aligned_to`    | `string` | The _phase_ of the regular interval to refresh the materialized view at, using the [refresh every](/sql/create-materialized-view/#refresh-every) strategy. If unspecified, defaults to the time when the materialized view is created.
`on_commit`     | `bool`   | Default: `false`. Whether to use the default [refresh on commit](/sql/create-materialized-view/#refresh-on-commit) strategy. Setting this component to `true` is equivalent to **not specifying** `refresh_interval` in the configuration block, so we recommend only using it for the special case of parametrizing the configuration option (e.g., in macros).

### Configuration: model contracts and constraints {#configuration-contracts}

#### Model contracts

**Minimum requirements:** `dbt-materialize` v1.6.0+

You can enforce [model contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
for `view`, `materialized_view` and `table` materializations to guarantee that
there are no surprise breakages to your pipelines when the shape of the data
changes.

```yaml
    - name: model_with_contract
    config:
      contract:
        enforced: true
    columns:
      - name: col_with_constraints
        data_type: string
      - name: col_without_constraints
        data_type: int
```

Setting the `contract` configuration to `enforced: true` requires you to specify
a `name` and `data_type` for every column in your models. If there is a
mismatch between the defined contract and the model you're trying to run, dbt
will fail during compilation! Optionally, you can also configure column-level
[constraints](#constraints).

#### Constraints

**Minimum requirements:** `dbt-materialize` v1.6.1+

Materialize supports enforcing column-level `not_null` [constraints](https://docs.getdbt.com/reference/resource-properties/constraints)
for `materialized_view` materializations. No other constraint or materialization
types are supported.

```yaml
    - name: model_with_constraints
    config:
      contract:
        enforced: true
    columns:
      - name: col_with_constraints
        data_type: string
        constraints:
          - type: not_null
      - name: col_without_constraints
        data_type: int
```

A `not_null` constraint will be compiled to an [`ASSERT NOT NULL`](/sql/create-materialized-view/#non-null-assertions)
option for the specified columns of the materialize view.

```mzsql
CREATE MATERIALIZED VIEW model_with_constraints
WITH (
        ASSERT NOT NULL col_with_constraints
     )
AS
SELECT NULL AS col_with_constraints,
       2 AS col_without_constraints;
```

## Build and run dbt

1. [Run](https://docs.getdbt.com/reference/commands/run) the dbt models:

    ```
    dbt run
    ```

    This command generates **executable SQL code** from any model files under
    the specified directory and runs it in the target environment. You can find
    the compiled statements under `/target/run` and `target/compiled` in the
    dbt project folder.

1. Using the [SQL Shell](/console/), or your preferred
   SQL client connected to Materialize, double-check that all objects have been
   created:

    ```mzsql
    SHOW SOURCES [FROM database.schema];
    ```

    <p></p>

    ```nofmt
           name
    -------------------
     mysql_table_a
     mysql_table_b
     postgres_table_a
     postgres_table_b
     kafka_topic_a
    ```

    <p></p>

    ```mzsql
    SHOW VIEWS;
    ```

    <p></p>

    ```nofmt
           name
    -------------------
     view_a
    ```

    <p></p>

    ```mzsql
    SHOW MATERIALIZED VIEWS;
    ```

    <p></p>

    ```nofmt
           name
    -------------------
     materialized_view_a
    ```

That's it! From here on, Materialize makes sure that your models
are **incrementally updated** as new data streams in, and that you get **fresh
and correct results** with millisecond latency whenever you query your views.

## Test and document a dbt project

[//]: # "TODO(morsapaes) Call out the cluster configuration for tests and
store_failures_as once this page is rehashed."

### Configure continuous testing

Using dbt in a streaming context means that you're able to run data quality and
integrity [tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests)
non-stop. This is useful to monitor failures as soon as they happen, and
trigger **real-time alerts** downstream.

1. To configure your project for continuous testing, add a `data_tests` property to
   `dbt_project.yml` with the `store_failures` configuration:

    ```yaml
    data_tests:
      dbt_project.name:
        models:
          +store_failures: true
          +schema: 'etl_failure'
    ```

    This will instruct dbt to create a materialized view for each configured
    test that can keep track of failures over time. By default, test views are
    created in a schema suffixed with `dbt_test__audit`. To specify a custom
    suffix, use the `schema` config.

    **Note:** As an alternative, you can specify the `--store-failures` flag
      when running `dbt test`.

1. Add tests to your models using the `data_tests` property in the model
   configuration `.yml` files:

    ```yaml
    models:
      - name: materialized_view_a
        description: 'materialized view a description'
        columns:
          - name: col_a
            description: 'column a description'
            data_tests:
              - not_null
              - unique
    ```

    The type of test and the columns being tested are used as a base for naming
    the test materialized views. For example, the configuration above would
    create views named `not_null_col_a` and `unique_col_a`.

1. Run the tests:

    ```bash
    dbt test # use --select test_type:data to only run data tests!
    ```

    When configured to `store_failures`, this command will create a materialized
    view for each test using the respective `SELECT` statements, instead of
    doing a one-off check for failures as part of its execution.

    This guarantees that your tests keep running in the background as views that
    are automatically updated as soon as an assertion fails.

1. Using the [SQL Shell](/console/), or your preferred
   SQL client connected to Materialize, that the schema storing the tests has been
   created, as well as the test materialized views:

    ```mzsql
    SHOW SCHEMAS;
    ```

    <p></p>

    ```nofmt
           name
    -------------------
     public
     public_etl_failure
    ```

    <p></p>

    ```mzsql
    SHOW MATERIALIZED VIEWS FROM public_etl_failure;
    ```

    <p></p>

    ```nofmt
           name
    -------------------
     not_null_col_a
     unique_col_a
    ```

With continuous testing in place, you can then build alerts off of the test
materialized views using any common PostgreSQL-compatible [client library](/integrations/client-libraries/)
and [`SUBSCRIBE`](/sql/subscribe/)(see the [Python cheatsheet](/integrations/client-libraries/python/#stream)
for a reference implementation).

### Generate documentation

[//]: # "TODO(morsapaes) Mention exposures and DAG costumization (e.g., colors)."

dbt can automatically generate [documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation)
for your project as a shareable website. This brings **data governance** to your
streaming pipelines, speeding up life-saving processes like data discovery
(_where_ to find _what_ data) and lineage (the path data takes from source
(s) to sink(s), as well as the transformations that happen along the way).

If you've already created `.yml` files with helpful [properties](https://docs.getdbt.com/reference/configs-and-properties)
about your project resources (like model and column descriptions, or tests), you
are all set.

1. To generate documentation for your project, run:

    ```bash
    dbt docs generate
    ```

    dbt will grab any additional project information and Materialize catalog
    metadata, then compile it into `.json` files (`manifest.json` and
    `catalog.json`, respectively) that can be used to feed the documentation
    website. You can find the compiled files under `/target`, in the dbt
    project folder.

1. Launch the documentation website. By default, this command starts a web
   server on port 8000:

    ```bash
    dbt docs serve #--port <port>
    ```

1. In a browser, navigate to `localhost:8000`. There, you can find an overview
   of your dbt project, browse existing models and metadata, and in general keep
   track of what's going on.

    If you click **View Lineage Graph** in the lower right corner, you can even
    inspect the lineage of your streaming pipelines!

    ![dbt lineage graph](https://user-images.githubusercontent.com/23521087/138125450-cf33284f-2a33-4c1e-8bce-35f22685213d.png)

### Persist documentation

**Minimum requirements:** `dbt-materialize` v1.6.1+

To persist model- and column-level descriptions as [comments](/sql/comment-on/)
in Materialize, use the [`persist_docs`](https://docs.getdbt.com/reference/resource-configs/persist_docs)
configuration.

{{< note >}}
Documentation persistence is tightly coupled with `dbt run` command invocations.
For "use-at-your-own-risk" workarounds, see [`dbt-core` #4226](https://github.com/dbt-labs/dbt-core/issues/4226). 👻
{{</ note >}}

1. To enable docs persistence, add a `models` property to `dbt_project.yml` with
   the `persist-docs` configuration:

    ```yaml
    models:
      +persist_docs:
        relation: true
        columns: true
    ```

    As an alternative, you can configure `persist-docs` in the config block of your models:

    ```mzsql
    {{ config(
        materialized=materialized_view,
        persist_docs={"relation": true, "columns": true}
    ) }}
    ```

1. Once `persist-docs` is configured, any `description` defined in your `.yml`
  files is persisted to Materialize in the [mz_internal.mz_comments](/sql/system-catalog/mz_internal/#mz_comments)
  system catalog table on every `dbt run`:

    ```mzsql
      SELECT * FROM mz_internal.mz_comments;
    ```
    <p></p>

    ```nofmt

        id  |    object_type    | object_sub_id |              comment
      ------+-------------------+---------------+----------------------------------
       u622 | materialize-view  |               | materialized view a description
       u626 | materialized-view |             1 | column a description
       u626 | materialized-view |             2 | column b description
    ```
