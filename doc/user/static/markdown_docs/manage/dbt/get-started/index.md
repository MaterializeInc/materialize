<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Manage
Materialize](/docs/self-managed/v25.2/manage/)  /  [Use dbt to manage
Materialize](/docs/self-managed/v25.2/manage/dbt/)

</div>

# Get started with dbt and Materialize

[dbt](https://docs.getdbt.com/docs/introduction) has become the standard
for data transformation (“the T in ELT”). It combines the accessibility
of SQL with software engineering best practices, allowing you to not
only build reliable data pipelines, but also document, test and
version-control them.

In this guide, we’ll cover how to use dbt and Materialize to transform
streaming data in real time — from model building to continuous testing.

## Setup

Setting up a dbt project with Materialize is similar to setting it up
with any other database that requires a non-native adapter. To get up
and running, you need to:

1.  Install the [`dbt-materialize`
    plugin](https://pypi.org/project/dbt-materialize/) (optionally using
    a virtual environment):

    <div class="note">

    **NOTE:** The `dbt-materialize` adapter can only be used with **dbt
    Core**. Making the adapter available in dbt Cloud depends on
    prioritization by dbt Labs. If you require dbt Cloud support, please
    [reach out to the dbt Labs
    team](https://www.getdbt.com/community/join-the-community/).

    </div>

    <div class="highlight">

    ``` chroma
    python3 -m venv dbt-venv                  # create the virtual environment
    source dbt-venv/bin/activate              # activate the virtual environment
    pip install dbt-core dbt-materialize      # install dbt-core and the adapter
    ```

    </div>

    The installation will include the `dbt-postgres` dependency. To
    check that the plugin was successfully installed, run:

    <div class="highlight">

    ``` chroma
    dbt --version
    ```

    </div>

    `materialize` should be listed under “Plugins”. If this is not the
    case, double-check that the virtual environment is activated!

2.  To get started, make sure you have a Materialize account.

## Create and configure a dbt project

A [dbt
project](https://docs.getdbt.com/docs/building-a-dbt-project/projects)
is a directory that contains all dbt needs to run and keep track of your
transformations. At a minimum, it must have a project file
(`dbt_project.yml`) and at least one [model](#build-and-run-dbt-models)
(`.sql`).

To create a new project, run:

<div class="highlight">

``` chroma
dbt init <project_name>
```

</div>

This command will bootstrap a starter project with default
configurations and create a `profiles.yml` file, if it doesn’t exist. To
help you get started, the `dbt init` project includes sample models to
run the [Materialize
quickstart](/docs/self-managed/v25.2/get-started/quickstart/).

### Connect to Materialize

dbt manages all your connection configurations (or, profiles) in a file
called
[`profiles.yml`](https://docs.getdbt.com/dbt-cli/configure-your-profile).
By default, this file is located under `~/.dbt/`.

1.  Locate the `profiles.yml` file in your machine:

    <div class="highlight">

    ``` chroma
    dbt debug --config-dir
    ```

    </div>

    **Note:** If you started from an existing project but it’s your
    first time setting up dbt, it’s possible that this file doesn’t
    exist yet. You can manually create it in the suggested location.

2.  Open `profiles.yml` and adapt it to connect to Materialize using the
    reference [profile
    configuration](https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#connecting-to-materialize-with-dbt-materialize).

    As an example, the following profile would allow you to connect to
    Materialize in two different environments: a developer environment
    (`dev`) and a production environment (`prod`).

    <div class="highlight">

    ``` chroma
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

    </div>

    The `target` parameter allows you to configure the [target
    environment](https://docs.getdbt.com/docs/guides/managing-environments#how-do-i-maintain-different-environments-with-dbt)
    that dbt will use to run your models.

3.  To test the connection to Materialize, run:

    <div class="highlight">

    ``` chroma
    dbt debug
    ```

    </div>

    If the output reads `All checks passed!`, you’re good to go! The
    [dbt
    documentation](https://docs.getdbt.com/docs/guides/debugging-errors#types-of-errors)
    has some helpful pointers in case you run into errors.

## Build and run dbt models

For dbt to know how to persist (or not) a transformation, the model
needs to be associated with a
[materialization](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations)
strategy. Because Materialize is optimized for real-time transformations
of streaming data and the core of dbt is built around batch, the
`dbt-materialize` adapter implements a few custom materialization types:

| Type | Details | Config options |
|----|----|----|
| source | Creates a [source](/docs/self-managed/v25.2/sql/create-source). | cluster, indexes |
| view | Creates a [view](/docs/self-managed/v25.2/sql/create-view). | indexes |
| materialized_view | Creates a [materialized view](/docs/self-managed/v25.2/sql/create-materialized-view). The `materializedview` legacy materialization name is supported for backwards compatibility. | cluster, indexes |
| table | Creates a [materialized view](/docs/self-managed/v25.2/sql/create-materialized-view) (actual table support pending [discussion#29633](https://github.com/MaterializeInc/materialize/discussions/29633)). | cluster, indexes |
| sink | Creates a [sink](/docs/self-managed/v25.2/sql/create-sink). | cluster |
| ephemeral | Executes queries using CTEs. |  |

Create a materialization for each SQL statement you’re planning to
deploy. Each individual materialization should be stored as a `.sql`
file under the directory defined by `model-paths` in `dbt_project.yml`.

### Sources

In Materialize, a [source](/docs/self-managed/v25.2/sql/create-source)
describes an **external** system you want to read data from, and
provides details about how to decode and interpret that data. You can
instruct dbt to create a source using the custom `source`
materialization. Once a source has been defined, it can be referenced
from another model using the dbt
[`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) or
[`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
functions.

<div class="note">

**NOTE:** To create a source, you first need to [create a
connection](/docs/self-managed/v25.2/sql/create-connection) that
specifies access and authentication parameters. Connections are **not
exposed** in dbt, and need to exist before you run any `source` models.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-kafka" class="tab-pane" title="Kafka">

Create a [Kafka
source](/docs/self-managed/v25.2/sql/create-source/kafka/).

**Filename:** sources/kafka_topic_a.sql

<div class="highlight">

``` chroma
{{ config(materialized='source') }}

FROM KAFKA CONNECTION kafka_connection (TOPIC 'topic_a')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
```

</div>

The source above would be compiled to:

```
database.schema.kafka_topic_a
```

</div>

<div id="tab-postgresql" class="tab-pane" title="PostgreSQL">

Create a [PostgreSQL
source](/docs/self-managed/v25.2/sql/create-source/postgres/).

**Filename:** sources/pg.sql

<div class="highlight">

``` chroma
{{ config(materialized='source') }}

FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
FOR ALL TABLES
```

</div>

Materialize will automatically create a **subsource** for each table in
the `mz_source` publication. Pulling subsources into the dbt context
automatically isn’t supported yet. Follow the discussion in [dbt-core
\#6104](https://github.com/dbt-labs/dbt-core/discussions/6104#discussioncomment-3957001)
for updates!

A possible **workaround** is to define PostgreSQL sources as a [dbt
source](https://docs.getdbt.com/docs/build/sources) in a `.yml` file,
nested under a `sources:` key, and list each subsource under the
`tables:` key.

<div class="highlight">

``` chroma
sources:
  - name: pg
    schema: "{{ target.schema }}"
    tables:
      - name: table_a
      - name: table_b
```

</div>

Once a subsource has been defined this way, it can be referenced from
another model using the dbt
[`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
function. To ensure that dbt is able to determine the proper order to
run the models in, you should additionally force a dependency on the
parent source model (`pg`), as described in the [dbt
documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies).

**Filename:** staging/dep_subsources.sql

<div class="highlight">

``` chroma
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

</div>

The source and subsources above would be compiled to:

```
database.schema.pg
database.schema.table_a
database.schema.table_b
```

</div>

<div id="tab-mysql" class="tab-pane" title="MySQL">

Create a [MySQL
source](/docs/self-managed/v25.2/sql/create-source/mysql/).

**Filename:** sources/mysql.sql

<div class="highlight">

``` chroma
{{ config(materialized='source') }}

FROM MYSQL CONNECTION mysql_connection
FOR ALL TABLES;
```

</div>

Materialize will automatically create a **subsource** for each table in
the upstream database. Pulling subsources into the dbt context
automatically isn’t supported yet. Follow the discussion in [dbt-core
\#6104](https://github.com/dbt-labs/dbt-core/discussions/6104#discussioncomment-3957001)
for updates!

A possible **workaround** is to define MySQL sources as a [dbt
source](https://docs.getdbt.com/docs/build/sources) in a `.yml` file,
nested under a `sources:` key, and list each subsource under the
`tables:` key.

<div class="highlight">

``` chroma
sources:
  - name: mysql
    schema: "{{ target.schema }}"
    tables:
      - name: table_a
      - name: table_b
```

</div>

Once a subsource has been defined this way, it can be referenced from
another model using the dbt
[`source()`](https://docs.getdbt.com/reference/dbt-jinja-functions/source)
function. To ensure that dbt is able to determine the proper order to
run the models in, you should additionally force a dependency on the
parent source model (`mysql`), as described in the [dbt
documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies).

**Filename:** staging/dep_subsources.sql

<div class="highlight">

``` chroma
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

</div>

The source and subsources above would be compiled to:

```
database.schema.mysql
database.schema.table_a
database.schema.table_b
```

</div>

<div id="tab-webhooks" class="tab-pane" title="Webhooks">

Create a [webhook
source](/docs/self-managed/v25.2/sql/create-source/webhook/).

**Filename:** sources/webhook.sql

<div class="highlight">

``` chroma
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

</div>

The source above would be compiled to:

```
database.schema.webhook
```

</div>

</div>

</div>

### Views and materialized views

In dbt, a
[model](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#getting-started)
is a `SELECT` statement that encapsulates a data transformation you want
to run on top of your database. When you use dbt with Materialize,
**your models stay up-to-date** without manual or configured refreshes.
This allows you to efficiently transform streaming data using the same
thought process you’d use for batch transformations against any other
database.

Depending on your usage patterns, you can transform data using
[`view`](#views) or [`materialized_view`](#materialized-views) models.
For guidance and best practices on when to use views and materialized
views in Materialize, see [Indexed views vs. materialized
views](/docs/self-managed/v25.2/concepts/views/#indexed-views-vs-materialized-views).

#### Views

dbt models are materialized as
[views](/docs/self-managed/v25.2/sql/create-view) by default. Although
this means you can skip the `materialized` configuration in the model
definition to create views in Materialize, we recommend explicitly
setting the materialization type for maintainability.

**Filename:** models/view_a.sql

<div class="highlight">

``` chroma
{{ config(materialized='view') }}

SELECT
    col_a, ...
-- Reference model dependencies using the dbt ref() function
FROM {{ ref('kafka_topic_a') }}
```

</div>

The model above will be compiled to the following SQL statement:

<div class="highlight">

``` chroma
CREATE VIEW database.schema.view_a AS
SELECT
    col_a, ...
FROM database.schema.kafka_topic_a;
```

</div>

The resulting view **will not** keep results incrementally updated
without an index (see [Creating an index on a
view](#creating-an-index-on-a-view)). Once a `view` model has been
defined, it can be referenced from another model using the dbt
[`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
function.

##### Creating an index on a view

<div class="tip">

**💡 Tip:** For guidance and best practices on how to use indexes in
Materialize, see [Indexes on
views](/docs/self-managed/v25.2/concepts/indexes/#indexes-on-views).

</div>

To keep results **up-to-date** in Materialize, you can create
[indexes](/docs/self-managed/v25.2/concepts/indexes/) on view models
using the [`index` configuration](#indexes). This allows you to bypass
the need for maintaining complex incremental logic or re-running dbt to
refresh your models.

**Filename:** models/view_a.sql

<div class="highlight">

``` chroma
{{ config(materialized='view',
          indexes=[{'columns': ['col_a'], 'cluster': 'cluster_a'}]) }}

SELECT
    col_a, ...
FROM {{ ref('kafka_topic_a') }}
```

</div>

The model above will be compiled to the following SQL statements:

<div class="highlight">

``` chroma
CREATE VIEW database.schema.view_a AS
SELECT
    col_a, ...
FROM database.schema.kafka_topic_a;

CREATE INDEX database.schema.view_a_idx IN CLUSTER cluster_a ON view_a (col_a);
```

</div>

As new data arrives, indexes keep view results **incrementally updated**
in memory within a
[cluster](/docs/self-managed/v25.2/concepts/clusters/). Indexes help
optimize query performance and make queries against views fast and
computationally free.

#### Materialized views

To materialize a model as a [materialized
view](/docs/self-managed/v25.2/concepts/views/#materialized-views), set
the `materialized` configuration to `materialized_view`.

**Filename:** models/materialized_view_a.sql

<div class="highlight">

``` chroma
{{ config(materialized='materialized_view') }}

SELECT
    col_a, ...
-- Reference model dependencies using the dbt ref() function
FROM {{ ref('view_a') }}
```

</div>

The model above will be compiled to the following SQL statement:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW database.schema.materialized_view_a AS
SELECT
    col_a, ...
FROM database.schema.view_a;
```

</div>

The resulting materialized view will keep results **incrementally
updated** in durable storage as new data arrives. Once a
`materialized_view` model has been defined, it can be referenced from
another model using the dbt
[`ref()`](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
function.

##### Creating an index on a materialized view

<div class="tip">

**💡 Tip:** For guidance and best practices on how to use indexes in
Materialize, see [Indexes on materialized
views](/docs/self-managed/v25.2/concepts/views/#indexes-on-materialized-views).

</div>

With a materialized view, your models are kept **up-to-date** in
Materialize as new data arrives. This allows you to bypass the need for
maintaining complex incremental logic or re-run dbt to refresh your
models.

These results are **incrementally updated** in durable storage — which
makes them available across clusters — but aren’t optimized for
performance. To make results also available in memory within a
[cluster](/docs/self-managed/v25.2/concepts/clusters/), you can create
[indexes](/docs/self-managed/v25.2/concepts/indexes/) on materialized
view models using the [`index` configuration](#indexes).

**Filename:** models/materialized_view_a.sql

<div class="highlight">

``` chroma
{{ config(materialized='materialized_view')
          indexes=[{'columns': ['col_a'], 'cluster': 'cluster_b'}]) }}

SELECT
    col_a, ...
FROM {{ ref('view_a') }}
```

</div>

The model above will be compiled to the following SQL statements:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW database.schema.materialized_view_a AS
SELECT
    col_a, ...
FROM database.schema.view_a;

CREATE INDEX database.schema.materialized_view_a_idx IN CLUSTER cluster_b ON materialized_view_a (col_a);
```

</div>

As new data arrives, results are **incrementally updated** in durable
storage and also accessible in memory within the
[cluster](/docs/self-managed/v25.2/concepts/clusters/) the index is
created in. Indexes help optimize query performance and make queries
against materialized views faster.

##### Using refresh strategies

<div class="tip">

**💡 Tip:** For guidance and best practices on how to use refresh
strategies in Materialize, see [Refresh
strategies](/docs/self-managed/v25.2/sql/create-materialized-view/#refresh-strategies).

</div>

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

For data that doesn’t require up-to-the-second freshness, or that can be
accessed using different patterns to optimize for performance and cost
(e.g., hot vs. cold data), it might be appropriate to use a non-default
[refresh
strategy](/docs/self-managed/v25.2/sql/create-materialized-view/#refresh-strategies).

To configure a refresh strategy in a materialized view model, use the
[`refresh_interval` configuration](#configuration-refresh-strategies).
Materialized view models configured with a refresh strategy must be
deployed in a [scheduled
cluster](/docs/self-managed/v25.2/sql/create-cluster/#scheduling) for
cost savings to be significant — so you must also specify a valid
scheduled `cluster` using the [`cluster` configuration](#configuration).

**Filename:** models/materialized_view_refresh.sql

<div class="highlight">

``` chroma
{{ config(materialized='materialized_view', cluster='my_scheduled_cluster', refresh_interval={'at_creation': True, 'every': '1 day', 'aligned_to': '2024-10-22T10:40:33+00:00'}) }}

SELECT
    col_a, ...
FROM {{ ref('view_a') }}
```

</div>

The model above will be compiled to the following SQL statement:

<div class="highlight">

``` chroma
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

</div>

Materialized views configured with a refresh strategy are **not
incrementally maintained** and must recompute their results from scratch
on every refresh.

##### Using retain history

<div class="tip">

**💡 Tip:** For guidance and best practices on how to use retain history
in Materialize, see [Retain
history](/docs/self-managed/v25.2/transform-data/patterns/durable-subscriptions/#set-history-retention-period).

</div>

To configure how long historical data is retained in a materialized
view, use the `retain_history` configuration. This is useful for
maintaining a window of historical data for time-based queries or for
compliance requirements.

**Filename:** models/materialized_view_history.sql

<div class="highlight">

``` chroma
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

</div>

The model above will be compiled to the following SQL statement:

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW database.schema.materialized_view_history
WITH (RETAIN HISTORY FOR '1hr')
AS
SELECT
    col_a,
    count(*) as count
FROM database.schema.view_a
GROUP BY col_a;
```

</div>

You can specify the retention period using common time units like:

- `'1hr'` for one hour
- `'1d'` for one day
- `'1w'` for one week

### Sinks

In Materialize, a [sink](/docs/self-managed/v25.2/sql/create-sink)
describes an **external** system you want to write data to, and provides
details about how to encode that data. You can instruct dbt to create a
sink using the custom `sink` materialization.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-kafka" class="tab-pane" title="Kafka">

Create a [Kafka sink](/docs/self-managed/v25.2/sql/create-sink).

**Filename:** sinks/kafka_topic_c.sql

<div class="highlight">

``` chroma
{{ config(materialized='sink') }}

FROM {{ ref('materialized_view_a') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'topic_c')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
ENVELOPE DEBEZIUM
```

</div>

The sink above would be compiled to:

```
database.schema.kafka_topic_c
```

</div>

</div>

</div>

### Configuration: clusters, databases and indexes

#### Clusters

Use the `cluster` option to specify the
[cluster](/docs/self-managed/v25.2/sql/create-cluster/ "pools of
compute resources (CPU, memory, and scratch disk space)") in which a
`materialized_view`, `source`, `sink` model, or `index` configuration is
created. If unspecified, the default cluster for the connection is used.

<div class="highlight">

``` chroma
{{ config(materialized='materialized_view', cluster='cluster_a') }}
```

</div>

To dynamically generate the name of a cluster (e.g., based on the target
environment), you can override the `generate_cluster_name` macro with
your custom logic under the directory defined by
[macro-paths](https://docs.getdbt.com/reference/project-configs/macro-paths)
in `dbt_project.yml`.

**Filename:** macros/generate_cluster_name.sql

<div class="highlight">

``` chroma
{% macro generate_cluster_name(custom_cluster_name) -%}
    {%- if target.name == 'prod' -%}
        {{ custom_cluster_name }}
    {%- else -%}
        {{ target.name }}_{{ custom_cluster_name }}
    {%- endif -%}
{%- endmacro %}
```

</div>

#### Databases

Use the `database` option to specify the
[database](/docs/self-managed/v25.2/sql/namespaces/#database-details) in
which a `source`, `view`, `materialized_view` or `sink` is created. If
unspecified, the default database for the connection is used.

<div class="highlight">

``` chroma
{{ config(materialized='materialized_view', database='database_a') }}
```

</div>

#### Indexes

Use the `indexes` configuration to define a list of
[indexes](/docs/self-managed/v25.2/concepts/indexes/) on `source`,
`view`, `table` or `materialized view` materializations. In Materialize,
[indexes](/docs/self-managed/v25.2/concepts/indexes/) on a view maintain
view results in memory within a
[cluster](/docs/self-managed/v25.2/concepts/clusters/ "pools of compute resources (CPU,
memory, and scratch disk space)"). As the underlying data changes,
indexes **incrementally update** the view results in memory.

Each `index` configuration can have the following components:

| Component | Value | Description |
|----|----|----|
| `columns` | `list` | One or more columns on which the index is defined. To create an index that uses *all* columns, use the `default` component instead. |
| `name` | `string` | The name for the index. If unspecified, Materialize will use the materialization name and column names provided. |
| `cluster` | `string` | The cluster to use to create the index. If unspecified, indexes will be created in the cluster used to create the materialization. |
| `default` | `bool` | Default: `False`. If set to `True`, creates a [default index](/docs/self-managed/v25.2/sql/create-index/#syntax). |

##### Creating a multi-column index

<div class="highlight">

``` chroma
{{ config(materialized='view',
          indexes=[{'columns': ['col_a','col_b'], 'cluster': 'cluster_a'}]) }}
```

</div>

##### Creating a default index

<div class="highlight">

``` chroma
{{ config(materialized='view',
    indexes=[{'default': True}]) }}
```

</div>

### Configuration: refresh strategies

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

**Minimum requirements:** `dbt-materialize` v1.7.3+

Use the `refresh_interval` configuration to define [refresh
strategies](#using-refresh-strategies) for materialized view models.

The `refresh_interval` configuration can have the following components:

| Component | Value | Description |
|----|----|----|
| `at` | `string` | The specific time to refresh the materialized view at, using the [refresh at](/docs/self-managed/v25.2/sql/create-materialized-view/#refresh-at) strategy. |
| `at_creation` | `bool` | Default: `false`. Whether to trigger a first refresh when the materialized view is created. |
| `every` | `string` | The regular interval to refresh the materialized view at, using the [refresh every](/docs/self-managed/v25.2/sql/create-materialized-view/#refresh-every) strategy. |
| `aligned_to` | `string` | The *phase* of the regular interval to refresh the materialized view at, using the [refresh every](/docs/self-managed/v25.2/sql/create-materialized-view/#refresh-every) strategy. If unspecified, defaults to the time when the materialized view is created. |
| `on_commit` | `bool` | Default: `false`. Whether to use the default [refresh on commit](/docs/self-managed/v25.2/sql/create-materialized-view/#refresh-on-commit) strategy. Setting this component to `true` is equivalent to **not specifying** `refresh_interval` in the configuration block, so we recommend only using it for the special case of parametrizing the configuration option (e.g., in macros). |

### Configuration: model contracts and constraints

#### Model contracts

**Minimum requirements:** `dbt-materialize` v1.6.0+

You can enforce [model
contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
for `view`, `materialized_view` and `table` materializations to
guarantee that there are no surprise breakages to your pipelines when
the shape of the data changes.

<div class="highlight">

``` chroma
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

</div>

Setting the `contract` configuration to `enforced: true` requires you to
specify a `name` and `data_type` for every column in your models. If
there is a mismatch between the defined contract and the model you’re
trying to run, dbt will fail during compilation! Optionally, you can
also configure column-level [constraints](#constraints).

#### Constraints

**Minimum requirements:** `dbt-materialize` v1.6.1+

Materialize supports enforcing column-level `not_null`
[constraints](https://docs.getdbt.com/reference/resource-properties/constraints)
for `materialized_view` materializations. No other constraint or
materialization types are supported.

<div class="highlight">

``` chroma
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

</div>

A `not_null` constraint will be compiled to an
[`ASSERT NOT NULL`](/docs/self-managed/v25.2/sql/create-materialized-view/#non-null-assertions)
option for the specified columns of the materialize view.

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW model_with_constraints
WITH (
        ASSERT NOT NULL col_with_constraints
     )
AS
SELECT NULL AS col_with_constraints,
       2 AS col_without_constraints;
```

</div>

## Build and run dbt

1.  [Run](https://docs.getdbt.com/reference/commands/run) the dbt
    models:

    ```
    dbt run
    ```

    This command generates **executable SQL code** from any model files
    under the specified directory and runs it in the target environment.
    You can find the compiled statements under `/target/run` and
    `target/compiled` in the dbt project folder.

2.  Using the [SQL Shell](/docs/self-managed/v25.2/console/), or your
    preferred SQL client connected to Materialize, double-check that all
    objects have been created:

    <div class="highlight">

    ``` chroma
    SHOW SOURCES [FROM database.schema];
    ```

    </div>

    ```
           name
    -------------------
     mysql_table_a
     mysql_table_b
     postgres_table_a
     postgres_table_b
     kafka_topic_a
    ```

    <div class="highlight">

    ``` chroma
    SHOW VIEWS;
    ```

    </div>

    ```
           name
    -------------------
     view_a
    ```

    <div class="highlight">

    ``` chroma
    SHOW MATERIALIZED VIEWS;
    ```

    </div>

    ```
           name
    -------------------
     materialized_view_a
    ```

That’s it! From here on, Materialize makes sure that your models are
**incrementally updated** as new data streams in, and that you get
**fresh and correct results** with millisecond latency whenever you
query your views.

## Test and document a dbt project

### Configure continuous testing

Using dbt in a streaming context means that you’re able to run data
quality and integrity
[tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests)
non-stop. This is useful to monitor failures as soon as they happen, and
trigger **real-time alerts** downstream.

1.  To configure your project for continuous testing, add a `data_tests`
    property to `dbt_project.yml` with the `store_failures`
    configuration:

    <div class="highlight">

    ``` chroma
    data_tests:
      dbt_project.name:
        models:
          +store_failures: true
          +schema: 'etl_failure'
    ```

    </div>

    This will instruct dbt to create a materialized view for each
    configured test that can keep track of failures over time. By
    default, test views are created in a schema suffixed with
    `dbt_test__audit`. To specify a custom suffix, use the `schema`
    config.

    **Note:** As an alternative, you can specify the `--store-failures`
    flag when running `dbt test`.

2.  Add tests to your models using the `data_tests` property in the
    model configuration `.yml` files:

    <div class="highlight">

    ``` chroma
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

    </div>

    The type of test and the columns being tested are used as a base for
    naming the test materialized views. For example, the configuration
    above would create views named `not_null_col_a` and `unique_col_a`.

3.  Run the tests:

    <div class="highlight">

    ``` chroma
    dbt test # use --select test_type:data to only run data tests!
    ```

    </div>

    When configured to `store_failures`, this command will create a
    materialized view for each test using the respective `SELECT`
    statements, instead of doing a one-off check for failures as part of
    its execution.

    This guarantees that your tests keep running in the background as
    views that are automatically updated as soon as an assertion fails.

4.  Using the [SQL Shell](/docs/self-managed/v25.2/console/), or your
    preferred SQL client connected to Materialize, that the schema
    storing the tests has been created, as well as the test materialized
    views:

    <div class="highlight">

    ``` chroma
    SHOW SCHEMAS;
    ```

    </div>

    ```
           name
    -------------------
     public
     public_etl_failure
    ```

    <div class="highlight">

    ``` chroma
    SHOW MATERIALIZED VIEWS FROM public_etl_failure;
    ```

    </div>

    ```
           name
    -------------------
     not_null_col_a
     unique_col_a
    ```

With continuous testing in place, you can then build alerts off of the
test materialized views using any common PostgreSQL-compatible [client
library](/docs/self-managed/v25.2/integrations/client-libraries/) and
[`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/)(see the [Python
cheatsheet](/docs/self-managed/v25.2/integrations/client-libraries/python/#stream)
for a reference implementation).

### Generate documentation

dbt can automatically generate
[documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation)
for your project as a shareable website. This brings **data governance**
to your streaming pipelines, speeding up life-saving processes like data
discovery (*where* to find *what* data) and lineage (the path data takes
from source (s) to sink(s), as well as the transformations that happen
along the way).

If you’ve already created `.yml` files with helpful
[properties](https://docs.getdbt.com/reference/configs-and-properties)
about your project resources (like model and column descriptions, or
tests), you are all set.

1.  To generate documentation for your project, run:

    <div class="highlight">

    ``` chroma
    dbt docs generate
    ```

    </div>

    dbt will grab any additional project information and Materialize
    catalog metadata, then compile it into `.json` files
    (`manifest.json` and `catalog.json`, respectively) that can be used
    to feed the documentation website. You can find the compiled files
    under `/target`, in the dbt project folder.

2.  Launch the documentation website. By default, this command starts a
    web server on port 8000:

    <div class="highlight">

    ``` chroma
    dbt docs serve #--port <port>
    ```

    </div>

3.  In a browser, navigate to `localhost:8000`. There, you can find an
    overview of your dbt project, browse existing models and metadata,
    and in general keep track of what’s going on.

    If you click **View Lineage Graph** in the lower right corner, you
    can even inspect the lineage of your streaming pipelines!

    ![dbt lineage
    graph](https://user-images.githubusercontent.com/23521087/138125450-cf33284f-2a33-4c1e-8bce-35f22685213d.png)

### Persist documentation

**Minimum requirements:** `dbt-materialize` v1.6.1+

To persist model- and column-level descriptions as
[comments](/docs/self-managed/v25.2/sql/comment-on/) in Materialize, use
the
[`persist_docs`](https://docs.getdbt.com/reference/resource-configs/persist_docs)
configuration.

<div class="note">

**NOTE:** Documentation persistence is tightly coupled with `dbt run`
command invocations. For “use-at-your-own-risk” workarounds, see
[`dbt-core` \#4226](https://github.com/dbt-labs/dbt-core/issues/4226).
👻

</div>

1.  To enable docs persistence, add a `models` property to
    `dbt_project.yml` with the `persist-docs` configuration:

    <div class="highlight">

    ``` chroma
    models:
      +persist_docs:
        relation: true
        columns: true
    ```

    </div>

    As an alternative, you can configure `persist-docs` in the config
    block of your models:

    <div class="highlight">

    ``` chroma
    {{ config(
        materialized=materialized_view,
        persist_docs={"relation": true, "columns": true}
    ) }}
    ```

    </div>

2.  Once `persist-docs` is configured, any `description` defined in your
    `.yml` files is persisted to Materialize in the
    [mz_internal.mz_comments](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_comments)
    system catalog table on every `dbt run`:

    <div class="highlight">

    ``` chroma
      SELECT * FROM mz_internal.mz_comments;
    ```

    </div>

    ```

        id  |    object_type    | object_sub_id |              comment
      ------+-------------------+---------------+----------------------------------
       u622 | materialize-view  |               | materialized view a description
       u626 | materialized-view |             1 | column a description
       u626 | materialized-view |             2 | column b description
    ```

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/manage/dbt/get-started.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
