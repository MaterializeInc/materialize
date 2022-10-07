---
title: "dbt and Materialize"
description: "How to use dbt and Materialize to transform streaming data in real time."
weight: 10
menu:
  main:
    parent: guides
---

{{< note >}}
The `dbt-materialize` adapter can only be used with dbt Core. We are working with the dbt community to bring native Materialize support to dbt Cloud!
{{</ note >}}

[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for data transformation ("the T in ELT"). It combines the accessibility of SQL with software engineering best practices, allowing you to not only build reliable data pipelines, but also document, test and version-control them.

While dbt is a great fit for **batch** transformations, it can only **approximate** transforming streaming data (officially through [incremental models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models), and unofficially through [lambda views](https://discourse.getdbt.com/t/how-to-create-near-real-time-models-with-just-dbt-sql/1457)). In this guide, we’ll cover how to use dbt and Materialize to transform streaming data in real time.

## Setup

**Minimum requirements:** dbt v0.18.1+

Setting up a dbt project with Materialize is similar to setting it up with any other database that requires a non-native adapter. To get up and running, you need to:

1. Install dbt by following the instructions in the [official documentation](https://docs.getdbt.com/dbt-cli/installation).

    **Note:** If you're completely new to dbt, we also recommend going through their [getting started guide](https://docs.getdbt.com/tutorial/create-a-project-dbt-cli#create-a-project) to get familiar with the CLI and core concepts.

1. Install the [`dbt-materialize` plugin](https://pypi.org/project/dbt-materialize/) (optionally using a virtual environment):

    ```bash
    python3 -m venv dbt-venv         # create the virtual environment
    source dbt-venv/bin/activate     # activate the virtual environment
    pip install dbt-materialize      # install the adapter
    ```

    To check that the plugin was successfully installed, run:

    ```bash
    dbt --version
    ```

    `materialize` should be listed under "Plugins". If this is not the case, double-check that the virtual environment is activated!

1. Last but not least, make sure you have Materialize [installed and running](/install/). You can also use the `dbt-materialize` adapter with [Materialize Cloud](/cloud/get-started-with-cloud/#sign-up).

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

1. Open `profiles.yml` and adapt it to connect to your Materialize instance using the reference [profile configuration](https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#connecting-to-materialize-with-dbt-materialize).

    As an example, the following profile would allow you to connect to Materialize in two different environments: an instance running locally (`dev`) and a Materialize Cloud instance (`prod`).

    ```yaml
    default:
      outputs:

        dev:
          type: materialize
          threads: 1
          host: localhost
          port: 6875
          user: materialize
          pass: password
          dbname: materialize
          schema: public

        prod:
          type: materialize
          threads: 1
          host: instance.materialize.cloud
          port: 6875
          user: materialize
          pass: password
          dbname: materialize
          schema: analytics
          sslmode: verify-ca
          sslcert: materialize.crt
          sslkey: materialize.key
          sslrootcert: ca.crt

      target: dev
    ```

    The `target` parameter allows you to configure the [target environment](https://docs.getdbt.com/docs/guides/managing-environments#how-do-i-maintain-different-environments-with-dbt) that dbt will use to run your models.

1. To test the connection to Materialize, run:

    ```bash
    dbt debug
    ```

    If the output reads `All checks passed!`, you're good to go! The [dbt documentation](https://docs.getdbt.com/docs/guides/debugging-errors#types-of-errors) has some helpful pointers in case you run into errors.

## Build and run dbt models

In dbt, a [model](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#getting-started) is a `SELECT` statement that encapsulates a data transformation you want to run on top of your database. For dbt to know how to persist (or not) a transformation, the model needs to be associated with a [materialization](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations) strategy.

### dbt models

When you use dbt with Materialize, **your models stay up-to-date** without manual or configured refreshes. This allows you to efficiently transform streaming data using the same thought process you'd use for batch transformations on top of any other database.

1. Create a model for each SQL statement you're planning to deploy. Each individual model should be stored as a `.sql` file under the directory defined by `source-paths` in `dbt_project.yml`.

    As an example, we'll use the SQL statements in our [getting started guide](/get-started/) and re-write them as dbt models.

    <h5>Creating a source</h5>

    You can instruct dbt to create a [source](/sql/create-source) in Materialize using the custom `source` [materialization](#materializations):

    ```sql
    {{ config(materialized='source') }}

    {% set source_name %}
        {{ mz_generate_name('market_orders_raw') }}
    {% endset %}

    CREATE SOURCE {{ source_name }}
    FROM PUBNUB
    SUBSCRIBE KEY 'sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8'
    CHANNEL 'pubnub-market-orders'
    ```

    The `mz_generate_name` [macro](https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros/#macros) allows you to generate a fully-qualified name from a base object name. Here, `source_name` would be compiled to `materialize.public.market_orders_raw`.

    <h5>Creating a view</h5>

    dbt models are materialized as `views` by default, so to create a [view](/sql/create-view) in Materialize you can simply provide the SQL statement in the model (and skip the `materialized` configuration parameter):

    ```sql
    SELECT
        ((text::jsonb)->>'bid_price')::float AS bid_price,
        (text::jsonb)->>'order_quantity' AS order_quantity,
        (text::jsonb)->>'symbol' AS symbol,
        (text::jsonb)->>'trade_type' AS trade_type,
        to_timestamp(((text::jsonb)->'timestamp')::bigint) AS ts
    FROM {{ ref('market_orders_raw') }}
    ```

    One thing to note here is that the model depends on the source defined in the previous step. To express this dependency and track the **lineage** of your project, you can use the dbt [ref()](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) function {{% gh 8744 %}}.

    <h5>Creating a materialized view</h5>

    This is where Materialize goes beyond dbt's incremental models (and traditional databases), with [materialized views](/sql/create-materialized-view) that **continuously update** as the underlying data changes:

    ```sql
    {{ config(materialized='materializedview') }}

    SELECT symbol,
           AVG(bid_price) AS avg
    FROM {{ ref('market_orders') }}
    GROUP BY symbol
    ```

    When should you use what? We recommend using `materializedview` models exclusively for your **core business logic** to ensure that you’re not consuming more memory than needed in Materialize. Intermediate or staging views should use the `view` materialization type instead.

1. [Run](https://docs.getdbt.com/reference/commands/run) the dbt models:

    ```
    dbt run
    ```

    This command generates **executable SQL code** from any model files under the specified directory and runs it in the target environment. You can find the compiled statements under `/target/run` and `target/compiled` in the dbt project folder.

1. Using a new terminal window, [connect](/connect/cli/) to Materialize to double-check that all objects have been created:

    ```bash
    psql -U materialize -h localhost -p 6875 materialize
    ```

    ```sql
    materialize=> SHOW SOURCES;

           name
    -------------------
     market_orders_raw

     materialize=> SHOW VIEWS;

           name
    -------------------
     avg_bid
     market_orders
    ```

That's it! From here on, Materialize makes sure that your models are **incrementally updated** as new data streams in, and that you get **fresh and correct results** with millisecond latency whenever you query your views.

### Materializations

dbt models are materialized as `views` by default, but can be configured to use a different materialization type through the `materialized` configuration parameter. This parameter can be set directly in the model file using:

```sql
{{ config(materialized='materializedview') }}
```

Because Materialize is optimized for real-time transformations of streaming data and the core of dbt is built around batch, the `dbt-materialize` adapter implements a few custom materialization types:

{{% dbt-materializations %}}

## Document and test a dbt project

dbt can automatically generate [documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation) for your project as a shareable website. This brings **data governance** to your streaming pipelines, speeding up life-saving processes like data discovery (_where_ to find _what_ data) and lineage (the path data takes from source(s) to sink(s), as well as the transformations that happen along the way).

1. Optionally, create a `.yml` file with helpful [properties](https://docs.getdbt.com/reference/configs-and-properties) about your project resources (like model and column descriptions, or tests) and add it to directory where your models live:

    ```yaml
    version: 2

    sources:
      - name: public
        description: "Public schema"
        tables:
          - name: market_orders_raw
            description: "Market order data source (PubNub)"
    models:
      - name: market_orders
        description: "Converts market order data to proper data types"

      - name: avg_bid
        description: "Computes the average bid price"
        columns:
          - name: symbol
            description: "The stock ticker"
            tests:
              - not_null
          - name: avg
            description: "The average bid price"
    ```

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

1. Finally, run some [tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests) on your models:

    ```bash
    dbt test
    ```

    Under the hood, dbt creates a `SELECT` query for each test that returns the rows where this assertion is _not_ true; if the test returns zero rows, the assertion passes.
