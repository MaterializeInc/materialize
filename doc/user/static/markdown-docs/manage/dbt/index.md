# Use dbt to manage Materialize

How to use dbt and Materialize to transform streaming data in real time.



[dbt](https://docs.getdbt.com/docs/introduction) has become the standard for
data transformation ("the T in ELT"). It combines the accessibility of SQL with
software engineering best practices, allowing you to not only build reliable
data pipelines, but also document, test and version-control them.

Setting up a dbt project with Materialize is similar to setting it up with any
other database that requires a non-native adapter.

> **Note:** The `dbt-materialize` adapter can only be used with **dbt Core**. Making the
> adapter available in dbt Cloud depends on prioritization by dbt Labs. If you
> require dbt Cloud support, please [reach out to the dbt Labs team](https://www.getdbt.com/community/join-the-community/).



## Available guides

<div class="multilinkbox">
<div class="linkbox ">
  <div class="title">
    To get started
  </div>
  <a href="./get-started/" >Get started with dbt and Materialize</a>
</div>

<div class="linkbox ">
  <div class="title">
    Development guidelines
  </div>
  <a href="./development-workflows" >Development guidelines</a>
</div>

<div class="linkbox ">
  <div class="title">
    Deployment
  </div>
  <ul>
<li>
<p><a href="/manage/dbt/blue-green-deployments/" >Blue-green deployment guide</a></p>
</li>
<li>
<p><a href="/manage/dbt/slim-deployments/" >Slim deployment guide</a></p>
</li>
</ul>

</div>

</div>



## See also

As a tool primarily meant to manage your data model, the `dbt-materialize`
adapter does not expose all Materialize objects types. If there is a **clear
separation** between data modeling and **infrastructure management ownership**
in your team, and you want to manage objects like
[clusters](/concepts/clusters/), [connections](/sql/create-connection/), or
[secrets](/sql/create-secret/) as code, we recommend using the [Materialize
Terraform provider](/manage/terraform/) as a complementary deployment tool.



---

## Blue-green deployment


> **Tip:** Once your dbt project is ready to move out of development, or as soon as you
> start managing multiple users and deployment environments, we recommend
> checking the code in to **version control** and setting up an **automated
> workflow** to control the deployment of changes.


The `dbt-materialize` adapter ships with helper macros to automate blue/green
deployments. We recommend using the blue/green pattern any time you need to
deploy changes to the definition of objects in Materialize in production
environments and **can't tolerate downtime**.

For development environments with no downtime considerations, you might prefer
to use the [slim deployment pattern](/manage/dbt/slim-deployments/) instead for quicker
iteration and reduced CI costs.

## RBAC permissions requirements

When using blue/green deployments with [role-based access control (RBAC)](/security/cloud/access-control/#role-based-access-control-rbac), ensure that the role executing the deployment operations has sufficient privileges on the target objects:

* The role must have ownership privileges on the schemas being deployed
* The role must have ownership privileges on the clusters being deployed

These permissions are required because the blue/green deployment process needs to create, modify, and swap resources during the deployment lifecycle.

## Configuration and initialization

> **Warning:** If your dbt project includes [sinks](/manage/dbt/get-started/#sinks), you
> **must** ensure that these are created in a **dedicated schema and cluster**.
> Unlike other objects, sinks must not be recreated in the process of a blue/green
> deployment, and must instead cut over to the new definition of their upstream
> dependencies after the environment swap.


In a blue/green deployment, you first deploy your code changes to a deployment
environment ("green") that is a clone of your production environment
("blue"), in order to validate the changes without causing unavailability.
These environments are later swapped transparently.

<br>

1. In `dbt_project.yml`, use the `deployment` variable to specify the cluster(s)
   and schema(s) that contain the changes you want to deploy. The dedicated schemas and clusters for sinks shouldn't be included in your deployment configuration.

    ```yaml
    vars:
      deployment:
        default:
          clusters:
            # To specify multiple clusters, use [<cluster1_name>, <cluster2_name>].
            - <cluster_name>
          schemas:
            # to specify multiple schemas, use [<schema1_name>, <schema2_name>].
            - <schema_name>
    ```

1. Use the [`run-operation`](https://docs.getdbt.com/reference/commands/run-operation)
   command to invoke the [`deploy_init`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_init.sql)
   macro:

    ```bash
    dbt run-operation deploy_init
    ```

    This macro spins up a new cluster named `<cluster_name>_dbt_deploy` and a new
    schema named `<schema_name>_dbt_deploy` using the same configuration
    as the current environment to swap with (including privileges).

1. Run the dbt project containing the code changes against the new deployment
   environment.

    ```bash
    dbt run --vars 'deploy: True'
    ```

    The `deploy: True` variable instructs the adapter to append `_dbt_deploy` to
    the original schema or cluster specified for each model scoped for
    deployment, which transparently handles running that subset of models
    against the deployment environment.

    You must [exclude sources and sinks](/manage/dbt/development-workflows/#exclude-sources-and-sinks) when running the dbt project.

    > If you encounter an error like `String 'deploy:' is not valid YAML`, you
>   might need to use an alternative syntax depending on your terminal environment.
>   Different terminals handle quotes differently, so try:
>   ```bash
>   dbt run --vars "{\"deploy\": true}"
>   ```
>   This alternative syntax is compatible with Windows terminals, PowerShell, or
>   PyCharm Terminal.




## Validation

[//]: # "TODO(morsapaes) Expand after we make dbt test more pliable to
deployment environments."

We **strongly** recommend validating the results of the deployed changes on the
deployment environment to ensure it's safe to [cutover](#cutover-and-cleanup).

<br>

1. After deploying the changes, the objects in the deployment cluster need to
   fully hydrate before you can safely cut over. Use the `run-operation` command
   to invoke the [`deploy_await`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_await.sql)
   macro, which periodically polls the cluster readiness status, and waits for all
   objects to meet a minimum lag threshold to return successfully.

    ```bash
    dbt run-operation deploy_await #--args '{poll_interval: 30, lag_threshold: "5s"}'
    ```

    By default, `deploy_await` polls for cluster readiness every **15 seconds**,
    and waits for all objects in the deployment environment to have a lag
    of **less than 1 second** before returning successfully. To override the
    default values, you can pass the following arguments to the macro:

    Argument                             | Default   | Description
    -------------------------------------|-----------|--------------------------------------------------
    `poll_interval`                      | `15s`     | The time (in seconds) between each cluster readiness check.
    `lag_threshold`                      | `1s`      | The maximum lag threshold, which determines when all objects in the environment are considered hydrated and it's safe to perform the cutover step. **We do not recommend** changing the default value, unless prompted by the Materialize team.

2. Once `deploy_await` returns successfully, you can manually run tests against
   the new deployment environment to validate the results.

## Cutover and cleanup

> **Warning:** To avoid breakages in your production environment, we recommend **carefully
> [validating](#validation)** the results of the deployed changes in the deployment
> environment before cutting over.


1. Once `deploy_await` returns successfully and you have [validated the results](#validation)
   of the deployed changes on the deployment environment, it is safe to push the
   changes to your production environment.

   Use the `run-operation` command to invoke the [`deploy_promote`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_promote.sql)
   macro, which (atomically) swaps the environments. To perform a dry run of the
   swap, and validate the sequence of commands that dbt will execute, you can
   pass the `dry_run: True` argument to the macro.

    ```bash
    # Do a dry run to validate the sequence of commands to execute
    dbt run-operation deploy_promote --args '{dry_run: true}'
    ```

    ```bash
    # Promote the deployment environment to production
    dbt run-operation deploy_promote #--args '{wait: true, poll_interval: 30, lag_threshold: "5s"}'
    ```

    By default, `deploy_promote` **does not** wait for all objects to be
    hydrated — we recommend carefully [validating](#validation) the results of
    the deployed changes in the deployment environment before running this
    operation, or setting `--args '{wait: true}'`. To override the default
    values, you can pass the following arguments to the macro:

    Argument                             | Default   | Description
    -------------------------------------|-----------|--------------------------------------------------
    `dry_run`                            | `false`   | Whether to print out the sequence of commands that dbt will execute without actually promoting the deployment, for validation.
    `wait`                               | `false`   | Whether to wait for all objects in the deployment environment to fully hydrate before promoting the deployment. We recommend setting this argument to `true` if you skip the [validation](#validation) step.
    `poll_interval`                      | `15s`     | When `wait` is set to `true`, the time (in seconds) between each cluster readiness check.
    `lag_threshold`                      | `1s`      | When `wait` is set to `true`, the maximum lag threshold, which determines when all objects in the environment are considered hydrated and it's safe to perform the cutover step.

    > **Note:** The `deploy_promote` operation might fail if objects are
>     concurrently modified by a different session. If this occurs, re-run the
>     operation.


    This macro ensures all deployment targets, including schemas and clusters,
    are deployed together as a **single atomic operation**, and that any sinks
    that depend on changed objects are automatically cut over to the new
    definition of their upstream dependencies. If any part of the deployment
    fails, the entire deployment is rolled back to guarantee consistency and
    prevent partial updates.

1. Use the run `run-operation` command to invoke the [`deploy_cleanup`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_cleanup.sql)
   macro, which (cascade) drops the `_dbt_deploy`-suffixed cluster(s) and schema(s):

    ```bash
    dbt run-operation deploy_cleanup
    ```

   > **Note:** Any **active `SUBSCRIBE` commands** attached to the swapped
>    cluster(s) **will break**. On retry, the client will automatically connect
>    to the newly deployed cluster



---

## Development guidelines


When you're prototyping your use case and fine-tuning the underlying data model,
your priority is **iteration speed**. dbt has many features that can help speed
up development, like [node selection](#node-selection) and [model preview](#model-results-preview).
Before you start, we recommend getting familiar with how these features
work with the `dbt-materialize` adapter to make the most of your development
time.

## Node selection

By default, the `dbt-materialize` adapter drops and recreates **all** models on
each `dbt run` invocation. This can have unintended consequences, in particular
if you're managing sources and sinks as models in your dbt project. dbt allows
you to selectively run specific models and exclude specific materialization
types from each run using [node selection](https://docs.getdbt.com/reference/node-selection/syntax).

### Exclude sources and sinks

> **Note:** As you move towards productionizing your data model, we recommend managing
> sources and sinks [using Terraform](/manage/terraform/) instead.


You can manually exclude specific materialization types using the
[`exclude` flag](https://docs.getdbt.com/reference/node-selection/exclude) in
your dbt run invocations. To exclude sources and sinks, use:

```bash
dbt run --exclude config.materialized:source config.materialized:sink
```

#### YAML selectors

Instead of manually specifying node selection on each run, you can create a
[YAML selector](https://docs.getdbt.com/reference/node-selection/yaml-selectors)
that makes this the default behavior when running dbt:

```yaml
# YAML selectors should be defined in a top-level file named selectors.yml
selectors:
  - name: exclude_sources_and_sinks
    description: >
      Exclude models that use source or sink materializations in the command
      invocation.
    default: true
    definition:
      union:
        # The fqn method combined with the "*" operator selects all nodes in the
        # dbt graph
        - method: fqn
          value: "*"
        - exclude:
            - 'config.materialized:source'
            - 'config.materialized:sink'
```

Because `default: true` is specified, dbt will use the selector's criteria
whenever you run an unqualified command (e.g. `dbt build`, `dbt run`). You can
still override this default by adding selection criteria to commands, or adjust
the value of `default` depending on the target environment. To learn more about
using the `default` and `exclude` properties with YAML selectors, check the
[dbt documentation](https://docs.getdbt.com/reference/node-selection/yaml-selectors).

### Run a subset of models

You can run individual models, or groups of models, using the [`select` flag](https://docs.getdbt.com/reference/node-selection/syntax#how-does-selection-work)
in your dbt run invocations:

```bash
dbt run --select "my_dbt_project_name"   # runs all models in your project
dbt run --select "my_dbt_model"          # runs a specific model
dbt run --select "my_model+"             # select my_model and all downstream dependencies
dbt run --select "path.to.my.models"     # runs all models in a specific directory
dbt run --select "my_package.some_model" # runs a specific model in a specific package
dbt run --select "tag:nightly"           # runs models with the "nightly" tag
dbt run --select "path/to/models"        # runs models contained in path/to/models
dbt run --select "path/to/my_model.sql"  # runs a specific model by its path
```

For a full rundown of selection logic options, check the [dbt documentation](https://docs.getdbt.com/reference/node-selection/syntax).

## Model results preview

> **Note:** The `dbt show` command uses a `LIMIT` clause under the hood, which has
> [known performance limitations](/transform-data/troubleshooting/#result-filtering)
> in Materialize.


To debug and preview the results of your models **without** materializing the
results, you can use the [`dbt show`](https://docs.getdbt.com/reference/commands/show)
command:

```bash
dbt show --select "model_name.sql"

23:02:20  Running with dbt=1.7.7
23:02:20  Registered adapter: materialize=1.7.3
23:02:20  Found 3 models, 1 test, 4 seeds, 1 source, 0 exposures, 0 metrics, 430 macros, 0 groups, 0 semantic models
23:02:20
23:02:23  Previewing node 'model_name':
| col                  |
| -------------------- |
| value1               |
| value2               |
| value3               |
| value4               |
| value5               |
```

By default, the `dbt show` command will return the first 5 rows from the query
result (i.e. `LIMIT 5`). You can adjust the number of rows returned using the
`--limit n` flag.

It's important to note that previewing results compiles the model and runs the
compiled SQL against Materialize; it doesn't query the already-materialized
database relation (see [`dbt-core` #7391](https://github.com/dbt-labs/dbt-core/issues/7391)).

## Unit tests

**Minimum requirements:** `dbt-materialize` v1.8.0+

> **Note:** Complex types like [`map`](/sql/types/map/) and [`list`](/sql/types/list/) are
> not supported in unit tests yet (see [`dbt-adapters` #113](https://github.com/dbt-labs/dbt-adapters/issues/113)).
> For an overview of other known limitations, check the [dbt documentation](https://docs.getdbt.com/docs/build/unit-tests#before-you-begin).


To validate your SQL logic without fully materializing a model, as well as
future-proof it against edge cases, you can use [unit tests](https://docs.getdbt.com/docs/build/unit-tests).
Unit tests can be a **quicker way to iterate on model development** in
comparison to re-running the models, since you don't need to wait for a model
to hydrate before you can validate that it produces the expected results.

1. As an example, imagine your dbt project includes the following models:

   **Filename:** _models/my_model_a.sql_
   ```mzsql
   SELECT
     1 AS a,
     1 AS id,
     2 AS not_testing,
     'a' AS string_a,
     DATE '2020-01-02' AS date_a
   ```

   **Filename:** _models/my_model_b.sql_
   ```mzsql
   SELECT
     2 as b,
     1 as id,
     2 as c,
     'b' as string_b
   ```

   **Filename:** models/my_model.sql
   ```mzsql
   SELECT
     a+b AS c,
     CONCAT(string_a, string_b) AS string_c,
     not_testing,
     date_a
   FROM {{ ref('my_model_a')}} my_model_a
   JOIN {{ ref('my_model_b' )}} my_model_b
   ON my_model_a.id = my_model_b.id
   ```

1. To add a unit test to `my_model`, create a `.yml` file under the `/models`
   directory, and use the [`unit_tests`](https://docs.getdbt.com/reference/resource-properties/unit-tests)
   property:

   **Filename:** _models/unit_tests.yml_
   ```yaml
   unit_tests:
     - name: test_my_model
       model: my_model
       given:
         - input: ref('my_model_a')
           rows:
             - {id: 1, a: 1}
         - input: ref('my_model_b')
           rows:
             - {id: 1, b: 2}
             - {id: 2, b: 2}
       expect:
         rows:
           - {c: 2}
   ```

   For simplicity, this example provides mock data using inline dictionary
   values, but other formats are supported. Check the [dbt documentation](https://docs.getdbt.com/reference/resource-properties/data-formats)
   for a full rundown of the available options.

1. Run the unit tests using `dbt test`:

    ```bash
    dbt test --select test_type:unit

    12:30:14  Running with dbt=1.8.0
    12:30:14  Registered adapter: materialize=1.8.0
    12:30:14  Found 6 models, 1 test, 4 seeds, 1 source, 471 macros, 1 unit test
    12:30:14
    12:30:16  Concurrency: 1 threads (target='dev')
    12:30:16
    12:30:16  1 of 1 START unit_test my_model::test_my_model ................................. [RUN]
    12:30:17  1 of 1 FAIL 1 my_model::test_my_model .......................................... [FAIL 1 in 1.51s]
    12:30:17
    12:30:17  Finished running 1 unit test in 0 hours 0 minutes and 2.77 seconds (2.77s).
    12:30:17
    12:30:17  Completed with 1 error and 0 warnings:
    12:30:17
    12:30:17  Failure in unit_test test_my_model (models/models/unit_tests.yml)
    12:30:17

    actual differs from expected:

    @@ ,c
    +++,3
    ---,2
    ```

    It's important to note that the **direct upstream dependencies** of the
    model that you're unit testing **must exist** in Materialize before you can
    execute the unit test via `dbt test`. To ensure these dependencies exist,
    you can use the `--empty` flag to build an empty version of the models:

    ```bash
    dbt run --select "my_model_a.sql" "my_model_b.sql" --empty
    ```

    Alternatively, you can execute unit tests as part of the `dbt build`
    command, which will ensure the upstream depdendencies are created before
    any unit tests are executed:

    ```bash
    dbt build --select "+my_model.sql"

    11:53:30  Running with dbt=1.8.0
    11:53:30  Registered adapter: materialize=1.8.0
    ...
    11:53:33  2 of 12 START sql view model public.my_model_a ................................. [RUN]
    11:53:34  2 of 12 OK created sql view model public.my_model_a ............................ [CREATE VIEW in 0.49s]
    11:53:34  3 of 12 START sql view model public.my_model_b ................................. [RUN]
    11:53:34  3 of 12 OK created sql view model public.my_model_b ............................ [CREATE VIEW in 0.45s]
    ...
    11:53:35  11 of 12 START unit_test my_model::test_my_model ............................... [RUN]
    11:53:36  11 of 12 FAIL 1 my_model::test_my_model ........................................ [FAIL 1 in 0.84s]
    11:53:36  Failure in unit_test test_my_model (models/models/unit_tests.yml)
    11:53:36

    actual differs from expected:

    @@ ,c
    +++,3
    ---,2
    ```


---

## Get started with dbt and Materialize


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

   > **Note:** The `dbt-materialize` adapter can only be used with **dbt Core**. Making the
>     adapter available in dbt Cloud depends on prioritization by dbt Labs. If you
>     require dbt Cloud support, please [reach out to the dbt Labs team](https://www.getdbt.com/community/join-the-community/).


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

> **Note:** As a best practice, we strongly recommend using [service
> accounts](/security/cloud/users-service-accounts/create-service-accounts) to
> connect external applications, like dbt, to Materialize.


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

Type              | Details                                                                                                                                                         | Config options
------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------
source            | Creates a [source](/sql/create-source).                                                                                                                         | cluster, indexes
view              | Creates a [view](/sql/create-view).                                                                                                                             | indexes
materialized_view | Creates a [materialized view](/sql/create-materialized-view). The `materializedview` legacy materialization name is supported for backwards compatibility.                                                                                                  | cluster, indexes
table             | Creates a [materialized view](/sql/create-materialized-view) (actual table support pending [discussion#29633](https://github.com/MaterializeInc/materialize/discussions/29633)). | cluster, indexes
sink              | Creates a [sink](/sql/create-sink).                                                                                                                           |  cluster
ephemeral         | Executes queries using CTEs.


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

> **Note:** To create a source, you first need to [create a connection](/sql/create-connection)
> that specifies access and authentication parameters. Connections are **not
> exposed** in dbt, and need to exist before you run any `source` models.



**Kafka:**
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


**PostgreSQL:**
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


**MySQL:**
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


**Webhooks:**
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

> **Tip:** For guidance and best practices on how to use indexes in Materialize, see
> [Indexes on views](/concepts/indexes/#indexes-on-views).


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

> **Tip:** For guidance and best practices on how to use indexes in Materialize, see
> [Indexes on materialized views](/concepts/views/#indexes-on-materialized-views).


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

> **Tip:** For guidance and best practices on how to use refresh strategies in Materialize,
> see [Refresh strategies](/sql/create-materialized-view/#refresh-strategies).




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

> **Tip:** For guidance and best practices on how to use retain history in Materialize,
> see [Retain history](/transform-data/patterns/durable-subscriptions/#set-history-retention-period).


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


**Kafka:**
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

> **Note:** Documentation persistence is tightly coupled with `dbt run` command invocations.
> For "use-at-your-own-risk" workarounds, see [`dbt-core` #4226](https://github.com/dbt-labs/dbt-core/issues/4226). 👻


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


---

## Slim deployments


> **Tip:** Once your dbt project is ready to move out of development, or as soon as you
> start managing multiple users and deployment environments, we recommend
> checking the code in to **version control** and setting up an **automated
> workflow** to control the deployment of changes.


[//]: # "TODO(morsapaes) Consider moving demos to template repo."

On each run, dbt generates [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts)
with metadata about your dbt project, including the [_manifest file_](https://docs.getdbt.com/reference/artifacts/manifest-json)
(`manifest.json`). This file contains a complete representation of the latest
state of your project, and you can use it to **avoid re-deploying resources
that didn't change** since the last run.

We recommend using the slim deployment pattern when you want to reduce
development idle time and CI costs in development environments. For
production deployments, you should prefer the [blue/green deployment pattern](/manage/dbt/blue-green-deployments/).


> **Note:** Check [this demo](https://github.com/morsapaes/dbt-ci-templates) for a sample
> end-to-end workflow using GitHub and GitHub Actions.


1. Fetch the production `manifest.json` file into the CI environment:

    ```bash
          - name: Download production manifest from s3
            env:
              AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
              AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
              AWS_SESSION_TOKEN: ${{ secrets.AWS_SESSION_TOKEN }}
              AWS_REGION: us-east-1
            run: |
              aws s3 cp s3://mz-test-dbt/manifest.json ./manifest.json
    ```

1. Then, instruct dbt to run and test changed models and dependencies only:

    ```bash
          - name: Build dbt
            env:
              MZ_HOST: ${{ secrets.MZ_HOST }}
              MZ_USER: ${{ secrets.MZ_USER }}
              MZ_PASSWORD: ${{ secrets.MZ_PASSWORD }}
              CI_TAG: "${{ format('{0}_{1}', 'gh_ci', github.event.number ) }}"
            run: |
              source .venv/bin/activate
              dbt run-operation drop_environment
              dbt build --profiles-dir ./ --select state:modified+ --state ./ --target production
    ```

    In the example above, `--select state:modified+` instructs dbt to run all
    models that were modified (`state:modified`) and their downstream
    dependencies (`+`). Depending on your deployment requirements, you might
    want to use a different combination of state selectors, or go a step
    further and use the [`--defer`](https://docs.getdbt.com/reference/node-selection/defer)
    flag to reduce even more the number of models that need to be rebuilt.
    For a full rundown of the available [state modifier](https://docs.getdbt.com/reference/node-selection/methods#the-state-method)
    and [graph operator](https://docs.getdbt.com/reference/node-selection/graph-operators)
    options, check the [dbt documentation](https://docs.getdbt.com/reference/node-selection/syntax).

1. Every time you deploy to production, upload the new `manifest.json` file to
   blob storage (e.g. s3):

    ```bash
          - name: upload new manifest to s3
            env:
              AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
              AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
              AWS_SESSION_TOKEN: ${{ secrets.AWS_SESSION_TOKEN }}
              AWS_REGION: us-east-1
            run: |
              aws s3 cp ./target/manifest.json s3://mz-test-dbt
    ```
