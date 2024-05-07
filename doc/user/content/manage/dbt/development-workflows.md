---
title: "Development workflows"
description: "How to use dbt to deploy and test changes to your SQL code against Materialize."
aliases:
  - /manage/blue-green/
menu:
  main:
    parent: manage-dbt
    name: "Development workflows"
    weight: 10
---

As you progress from development to production, deploying changes to Materialize
requires different workflows. This page provides an overview of best practices
and deployment patterns across the different stages you will progress through
using [dbt](/manage/dbt/) as your deployment tool.

## Development

When you're prototyping your use case and fine-tuning the underlying data model,
your priority is **iteration speed**. dbt has many features that can help speed
up development, like [node selection](#node-selection) and [model preview](#preview-model-results).
Before you start, we recommend getting familiar with how these features
work with the `dbt-materialize` adapter to make the most of your development
time.

### Node selection

By default, the `dbt-materialize` adapter drops and recreates **all** models on
each `dbt run` invocation. This can have unintended consequences, in particular
if you're managing sources and sinks as models in your dbt project. dbt allows
you to selectively run specific models and exclude specific materialization
types from each run using [node selection](https://docs.getdbt.com/reference/node-selection/syntax).

#### Exclude sources and sinks

{{< note >}}
As you move towards productionizing your data model, we recommend managing
sources and sinks [using Terraform](#other-deployment-tools) instead.
{{</ note >}}

You can manually exclude specific materialization types using the
[`exclude` flag](https://docs.getdbt.com/reference/node-selection/exclude) in
your dbt run invocations. To exclude sources and sinks, use:

```bash
dbt run --exclude config.materialized:source config.materialized:sink
```

##### YAML selectors

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

#### Run a subset of models

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

### Preview model results

{{< note >}}
The `dbt show` command uses a `LIMIT` clause under the hood, which has
[known performance limitations](https://materialize.com/docs/transform-data/troubleshooting/#result-filtering)
in Materialize.
{{</ note >}}

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

## Deployment

Once your dbt project is ready to move out of development, or as soon as you
start managing multiple users and deployment environments, we recommend
checking the code in to **version control** and setting up an **automated
workflow** to control the deployment of changes.

Depending on the environment context and your deployment requirements, there are
two patterns that help optimize and automate your dbt runs:
[blue/green deployments](#bluegreen-deployments) and [slim deployments](#slim-deployments).

### Blue/green deployments

The `dbt-materialize` adapter ships with helper macros to automate blue/green
deployments. We recommend using the blue/green pattern any time you need to
deploy changes to the definition of objects in Materialize in production
environments and **can't tolerate downtime**.

For development environments with no downtime considerations, you might prefer
to use the [slim deployment pattern](#slim-deployments) instead for quicker
iteration and reduced CI costs.

#### Configuration and initialization

In a blue/green deployment, you first deploy your code changes to a deployment
environment ("green") that is a clone of your production environment
("blue"), in order to validate the changes without causing unavailability.
These environments are later swapped transparently.

<br>

1. In `dbt_project.yml`, use the `deployment` variable to specify the cluster(s)
   and schema(s) that contain the changes you want to deploy.

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

#### Validation

[//]: # "TODO(morsapaes) Expand after we make dbt test more pliable to
deployment environments."

We **strongly** recommend validating the results of the deployed changes on the
deployment environment to ensure it's safe to [cutover](#cutover-and-cleanup).

<br>

1. After deploying the changes, the objects in the deployment cluster need to fully
   hydrate before you can safely cut over. Use the `run-operation` command
   to invoke the [`deploy_await`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_await.sql)
   macro, which periodically polls the cluster readiness status, and waits for all
   objects to have a **lag of less than 1 second** to return successfully.

    ```bash
    dbt run-operation deploy_await
    ```

    The default poll interval is `15` seconds, but can be configured to a
    different value by passing the `poll_interval=<value>` argument in the macro
    invocation.

1. Once `deploy_await` returns successfully, you can manually run tests against
   the new deployment environment to validate the results.

#### Cutover and cleanup

{{< warning >}}
To avoid breakages in your production environment, we recommend **carefully
[validating](#validation)** the results of the deployed changes in the deployment
environment before cutting over.
{{</ warning >}}

1. Once `deploy_await` returns successfully and you have [validated the results](#validation)
   of the deployed changes on the deployment environment, it is safe to push the
   changes to your production environment.

    Use the `run-operation` command to invoke the [`deploy_promote`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_promote.sql)
     macro, which (atomically) swaps the environments.

    ```bash
    dbt run-operation deploy_promote
    ```

    {{< note >}} The `deploy_promote` operation might fail if objects are
    concurrently modified by a different session. If this occurs, re-run the
    operation.{{</ note >}}

    This macro ensures all deployment targets, including schemas and clusters, are
    deployed together as a single atomic operation. If any part of the deployment
    fails, the entire deployment is rolled back to guarantee consistency and prevent
    partial updates.

1. Use the run `run-operation` command to invoke the [`deploy_cleanup`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_cleanup.sql)
   macro, which (cascade) drops the `_dbt_deploy`-suffixed cluster(s) and schema(s):

    ```bash
    dbt run-operation deploy_cleanup
    ```

   {{< note >}}Any **active `SUBSCRIBE` commands** attached to the swapped
   cluster(s) **will break**. On retry, the client will automatically connect
   to the newly deployed cluster{{</ note >}}

### Slim deployments

[//]: # "TODO(morsapaes) Consider moving demos to template repo."

On each run, dbt generates [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts)
with metadata about your dbt project, including the [_manifest file_](https://docs.getdbt.com/reference/artifacts/manifest-json)
(`manifest.json`). This file contains a complete representation of the latest
state of your project, and you can use it to **avoid re-deploying resources
that didn't change** since the last run.

We recommend using the slim deployment pattern when you want to reduce
development idle time and CI costs in development environments. For
production deployments, you should prefer the [blue/green deployment pattern](#bluegreen-deployments).

{{< tabs tabID="1" >}}

{{< tab "GitHub Actions">}}

{{< note >}}
Check [this demo](https://github.com/morsapaes/dbt-ci-templates) for a sample
end-to-end workflow using GitHub and GitHub Actions.
{{</ note >}}

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

{{< /tab >}}
{{< /tabs >}}

## Other deployment tools

As a tool primarily meant to manage your data model, the `dbt-materialize`
adapter does not expose all Materialize objects types. If there is a **clear
separation** between data modeling and **infrastructure management ownership**
in your team, and you want to manage objects like [clusters](/get-started/key-concepts/#clusters),
[connections](/sql/create-connection/), or [secrets](/sql/create-secret/) as code,
we recommend using the [Materialize Terraform provider](/manage/terraform/) as a
complementary deployment tool.
