---
title: "Blue-green deployment"
description: "How to use dbt for blue-green deployments."
aliases:
  - /manage/blue-green/
menu:
  main:
    parent: manage-dbt
    weight: 30
---

{{< tip >}}
Once your dbt project is ready to move out of development, or as soon as you
start managing multiple users and deployment environments, we recommend
checking the code in to **version control** and setting up an **automated
workflow** to control the deployment of changes.
{{</ tip >}}

The `dbt-materialize` adapter ships with helper macros to automate blue/green
deployments. We recommend using the blue/green pattern any time you need to
deploy changes to the definition of objects in Materialize in production
environments and **can't tolerate downtime**.

For development environments with no downtime considerations, you might prefer
to use the [slim deployment pattern](/manage/dbt/slim-deployments/) instead for quicker
iteration and reduced CI costs.

## RBAC permissions requirements

When using blue/green deployments with [role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac), ensure that the role executing the deployment operations has sufficient privileges on the target objects:

* The role must have ownership privileges on the schemas being deployed
* The role must have ownership privileges on the clusters being deployed

These permissions are required because the blue/green deployment process needs to create, modify, and swap resources during the deployment lifecycle.

## Configuration and initialization

{{< warning >}}
If your dbt project includes [sinks](/manage/dbt/get-started/#sinks), you
**must** ensure that these are created in a **dedicated schema and cluster**.
Unlike other objects, sinks must not be recreated in the process of a blue/green
deployment, and must instead cut over to the new definition of their upstream
dependencies after the environment swap. The schema and cluster for your sinks should be
included in the dbt_project definition as well. 
{{</ warning >}}

In a blue/green deployment, you first deploy your code changes to a deployment
environment ("green") that is a clone of your production environment
("blue"), in order to validate the changes without causing unavailability.
These environments are later swapped transparently.

<br>

1. In `dbt_project.yml`, use the `deployment` variable to specify the cluster(s)
   and schema(s) that contain the changes you want to deploy. If you have sinks that 
   should be altered to point to the new green environment, their schemas and clusters 
   should be included as well. 

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

    {{< callout >}}
  If you encounter an error like `String 'deploy:' is not valid YAML`, you
  might need to use an alternative syntax depending on your terminal environment.
  Different terminals handle quotes differently, so try:

  ```bash
  dbt run --vars "{\"deploy\": true}"
  ```

  This alternative syntax is compatible with Windows terminals, PowerShell, or
  PyCharm Terminal.
    {{</ callout >}}

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

{{< warning >}}
To avoid breakages in your production environment, we recommend **carefully
[validating](#validation)** the results of the deployed changes in the deployment
environment before cutting over.
{{</ warning >}}

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

    {{< note >}}The `deploy_promote` operation might fail if objects are
    concurrently modified by a different session. If this occurs, re-run the
    operation.{{</ note >}}

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

   {{< note >}}
   Any **active `SUBSCRIBE` commands** attached to the swapped
   cluster(s) **will break**. On retry, the client will automatically connect
   to the newly deployed cluster
   {{</ note >}}
