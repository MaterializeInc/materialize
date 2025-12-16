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

# Blue-green deployment

<div class="tip">

**💡 Tip:** Once your dbt project is ready to move out of development,
or as soon as you start managing multiple users and deployment
environments, we recommend checking the code in to **version control**
and setting up an **automated workflow** to control the deployment of
changes.

</div>

The `dbt-materialize` adapter ships with helper macros to automate
blue/green deployments. We recommend using the blue/green pattern any
time you need to deploy changes to the definition of objects in
Materialize in production environments and **can’t tolerate downtime**.

For development environments with no downtime considerations, you might
prefer to use the [slim deployment
pattern](/docs/self-managed/v25.2/manage/dbt/slim-deployments/) instead
for quicker iteration and reduced CI costs.

## RBAC permissions requirements

When using blue/green deployments with [role-based access control
(RBAC)](/docs/self-managed/v25.2/manage/access-control/#role-based-access-control-rbac),
ensure that the role executing the deployment operations has sufficient
privileges on the target objects:

- The role must have ownership privileges on the schemas being deployed
- The role must have ownership privileges on the clusters being deployed

These permissions are required because the blue/green deployment process
needs to create, modify, and swap resources during the deployment
lifecycle.

## Configuration and initialization

<div class="warning">

**WARNING!** If your dbt project includes
[sinks](/docs/self-managed/v25.2/manage/dbt/get-started/#sinks), you
**must** ensure that these are created in a **dedicated schema and
cluster**. Unlike other objects, sinks must not be recreated in the
process of a blue/green deployment, and must instead cut over to the new
definition of their upstream dependencies after the environment swap.

</div>

In a blue/green deployment, you first deploy your code changes to a
deployment environment (“green”) that is a clone of your production
environment (“blue”), in order to validate the changes without causing
unavailability. These environments are later swapped transparently.

  

1.  In `dbt_project.yml`, use the `deployment` variable to specify the
    cluster(s) and schema(s) that contain the changes you want to
    deploy. The dedicated schemas and clusters for sinks shouldn’t be
    included in your deployment configuration.

    <div class="highlight">

    ``` chroma
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

    </div>

2.  Use the
    [`run-operation`](https://docs.getdbt.com/reference/commands/run-operation)
    command to invoke the
    [`deploy_init`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_init.sql)
    macro:

    <div class="highlight">

    ``` chroma
    dbt run-operation deploy_init
    ```

    </div>

    This macro spins up a new cluster named `<cluster_name>_dbt_deploy`
    and a new schema named `<schema_name>_dbt_deploy` using the same
    configuration as the current environment to swap with (including
    privileges).

3.  Run the dbt project containing the code changes against the new
    deployment environment.

    <div class="highlight">

    ``` chroma
    dbt run --vars 'deploy: True'
    ```

    </div>

    The `deploy: True` variable instructs the adapter to append
    `_dbt_deploy` to the original schema or cluster specified for each
    model scoped for deployment, which transparently handles running
    that subset of models against the deployment environment.

    You must [exclude sources and
    sinks](/docs/self-managed/v25.2/manage/dbt/development-workflows/#exclude-sources-and-sinks)
    when running the dbt project.

    <div class="callout">

    <div>

    If you encounter an error like `String 'deploy:' is not valid YAML`,
    you might need to use an alternative syntax depending on your
    terminal environment. Different terminals handle quotes differently,
    so try:

    <div class="highlight">

    ``` chroma
    dbt run --vars "{\"deploy\": true}"
    ```

    </div>

    This alternative syntax is compatible with Windows terminals,
    PowerShell, or PyCharm Terminal.

    </div>

    </div>

## Validation

We **strongly** recommend validating the results of the deployed changes
on the deployment environment to ensure it’s safe to
[cutover](#cutover-and-cleanup).

  

1.  After deploying the changes, the objects in the deployment cluster
    need to fully hydrate before you can safely cut over. Use the
    `run-operation` command to invoke the
    [`deploy_await`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_await.sql)
    macro, which periodically polls the cluster readiness status, and
    waits for all objects to meet a minimum lag threshold to return
    successfully.

    <div class="highlight">

    ``` chroma
    dbt run-operation deploy_await #--args '{poll_interval: 30, lag_threshold: "5s"}'
    ```

    </div>

    By default, `deploy_await` polls for cluster readiness every **15
    seconds**, and waits for all objects in the deployment environment
    to have a lag of **less than 1 second** before returning
    successfully. To override the default values, you can pass the
    following arguments to the macro:

    | Argument | Default | Description |
    |----|----|----|
    | `poll_interval` | `15s` | The time (in seconds) between each cluster readiness check. |
    | `lag_threshold` | `1s` | The maximum lag threshold, which determines when all objects in the environment are considered hydrated and it’s safe to perform the cutover step. **We do not recommend** changing the default value, unless prompted by the Materialize team. |

2.  Once `deploy_await` returns successfully, you can manually run tests
    against the new deployment environment to validate the results.

## Cutover and cleanup

<div class="warning">

**WARNING!** To avoid breakages in your production environment, we
recommend **carefully [validating](#validation)** the results of the
deployed changes in the deployment environment before cutting over.

</div>

1.  Once `deploy_await` returns successfully and you have [validated the
    results](#validation) of the deployed changes on the deployment
    environment, it is safe to push the changes to your production
    environment.

    Use the `run-operation` command to invoke the
    [`deploy_promote`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_promote.sql)
    macro, which (atomically) swaps the environments. To perform a dry
    run of the swap, and validate the sequence of commands that dbt will
    execute, you can pass the `dry_run: True` argument to the macro.

    <div class="highlight">

    ``` chroma
    # Do a dry run to validate the sequence of commands to execute
    dbt run-operation deploy_promote --args '{dry_run: true}'
    ```

    </div>

    <div class="highlight">

    ``` chroma
    # Promote the deployment environment to production
    dbt run-operation deploy_promote #--args '{wait: true, poll_interval: 30, lag_threshold: "5s"}'
    ```

    </div>

    By default, `deploy_promote` **does not** wait for all objects to be
    hydrated — we recommend carefully [validating](#validation) the
    results of the deployed changes in the deployment environment before
    running this operation, or setting `--args '{wait: true}'`. To
    override the default values, you can pass the following arguments to
    the macro:

    | Argument | Default | Description |
    |----|----|----|
    | `dry_run` | `false` | Whether to print out the sequence of commands that dbt will execute without actually promoting the deployment, for validation. |
    | `wait` | `false` | Whether to wait for all objects in the deployment environment to fully hydrate before promoting the deployment. We recommend setting this argument to `true` if you skip the [validation](#validation) step. |
    | `poll_interval` | `15s` | When `wait` is set to `true`, the time (in seconds) between each cluster readiness check. |
    | `lag_threshold` | `1s` | When `wait` is set to `true`, the maximum lag threshold, which determines when all objects in the environment are considered hydrated and it’s safe to perform the cutover step. |

    <div class="note">

    **NOTE:** The `deploy_promote` operation might fail if objects are
    concurrently modified by a different session. If this occurs, re-run
    the operation.

    </div>

    This macro ensures all deployment targets, including schemas and
    clusters, are deployed together as a **single atomic operation**,
    and that any sinks that depend on changed objects are automatically
    cut over to the new definition of their upstream dependencies. If
    any part of the deployment fails, the entire deployment is rolled
    back to guarantee consistency and prevent partial updates.

2.  Use the run `run-operation` command to invoke the
    [`deploy_cleanup`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_cleanup.sql)
    macro, which (cascade) drops the `_dbt_deploy`-suffixed cluster(s)
    and schema(s):

    <div class="highlight">

    ``` chroma
    dbt run-operation deploy_cleanup
    ```

    </div>

    <div class="note">

    **NOTE:** Any **active `SUBSCRIBE` commands** attached to the
    swapped cluster(s) **will break**. On retry, the client will
    automatically connect to the newly deployed cluster

    </div>

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/manage/dbt/blue-green-deployments.md"
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
