<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Manage Materialize](/docs/manage/)  /  [Use dbt to
manage Materialize](/docs/manage/dbt/)

</div>

# Development guidelines

When you’re prototyping your use case and fine-tuning the underlying
data model, your priority is **iteration speed**. dbt has many features
that can help speed up development, like [node
selection](#node-selection) and [model preview](#model-results-preview).
Before you start, we recommend getting familiar with how these features
work with the `dbt-materialize` adapter to make the most of your
development time.

## Node selection

By default, the `dbt-materialize` adapter drops and recreates **all**
models on each `dbt run` invocation. This can have unintended
consequences, in particular if you’re managing sources and sinks as
models in your dbt project. dbt allows you to selectively run specific
models and exclude specific materialization types from each run using
[node
selection](https://docs.getdbt.com/reference/node-selection/syntax).

### Exclude sources and sinks

<div class="note">

**NOTE:** As you move towards productionizing your data model, we
recommend managing sources and sinks [using
Terraform](/docs/manage/terraform/) instead.

</div>

You can manually exclude specific materialization types using the
[`exclude`
flag](https://docs.getdbt.com/reference/node-selection/exclude) in your
dbt run invocations. To exclude sources and sinks, use:

<div class="highlight">

``` chroma
dbt run --exclude config.materialized:source config.materialized:sink
```

</div>

#### YAML selectors

Instead of manually specifying node selection on each run, you can
create a [YAML
selector](https://docs.getdbt.com/reference/node-selection/yaml-selectors)
that makes this the default behavior when running dbt:

<div class="highlight">

``` chroma
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

</div>

Because `default: true` is specified, dbt will use the selector’s
criteria whenever you run an unqualified command (e.g. `dbt build`,
`dbt run`). You can still override this default by adding selection
criteria to commands, or adjust the value of `default` depending on the
target environment. To learn more about using the `default` and
`exclude` properties with YAML selectors, check the [dbt
documentation](https://docs.getdbt.com/reference/node-selection/yaml-selectors).

### Run a subset of models

You can run individual models, or groups of models, using the [`select`
flag](https://docs.getdbt.com/reference/node-selection/syntax#how-does-selection-work)
in your dbt run invocations:

<div class="highlight">

``` chroma
dbt run --select "my_dbt_project_name"   # runs all models in your project
dbt run --select "my_dbt_model"          # runs a specific model
dbt run --select "my_model+"             # select my_model and all downstream dependencies
dbt run --select "path.to.my.models"     # runs all models in a specific directory
dbt run --select "my_package.some_model" # runs a specific model in a specific package
dbt run --select "tag:nightly"           # runs models with the "nightly" tag
dbt run --select "path/to/models"        # runs models contained in path/to/models
dbt run --select "path/to/my_model.sql"  # runs a specific model by its path
```

</div>

For a full rundown of selection logic options, check the [dbt
documentation](https://docs.getdbt.com/reference/node-selection/syntax).

## Model results preview

<div class="note">

**NOTE:** The `dbt show` command uses a `LIMIT` clause under the hood,
which has [known performance
limitations](/docs/transform-data/troubleshooting/#result-filtering) in
Materialize.

</div>

To debug and preview the results of your models **without**
materializing the results, you can use the
[`dbt show`](https://docs.getdbt.com/reference/commands/show) command:

<div class="highlight">

``` chroma
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

</div>

By default, the `dbt show` command will return the first 5 rows from the
query result (i.e. `LIMIT 5`). You can adjust the number of rows
returned using the `--limit n` flag.

It’s important to note that previewing results compiles the model and
runs the compiled SQL against Materialize; it doesn’t query the
already-materialized database relation (see [`dbt-core`
\#7391](https://github.com/dbt-labs/dbt-core/issues/7391)).

## Unit tests

**Minimum requirements:** `dbt-materialize` v1.8.0+

<div class="note">

**NOTE:** Complex types like [`map`](/docs/sql/types/map/) and
[`list`](/docs/sql/types/list/) are not supported in unit tests yet (see
[`dbt-adapters`
\#113](https://github.com/dbt-labs/dbt-adapters/issues/113)). For an
overview of other known limitations, check the [dbt
documentation](https://docs.getdbt.com/docs/build/unit-tests#before-you-begin).

</div>

To validate your SQL logic without fully materializing a model, as well
as future-proof it against edge cases, you can use [unit
tests](https://docs.getdbt.com/docs/build/unit-tests). Unit tests can be
a **quicker way to iterate on model development** in comparison to
re-running the models, since you don’t need to wait for a model to
hydrate before you can validate that it produces the expected results.

1.  As an example, imagine your dbt project includes the following
    models:

    **Filename:** *models/my_model_a.sql*

    <div class="highlight">

    ``` chroma
    SELECT
      1 AS a,
      1 AS id,
      2 AS not_testing,
      'a' AS string_a,
      DATE '2020-01-02' AS date_a
    ```

    </div>

    **Filename:** *models/my_model_b.sql*

    <div class="highlight">

    ``` chroma
    SELECT
      2 as b,
      1 as id,
      2 as c,
      'b' as string_b
    ```

    </div>

    **Filename:** models/my_model.sql

    <div class="highlight">

    ``` chroma
    SELECT
      a+b AS c,
      CONCAT(string_a, string_b) AS string_c,
      not_testing,
      date_a
    FROM {{ ref('my_model_a')}} my_model_a
    JOIN {{ ref('my_model_b' )}} my_model_b
    ON my_model_a.id = my_model_b.id
    ```

    </div>

2.  To add a unit test to `my_model`, create a `.yml` file under the
    `/models` directory, and use the
    [`unit_tests`](https://docs.getdbt.com/reference/resource-properties/unit-tests)
    property:

    **Filename:** *models/unit_tests.yml*

    <div class="highlight">

    ``` chroma
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

    </div>

    For simplicity, this example provides mock data using inline
    dictionary values, but other formats are supported. Check the [dbt
    documentation](https://docs.getdbt.com/reference/resource-properties/data-formats)
    for a full rundown of the available options.

3.  Run the unit tests using `dbt test`:

    <div class="highlight">

    ``` chroma
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

    </div>

    It’s important to note that the **direct upstream dependencies** of
    the model that you’re unit testing **must exist** in Materialize
    before you can execute the unit test via `dbt test`. To ensure these
    dependencies exist, you can use the `--empty` flag to build an empty
    version of the models:

    <div class="highlight">

    ``` chroma
    dbt run --select "my_model_a.sql" "my_model_b.sql" --empty
    ```

    </div>

    Alternatively, you can execute unit tests as part of the `dbt build`
    command, which will ensure the upstream depdendencies are created
    before any unit tests are executed:

    <div class="highlight">

    ``` chroma
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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/manage/dbt/development-workflows.md"
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
