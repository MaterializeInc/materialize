# Development guidelines
How to use dbt to develop and test changes to your SQL code against Materialize.
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
