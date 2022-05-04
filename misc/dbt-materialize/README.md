# dbt-materialize

[dbt] adapter for [Materialize].

For a complete step-by-step guide on how to use dbt and Materialize, check the [documentation](https://materialize.com/docs/guides/dbt/).

## Installation

`dbt-materialize` is available on [PyPI]. To install the latest version via `pip` (optionally using a virtual environment),
run:

```nofmt
python3 -m venv dbt-venv         # create the virtual environment
source dbt-venv/bin/activate     # activate the virtual environment
pip install dbt-materialize      # install the adapter
```

## Requirements

<!-- If you update this, bump the constraint in connections.py too. -->
`dbt-materialize` requires Materialize v0.20.0+.

## Configuring your profile

To connect to a Materialize instance, use the reference [profile configuration](https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile#connecting-to-materialize-with-dbt-materialize) in your
connection profile:

```yml
dbt-materialize:
  target: dev
  outputs:
    dev:
      type: materialize
      threads: 1
      host: [host]
      port: [port]
      user: materialize
      pass: [password]
      dbname: [database]
      schema: [name of your dbt schema]
```

Complete sample profiles (including for [Materialize Cloud](https://materialize.com/docs/cloud/get-started-with-cloud/#sign-up)) can be found in
[sample_profiles.yml](dbt/include/materialize/sample_profiles.yml).

## Supported Features

### Materializations

Type               | Supported? | Details
-------------------|------------|--------
`source`           | YES        | Creates a [source].
`view`             | YES        | Creates a [view].
`materializedview` | YES        | Creates a [materialized view].
`table`            | YES        | Creates a [materialized view]. (Actual table support pending [#5266].)
`index`            | YES         | (Deprecated) Creates an index. Use the `indexes` config to create indexes on `materializedview`, `view` or `source` relations instead.
`sink`             | YES        | Creates a [sink].
`ephemeral`        | YES        | Executes queries using CTEs.
`incremental`      | NO         | Use the `materializedview` materialization instead! dbt's incremental models are valuable because they only spend your time and money transforming your new data as it arrives. Luckily, this is exactly what Materialize's materialized views were built to do! Better yet, our materialized views will always return up-to-date results without manual or configured refreshes. For more information, check out [our documentation](https://materialize.com/docs/).

### Additional macros

Macro | Purpose
------|----------
`mz_generate_name(identifier)` | Generates a fully-qualified name (including the database and schema) given an object name.

We provide a `materialize-dbt-utils` package with Materialize-specific implementations of dispatched macros from `dbt-utils`. To use this package in your dbt project, check the latest installation instructions in [dbt Hub](https://hub.getdbt.com/materializeinc/materialize_dbt_utils/latest/).

### Seeds

[`dbt seed`](https://docs.getdbt.com/reference/commands/seed/) will create a
static materialized view from a CSV file. You will not be able to add to or
update this view after it has been created.

### Hooks

Not tested.

### Custom Schemas

Not tested.

### Sources

Not tested.

### Documentation
[`dbt docs`](https://docs.getdbt.com/reference/commands/cmd-docs) is supported.

### Testing
[`dbt test`](https://docs.getdbt.com/reference/commands/test) is supported.

If you set the optional `--store-failures` flag or [`store-failures` config](https://docs.getdbt.com/reference/resource-configs/store_failures), dbt will save the results of a test query to a `materializedview`.
These will be created in a schema suffixed or named `dbt_test__audit` by default. Change this value by setting a `schema` config.

### Snapshots

Not supported. Support is not planned for the near term.

## Contributors

A huge thank you to [Josh Wills](https://github.com/jwills), who created the
original version of this adapter.

[#5266]: https://github.com/MaterializeInc/materialize/issues/5266
[dbt]: https://www.getdbt.com/
[index]: https://materialize.com/docs/sql/create-index/
[Materialize]: http://materialize.com
[materialized view]: https://materialize.com/docs/sql/create-materialized-view/
[PyPI]: https://pypi.org/project/dbt-materialize/
[sink]: https://materialize.com/docs/sql/create-sink/
[source]: https://materialize.com/docs/sql/create-source/
[view]: https://materialize.com/docs/sql/create-view/
