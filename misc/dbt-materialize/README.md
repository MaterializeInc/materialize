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
`dbt-materialize` requires Materialize v0.49.0+.

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
      user: [user@domain.com]
      pass: [password]
      dbname: [database]
      schema: [dbt schema]
      cluster: [cluster] # default 'default'
      sslmode: require
      keepalives_idle: 0 # default 0

```

Complete sample profiles can be found in [sample_profiles.yml](dbt/include/materialize/sample_profiles.yml).

## Supported Features

### Materializations

Type               | Supported? | Details
-------------------|------------|--------
`source`           | YES        | Creates a [source].
`view`             | YES        | Creates a [view].
`materializedview` | YES        | Creates a [materialized view].
`table`            | YES        | Creates a [materialized view]. (Actual table support pending [#5266].)
`sink`             | YES        | Creates a [sink].
`ephemeral`        | YES        | Executes queries using CTEs.
`incremental`      | NO         | Use the `materializedview` materialization instead! dbt's incremental models are valuable because they only spend your time and money transforming your new data as it arrives. Luckily, this is exactly what Materialize's materialized views were built to do! Better yet, our materialized views will always return up-to-date results without manual or configured refreshes. For more information, check out [our documentation](https://materialize.com/docs/).

### Indexes

Use the indexes option to define a list of [indexes](/sql/create-index/) on `source`, `view`, or `materialized view` materializations. Each Materialize index can have the following components:

Component                            | Value     | Description
-------------------------------------|-----------|--------------------------------------------------
`columns`                            | `list`    | One or more columns on which the index is defined. To create an index that uses _all_ columns, use the `default` component instead.
`name`                               | `string`  | The name for the index. If unspecified, Materialize will use the materialization name and column names provided.
`cluster`                            | `string`  | The cluster to use to create the index. If unspecified, indexes will be created in the cluster used to create the materialization.
`default`                            | `bool`    | Default: `False`. If set to `True`, creates a default index that uses all columns.

### Additional macros

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

You can instruct dbt to create a [`dbt source`](https://docs.getdbt.com/docs/build/sources) in Materialize using the custom [source] materialization, which allows for injecting the complete source statement into your .sql file.

`source freshness` is not supported because using Materialize, your sources will always be fresh.

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
