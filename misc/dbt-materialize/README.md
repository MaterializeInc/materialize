# dbt-materialize

[dbt] adapter for [Materialize].

## Installation

dbt-materialize is available on [PyPI]. To install the latest version via pip,
run:

```nofmt
pip install dbt-materialize
```

## Requirements

<!-- If you update this, bump the constraint in connections.py too. -->
dbt-materialize requires Materialize v0.20.0+.

## Configuring your profile

[Materialize] is based on the PostgreSQL database protocol, so use the
[dbt-postgres settings](https://docs.getdbt.com/docs/profile-postgres) in your
connection profile, with the following alterations:

```yml
type: materialize
user: materialize
```

Complete sample profiles can be found in
[sample_profiles.yml](dbt/include/materialize/sample_profiles.yml).

## Supported Features

### Materializations

Type               | Supported? | Details
-------------------|------------|--------
`source`           | YES        | Creates a [source].
`view`             | YES        | Creates a [view].
`materializedview` | YES        | Creates a [materialized view].
`table`            | YES        | Creates a [materialized view]. (Actual table support pending [#5266].)
`index`            | YES        | Creates an [index].
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

### Testing and Documentation

[`dbt docs`](https://docs.getdbt.com/reference/commands/cmd-docs) and [`dbt
test`](https://docs.getdbt.com/reference/commands/test) commands are supported.

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
