## dbt-materialize

[dbt](https://www.getdbt.com/) adapter for [Materialize](http://materialize.io).

Note: this plugin is a work in progress, and not yet suitable for production.

### Installation

This package can be installed via pip:
```nofmt
pip install dbt-materialize
```

or locally from the Materialize Github repository:
```nofmt
git clone https://github.com/MaterializeInc/materialize
pip install materialize/dbt-materialize
```

### Configuring your profile

[Materialize](http://materialize.io) is based on the Postgres database protocols, so use the
[dbt postgres settings](https://docs.getdbt.com/docs/profile-postgres) in your connection profile,
with the following alterations:
- `type: materialize`
- `user: materialize`

Complete sample profiles can be found in [sample_profiles.yml](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/sample_profiles.yml).

## Supported Features

### Materializations

Type | Supported? | Details
-----|------------|----------------
view | YES | Creates a [view](https://materialize.com/docs/sql/create-view/#main).
materializedview | YES | Creates a [materialized view](https://materialize.com/docs/sql/create-materialized-view/#main).
table | YES | Creates a [materialized view](https://materialize.com/docs/sql/create-materialized-view/#main). (Actual table support pending [#5266](https://github.com/MaterializeInc/materialize/issues/5266))
ephemeral | YES | Executes queries using CTEs.
incremental | NO | Use the `materializedview` materialization instead! dbt's incremental models are valuable because they only spend your time and money transforming your new data as it arrives. Luckily, this is exactly what Materialize's materialized views were built to do! Better yet, our materialized views will always return up-to-date results without manual or configured refreshes. For more information, check out [our documentation](https://materialize.com/docs/).

### Additional macros

dbt only supports a limited set of [materialization types](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations). To create other types of objects in Materialize
via dbt, use the following Materialize-specific macros:

Macro | Purpose
------|----------
mz_generate_name(identifier) | Generates a fully-qualified name (including the database and schema) given an object name.
mz_create_source(sql) | Given some [`CREATE SOURCE`](https://materialize.com/docs/sql/create-source/) sql statement, creates the source in the Materialize instance.
mz_drop_source(name, if_exists, cascade) | [Drops the named source](https://materialize.com/docs/sql/drop-source/) in Materialize.
mz_create_index(obj_name, default, idx_name, col_refs, with_options) | [Creates an index](https://materialize.com/docs/sql/create-index/) in Materialize.
mz_drop_index(name, default, if_exists, cascade) | [Drops an index](https://materialize.com/docs/sql/drop-index/) in Materialize.

### Additional macro details

#### mz_drop_source(name, if_exists, cascade)

Argument | Type | Detail
---------|------|--------
name     | string | Name of the source to drop
if_exists | boolean | Does not return an error if the named source does not exist
cascade | boolean | Drops the source and its dependent objects

#### mz_create_index(obj_name, name, default, col_refs, with_options)

Argument | Type | Detail
---------|------|--------
obj_name | string | Name of the source or view to create an index on
name | optional string | If not a `default` index, the name of the index to create
default | boolean | `True` will create a default index on the named source or view
col_refs | optional list of strings | The columns to use as the key into the index
with_options | optional list of strings | The name of index parameters to set as val

#### mz_drop_index(name, default, if_exists, cascade)

Argument | Type | Detail
---------|------|--------
name | string | Name of the index to drop
default | boolean | `True` will drop the default index
if_exists | boolean | Does not return an error if the named index does not exist
cascade | boolean | Drops the index and its dependent objects

### Seeds

[`dbt seed`](https://docs.getdbt.com/reference/commands/seed/) will create a static materialized
view from a CSV file. You will not be able to add to or update this view after it has been created.

### Hooks

Not tested.

### Custom Schemas

Not tested.

### Sources

Not tested.

### Testing and Documentation

[`dbt docs`](https://docs.getdbt.com/reference/commands/cmd-docs) and [`dbt test`](https://docs.getdbt.com/reference/commands/test)
commands are supported.

### Snapshots

Not supported, will likely not be supported in the near term.

## Contributors

A huge thank you to [Josh Wills](https://github.com/jwills), who originally created this adapter.
