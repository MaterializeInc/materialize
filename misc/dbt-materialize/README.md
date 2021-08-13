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
source | YES | Creates a [source](https://materialize.com/docs/sql/create-source/).
view | YES | Creates a [view](https://materialize.com/docs/sql/create-view/#main).
materializedview | YES | Creates a [materialized view](https://materialize.com/docs/sql/create-materialized-view/#main).
table | YES | Creates a [materialized view](https://materialize.com/docs/sql/create-materialized-view/#main). (Actual table support pending [#5266](https://github.com/MaterializeInc/materialize/issues/5266))
index | YES | Creates an [index](https://materialize.com/docs/sql/create-index/#main).
sink | YES | Creates a [sink](https://materialize.com/docs/sql/create-sink/#main).
ephemeral | YES | Executes queries using CTEs.
incremental | NO | Use the `materializedview` materialization instead! dbt's incremental models are valuable because they only spend your time and money transforming your new data as it arrives. Luckily, this is exactly what Materialize's materialized views were built to do! Better yet, our materialized views will always return up-to-date results without manual or configured refreshes. For more information, check out [our documentation](https://materialize.com/docs/).

### Additional macros

Macro | Purpose
------|----------
mz_generate_name(identifier) | Generates a fully-qualified name (including the database and schema) given an object name.

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
