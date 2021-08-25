## dbt-materialize 0.21.0.post1

### Features

- Adds three new `dbt-materialize` `materialization` types: `source`, `index`, and `sink`.
  These are intended to replace the deprecated `mz_create_object` and `mz_drop_object`
  macros. [#7836](https://github.com/MaterializeInc/materialize/pull/7836)

### Breaking changes

- Removes the `mz_create_source`, `mz_create_index`, `mz_create_sink`, `mz_drop_source`,
  `mz_drop_index`, `mz_drop_sink` macros. [#7836](https://github.com/MaterializeInc/materialize/pull/7836)
