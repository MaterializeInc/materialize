# dbt-materialize Changelog
## 1.2.0 - 2022-08-31

* Enable additional configuration for indexes created on view,
  materializedview, or source materializations. Fix to use Materialize's
  internal naming convention when creating indexes without providing
  explicit names.

  * A new optional `name` parameter:
  ```sql
  {{ config(materialized='materializedview',
  indexes=[{'columns': ['col_1'], 'name':'col_1_idx'}]) }}
  SELECT ...
  ```

  * A new `default` parameter. Defaults to `False`. If set to `True`, will create
    default primary indexes.
  ```sql
  {{ config(materialized='materializedview',
    indexes=[{'default': True}]) }}
    SELECT ...
  ```

* Enable configuration of [clusters](https://materialize.com/docs/unstable/sql/create-cluster/#conceptual-framework),
  which are a feature in a forthcoming version of Materialize Cloud, via:

  * A new `cluster` connection parameter, which specifies the default cluster
    for the connection.
  * A new `cluster` option for materializedview and view materializations.
    For materialized views, this determines the cluster in which the materialized view
    is created. For both views and materializedviews, this also determines the cluster
    in which any indexes are created by default. If unspecified, the default cluster for
    the connection is used.

  ```sql
  {{ config(materialized='materializedview', cluster='not_default') }}
    SELECT ...
  ```

  * A new `cluster` option for indexes on view, materializedview, or source
    materializations. If 'cluster' is not supplied, indexes will be created
    in the cluster used to create the materialization.

  ```sql
  {{ config(materialized='view',
    indexes=[{'columns': ['col_1'], 'cluster': 'not_default', 'name':'col_1_idx'}]) }}
    SELECT ...
  ```
* Upgrade to `dbt-postgres` v1.2.0.

* Fully deprecate the custom index materialization.

## 1.1.3 - 2022-08-17

* Deprecate the `mz_generate_name` macro. The native Jinja function [`{
  { this }}`]
  (https://docs.getdbt.com/reference/dbt-jinja-functions/this) should be used
  to reference the relation instead.

  ```sql
  {{ config(materialized='source') }}
    CREATE SOURCE {{ this }} ...
  ```

* Fix a bug that prevented old relations from being correctly dropped on
  re-creation.

* Make custom materialization types available to dbt docs by swapping
  `pg_catalog` with `mz_catalog` metadata.

* Migrate to new `pytest` testing framework.

## 1.1.2 - 2022-06-15

* Mark the adapter as not supporting query cancellation, as Materialize does not
  support the `pg_terminate_backend` function that dbt uses to cancel queries.

## 1.1.1 - 2022-05-04

* Provide support for storing the results of a test query in a
  `materializedview` using the [`store_failures` config]
  (https://docs.getdbt.com/reference/resource-configs/store_failures).

## 1.1.0 - 2022-05-02

* Upgrade to `dbt-postgres` v1.1.0.

## 1.0.5 - 2022-04-26

* Deprecate support for custom index materialization.
* Enable defining indexes when creating a `materializedview`, `view`, or
  `source` using the `indexes` config.

  ```sql
  {{ config(materialized='view',
    indexes=[{'columns':['symbol']}]) }}
  ```

## 1.0.4 - 2022-03-27

* Upgrade to `dbt-postgres` v1.0.4.

## 1.0.3.post1 - 2022-03-16

* Produce a proper error message when attempting to use an incremental
  materialization.

## 1.0.3 - 2022-02-27

* Upgrade to `dbt-postgres` v1.0.3.

## 1.0.1.post3 - 2022-02-17

* Fix a bug introduced in v1.0.1.post1 that prevented use of the custom
  materialization types (`sink`, `source`, `index`, and `materializedview`).

## 1.0.1.post2 - 2022-02-14

* Execute hooks that specify `transaction: true` ([#7675]). In particular, this
  includes hooks that are configured as a simple string.

  Previously, `dbt-materialize` would only execute hooks that specified
  `transaction: false`. The new behavior matches the other non-transactional
  dbt adapters, which simply execute all hooks outside of a transaction
  regardless of their configured `transaction` behavior.

[#7675]: https://github.com/MaterializeInc/materialize/issues/7675

## 1.0.1.post1 - 2022-02-14

* Disable transactions. This avoids errors of the form "CREATE ... must be
  executed outside of a transaction block" ([materialize-dbt-utils#11]).

  Materialize's transactions are not powerful enough to support dbt's use cases.
  Disabling transactions follows the precedent set by the dbt-snowflake and
  dbt-bigquery adapters.

* Respect type overrides in the views created by seeds.

* Fix the implementation of the `list_relations_without_caching` macro.
  Previously it always returned an empty list of relations.

[materialize-dbt-utils#11]: https://github.com/MaterializeInc/materialize-dbt-utils/issues/11

## 1.0.1 - 2022-01-03

* Upgrade to `dbt-postgres` v1.0.1.

## 1.0.0 - 2021-12-11

* Upgrade to `dbt-postgres` v1.0.0.

## 0.21.0 - 2021-10-05

* Upgrade to `dbt-postgres` v0.21.0.

## 0.20.2 - 2021-09-08

* Upgrade to `dbt-postgres` v0.20.2.

## 0.20.1.post1 - 2021-08-18

* **Breaking change.** Remove the `mz_create_source`, `mz_drop_source`,
    `mz_create_sink`, `mz_drop_sink`, `mz_create_index`, and `mz_drop_index`
    macros as they caused incorrect behavior in `dbt docs` ([#7810]).

* Add three new custom materialization types: `source`, `index`, and `sink`.
  These replace the aforementioned macros that were removed in this release.

[#7810]: https://github.com/MaterializeInc/materialize/issues/7810

## 0.20.1 - 2021-08-12

* Upgrade to `dbt-postgres` v0.20.1.

## 0.20.0 - 2021-08-06

* Upgrade to `dbt-postgres` v0.20.0.

* Add the `mz_create_index` and `mz_drop_index` macros to manage the creation
  and deletion of indexes.

* Add the `mz_create_sink` and `mz_drop_sink` macros to manage the creation and
  deletion of sinks.

## 0.18.1.post4 - 2021-07-14

* Add the `mz_create_source` and `mz_drop_source` macros to manage the creation
  and deletion of sources, respectively.

## 0.18.1.post3 - 2021-06-17

* Support the `sslcert`, `sslkey`, and `sslrootcert` parameters for specifying a
  TLS client certificate. Notably, this permits using dbt-materialize with
  [Materialize Cloud].

[Materialize Cloud]: https://cloud.materialize.com

## 0.18.1.post2 - 2021-04-21

* Account for changes in how Materialize v0.7.3+ handles transactions.

## 0.18.1.post1 - 2021-03-17

* Fix a bug in the `get_catalog` macro which could cause column types to be
  incorrectly determined ([#6063]). This most notably caused information about
  model columns to be missing in the documentation generated by `dbt docs`.

[#6063]: https://github.com/MaterializeInc/materialize/issues/6063


## 0.18.1 - 2021-02-25

Initial release.
