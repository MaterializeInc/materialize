# dbt-materialize Changelog

## 1.6.1 - 2023-11-03

* Support the [`ASSERT NOT NULL` option](https://materialize.com/docs/sql/create-materialized-view/#non-null-assertions)
  for `materialized_view` materializations via the `not_null` column-level
  constraint.

  ```yaml
    - name: model_with_constraints
    config:
      contract:
        enforced: true
    columns:
      - name: col_with_constraints
        data_type: string
        constraints:
          - type: not_null
      - name: col_without_constraints
        data_type: int
  ```

  It's important to note that other constraint types are not
  supported, and that `not_null` constraints can only be defined at the
  column-level (not model-level).

* Work around a bug in [`--persist-docs`](https://docs.getdbt.com/reference/resource-configs/persist_docs)
  that prevented comments from being persisted for `materialized_view`
  materializations. See [#21878]
  (https://github.com/MaterializeInc/materialize/pull/21878) for details.

  The `--persist-docs` flag requires [Materialize >=0.68.0](https://materialize.com/docs/releases/v0.68/).
  Previous versions **do not** have support for the `COMMENT ON` syntax, which
  is required to persist resource descriptions as column and relation comments
  in Materialize.

* Load seeds into tables rather than materialized views.

  For historical reasons, `dbt-materialize` has loaded seed data by injecting
  the values from the CSV file in a `CREATE MATERIALIZED VIEW AS ...`
  statement. `dbt-materialize` now creates a table and loads the values from
  the CSV into that file, matching the behavior of other dbt adapters.

## 1.6.0 - 2023-10-12

* Upgrade to `dbt-postgres` v1.6.0:

  * Support [model contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
    for `view`, `materialized_view` and `table` materializations.
    Materialize does not have a notion of constraints, so [model- and column-level constraints](https://docs.getdbt.com/reference/resource-properties/constraints)
    are **not supported**.

  * Deprecate the custom `materializedview` materialization name in favor of
    `materialized_view`, which is built-in from dbt v1.6.

    **New**

    ```sql
    {{ config( materialized = 'materialized_view' )}}
    ```

    **Deprecated**

    ```sql
    {{ config( materialized = 'materializedview' )}}
    ```

    The deprecated materialization name will be removed in a future release of the
    adapter.

* Enable the `cluster` configuration for tests, which allows specifying a target
  cluster for `dbt test` to run against (for both one-shot and [continuous testing](https://materialize.com/docs/manage/dbt/#configure-continuous-testing)).

  ```yaml
  tests:
    example:
      +store_failures: true
      +schema: 'dbt_test_schema'
      +cluster: 'dbt_test_cluster'
  ```

* Override the `dbt init` command to generate a project based on the [quickstart](https://materialize.com/docs/get-started/quickstart/),
  instead of the default project generated in `dbt-core`.

* **Breaking change.** Set 255 as the maximum identifier length for relation
    names, after [#20999](https://github.com/MaterializeInc/materialize/pull/20999)
    introduced a `max_identifier_length` session variable that enforces this
    limit in Materialize.

* Support cancelling outstanding queries when pressing Ctrl+C.

## 1.5.1 - 2023-07-24

* Enable the `indexes` config for `table` materializations.

## 1.5.0 - 2023-07-13

* Upgrade to `dbt-postgres` v1.5.0. dbt contracts and dbt constraints are **not
  supported** in this release (see [dbt-core #7213](https://github.com/dbt-labs/dbt-core/discussions/7213#discussioncomment-5903205)).

* Fix a bug in the `materialize__list_relations_without_caching` macro which
  could cause the adapter to break for multi-output sources ([#20483](https://github.com/MaterializeInc/materialize/issues/20483)).

* Expose `owner` in the dbt documentation, now that Materialize supports
  [role-based access control (RBAC)](https://materialize.com/docs/manage/access-control/).

## 1.4.1 - 2023-04-28

* Let Materialize automatically run introspection queries in the
  `mz_introspection` cluster via the new `auto_route_introspection_queries`
  session variable, instead of hardcoding the cluster on connection.

  This change requires [Materialize >=0.49.0](https://materialize.com/docs/releases/v0.49/).
  **Users of older versions should pin `dbt-materialize` to `v1.4.0`.**

## 1.4.0 - 2023-02-03

* Upgrade to `dbt-postgres` v1.4.0.

## 1.3.4 - 2023-01-19

* Fix a bug where the adapter would fail if the pre-installed `default` cluster
  doesn't exist in Materialize (i.e. in case it was dropped by the user).

## 1.3.3 - 2023-01-05

* Remove the 63-character limitation on relation names. Materialize does not
  have this limitation, unlike PostgreSQL (see [dbt-core #2727](https://github.com/dbt-labs/dbt-core/pull/2727)).

* Produce an error message when attempting to use the [`listagg`](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros#listagg)
  cross-database macro. Materialize has native support for [`list_agg()`](https://materialize.com/docs/sql/functions/list_agg/),
  which should be used instead.

* Remove the deprecated `mz_generate_name` macro.

## 1.3.2 - 2022-11-17

* Add the `IN CLUSTER` clause to the custom seed materialization to ensure that
  seeds are created in the target cluster, rather than in the active cluster.
  This fixes a bug in 1.3.1 where seeds would run against the
  `mz_introspection` cluster, causing an error.

## 1.3.1 - 2022-11-15

* **Breaking change.** Use `mz_introspection` as the initial active cluster on
    connection to ensure optimal performance for introspection queries.

  This change requires [Materialize >=0.28.0](https://materialize.com/docs/releases/v0.28/).
  **Users of older versions should pin `dbt-materialize` to `v1.3.0`.**

## 1.3.0 - 2022-11-09

* Upgrade to `dbt-postgres` v1.3.0.

* Migrate cross-database macros from [`materialize-dbt-utils`](https://github.com/MaterializeInc/materialize-dbt-utils)
  into the adapter, as a result of [dbt-core #5298](https://github.com/dbt-labs/dbt-core/pull/5298).
  The `utils` macros will be deprecated in the upcoming release of the package,
  and removed in a subsequent release.

## 1.2.1 - 2022-11-01

* Add `cluster` to the connection parameters returned on `dbt debug`.

* Disallow the `cluster` option for `view` materializations. In the new
  architecture, only materialized views and indexes are associated with a
  cluster.

## 1.2.0 - 2022-08-31

* Enable additional configuration for indexes created on `view`,
  `materializedview`, or `source` materializations. Fix to use Materialize's
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
  which are a feature in a forthcoming version of Materialize, via:

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
    materializations. If `cluster` is not supplied, indexes will be created
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
  TLS client certificate. Notably, this allows using `dbt-materialize` with the
  new architecture of [Materialize](https://console.materialize.com).

## 0.18.1.post2 - 2021-04-21

* Account for changes in how Materialize v0.7.3+ handles transactions.

## 0.18.1.post1 - 2021-03-17

* Fix a bug in the `get_catalog` macro which could cause column types to be
  incorrectly determined ([#6063]). This most notably caused information about
  model columns to be missing in the documentation generated by `dbt docs`.

[#6063]: https://github.com/MaterializeInc/materialize/issues/6063


## 0.18.1 - 2021-02-25

Initial release.
