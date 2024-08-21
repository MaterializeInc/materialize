# dbt-materialize Changelog

## 1.8.5 - 2024-08-21

* Fix a bug in the `materialize__drop_relation` macro that prevented using the
  [`--full-refresh` flag](https://docs.getdbt.com/reference/resource-configs/full_refresh)
  (or the `full_refresh` configuration) with `dbt seed`.

* Fix a bug that would prevent `dbt seed` from succeeding on subsequent runs
  without a valid default cluster. It's important to note that this scenario
  will still fail if no cluster is specified for the target in
  `profiles.yml` _and_ the default cluster for the user is invalid
  (or intentionally set to `mz_catalog_server`, which cannot query user data).

* Produce an error message when attempting to use the [`grants` configuration](https://docs.getdbt.com/reference/resource-configs/grants),
  which is not supported in the adapter. This configuration will be supported in
  the future (see [#20244](https://github.com/MaterializeInc/materialize/issues/20244)).

* Stop hardcoding `quickstart` as the default cluster to fall back to when no
  cluster is specified. When no cluster is specified, either in `profiles.yml`
  or as a configuration, we should default to the default cluster configured
  for the connected dbt user (or, the active cluster for the connection). This
  will still fail if the defalt cluster for the connected user is invalid or
  set to `mz_catalog_server` (which cannot be modified).

## 1.8.4 - 2024-08-07

* Include the dbt version in the `application_name` connection parameter [#28813](https://github.com/MaterializeInc/materialize/pull/28813).

* Allow users to override the maximum lag threshold in [blue/green deployment
  macros](https://materialize.com/docs/manage/dbt/development-workflows/#bluegreen-deployments)
  using the new `lag_threshold` argument.

  **Example**
  ```bash
  dbt run-operation deploy_await --args '{lag_threshold: "5s"}'
  dbt run-operation deploy_promote --args '{wait: true, poll_interval: 30, lag_threshold: "5s"}'
  ```

  **We do not recommend** changing the default value of the `lag_threshold`
  (`1s`), unless prompted by the Materialize team.

## 1.8.3 - 2024-07-19

* Enable cross-database references ([#27686](https://github.com/MaterializeInc/materialize/pull/27686)). Although cross-database references are not supported in `dbt-postgres`, databases in Materialize are purely used for namespacing, and therefore do not present the same constraint.

* Add the `create_cluster` and `drop_cluster` macros, which allow managing the
  creation and deletion of clusters in workflows requiring transient
  infrastructure (e.g. CI/CD).

## 1.8.2 - 2024-06-21

* Add support for sink cutover to the blue/green deployment workflow [#27557](https://github.com/MaterializeInc/materialize/pull/27557).
  Sinks **must** be created in a **dedicated schema and cluster**.

* Add a `dry_run` argument to the `deploy_promote` macro, which allows
  previewing the sequence of commands that will be run as part of the
  environment promotion step of the blue/green deployment workflow.

* Fix the `deploy_init` macro to correctly account for scheduled clusters.
  Before, these clusters would be incorrectly recreated in the deployment
  environment with the `SCHEDULE` option set to `manual` (instead of
  `on-refresh`).

## 1.8.1 - 2024-06-08

* Add support for overriding the `generate_cluster_name` macro to customize the
  cluster name generation logic in user projects.
* Stop adding aliases to subqueries when calling with `--empty` flag.

## 1.8.0 - 2024-05-23

* Update base adapter references as part of
  [decoupling migration from dbt-core](https://github.com/dbt-labs/dbt-adapters/discussions/87)
  * Migrate to dbt-common and dbt-adapters packages.
  * Add tests for `--empty` flag as part of [dbt-labs/dbt-core#8971](https://github.com/dbt-labs/dbt-core/pull/8971)
  * Add functional tests for unit testing.
* Support enforcing model contracts for the [`map`](https://materialize.com/docs/sql/types/map/),
  [`list`](https://materialize.com/docs/sql/types/list/),
  and [`record`](https://materialize.com/docs/sql/types/record/) pseudo-types.

## 1.7.8 - 2024-05-06

* Fix permission management in blue/green automation macros for non-admin users
  ([#26773](https://github.com/MaterializeInc/materialize/pull/26773)).

## 1.7.7 - 2024-04-19

* Tweak [`deploy_permission_validation`](https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize/dbt/include/materialize/macros/deploy/deploy_permission_validation.sql)
  macro to work around [#26738](https://github.com/MaterializeInc/materialize/issues/26738).

## 1.7.6 - 2024-04-18

* **Breaking change.** The `source` and `sink` materialization types no longer
    accept arbitrary SQL statements, and now accept the `cluster` configuration
    option. The new syntax omits the `CREATE { SOURCE | SINK }` clause, and
    requires migrating existing `source` and `sink` models to use that syntax
    before upgrading.

    **New**

    ```sql
    {{ config(
         materialized='source',
         cluster='quickstart'
       ) }}
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'test-topic')
    FORMAT BYTES
    ```

    **Deprecated**

    ```sql
    {{ config(
         materialized='source'
       ) }}
    CREATE SOURCE {{ this }} IN CLUSTER 'quickstart'
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'test-topic')
    FORMAT BYTES
    ```

## 1.7.5 - 2024-03-15

* `deploy_init(ignore_existing_objects)` now automatically copies the default
   privileges and existing grants of existing clusters and schemas to their
   deployment counterparts.

## 1.7.4 - 2024-02-27

* Add macros to automate blue/green deployments, which help minimize downtime
  when deploying changes to the definition of objects in Materialize to
  production environments:

  * `deploy_init(ignore_existing_objects)`: creates the deployment schemas and
    clusters using the same configuration as the corresponding production
    environment to swap with.

  * `deploy_await(poll_interval)`: waits for all objects within the deployment
    clusters to be fully hydrated, polling the cluster readiness status at a
    specified `poll_interval`.

  * `deploy_promote`: deploys the current dbt targets to the production
    environment, encuring all deployment targets, including schemas and
    clusters, are fully hydrated and deployed together as a single atomic
    operation. If any part of the deployment fails, the entire deployment is
    rolled back to maintain consistency and prevent partial updates.

  * `deploy_cleanup`: tears down the deployment schemas and clusters.

  **Sample workflow**

  ```yaml
  # dbt_project.yml
  vars:
  deployment:
    default:
      clusters: ["prod"]
      schemas: ["prod "]
  ```

  ```bash
  dbt run-operation deploy_init
  dbt run --vars 'deploy: True'
  # deploy_await can run automatically by specifying deploy_promote
  # (wait=True), but we recommend running this step manually and running
  # validation checks before promoting.
  dbt run-operation deploy_await
  dbt run-operation deploy_promote
  dbt run-operation deploy_cleanup
  ```

  In this version, the blue/green deployment workflow will fail if sources or
  sinks exist in the schemas or cluster to swap. This might change in a future
  release.

* Revert backport of [dbt-core #8887](https://github.com/dbt-labs/dbt-core/pull/8887),
  which shipped with[dbt v1.7.6](https://github.com/dbt-labs/dbt-core/releases/tag/v1.7.6).
  Non-standard types should just work™️ in model contracts as part of dbt core
  functionality.

## 1.7.3 - 2024-01-24

* Support scheduled refreshes in the `materialized_view` materialization via the
  new `refresh_interval` configuration. This is a private preview feature in
  Materialize, so configuration details are likely to change in the future.

## 1.7.2 - 2023-12-18

* Backport [dbt-core #8887](https://github.com/dbt-labs/dbt-core/pull/8887) to
  unblock users using any custom type with data contracts.

## 1.7.1 - 2023-12-14

* Remove the dependency of data contracts pre-flight checks on the existence of
  the pre-installed `default` cluster. Fixes [#23600](https://github.com/MaterializeInc/materialize/issues/23600).

* Work around [dbt-core #8353](https://github.com/dbt-labs/dbt-core/issues/8353)
  while a permanent fix doesn't land in dbt Core to unblock users using UUID
  types with data contracts.

## 1.7.0 - 2023-11-20

* Support specifying the materialization type used to store test failures via
  the new [`store_failures_as` configuration](https://docs.getdbt.com/reference/resource-configs/store_failures_as).
  Accepted values: `materialized_view` (default), `view`, `ephemeral`.

  * **Project level**
  ```yaml
  tests:
    my_project:
      +store_failures_as: view
  ```

  * **Model level**
  ```yaml
  models:
    - name: my_model
      columns:
        - name: id
          tests:
            - not_null:
                config:
                  store_failures_as: view
            - unique:
                config:
                  store_failures_as: ephemeral
  ```

  If both [`store_failures`](https://docs.getdbt.com/reference/resource-configs/store_failures)
  and `store_failures_as` are specified, `store_failures_as` takes precedence.

* Mark `dbt source freshness` as not supported. Materialize supports the
  functionality required to enable column- and metadata-based source freshness
  checks, but the value of this feature in a real-time data warehouse is
  limited.

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
