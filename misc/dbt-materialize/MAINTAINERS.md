# Maintainer instructions

## Versioning

The `dbt-materialize` adapter should always match **major** and **minor** releases of `dbt-core` (i.e. if `dbt-core` v1.2.x is released, we should also bump `dbt-materialize` to v1.2.x). For patch releases, version numbers might differ and we should follow our own release cadence (i.e. if `dbt-core` v1.1.1 is released and `dbt-materialize` is on v1.1.5, that's legit).

The following line in [`setup.py`](./setup.py#L42) guarantees that the adapter always installs the latest patch version of `dbt-postgres` and `dbt-core`:

```
 install_requires=["dbt-postgres~=1.1.0"],
```

See the [dbt documentation](https://docs.getdbt.com/docs/core-versions#how-we-version-adapter-plugins) for more details on versioning.

## Running tests

Run the `dbt-materialize` test suite via [mzcompose](../../doc/developer/mzbuild.md#mzcompose)
to bring up all the dependencies and match how it is run in CI:

```shell
./mzcompose --dev run default
```

### Useful links

* [Guide: testing a new adapter](https://docs.getdbt.com/docs/contributing/testing-a-new-adapter)

* [Test utilities](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/tests/util.py)
