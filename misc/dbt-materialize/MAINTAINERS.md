# Maintainer instructions

## Versioning

The `dbt-materialize` adapter should always match **major** and **minor** releases of `dbt-core` (i.e. if `dbt-core` v1.2.x is released, we should also bump `dbt-materialize` to v1.2.x). For patch releases, version numbers might differ and we should follow our own release cadence (i.e. if `dbt-core` v1.1.1 is released and `dbt-materialize` is on v1.1.5, that's legit).

The following line in [`setup.py`](./setup.py#L42) guarantees that the adapter always installs the latest patch version of `dbt-postgres` and `dbt-core`:

```
 install_requires=["dbt-postgres~=1.1.0"],
```

See the [dbt documentation](https://docs.getdbt.com/docs/core-versions#how-we-version-adapter-plugins) for more details on versioning.

## Running tests locally

1. Enter the `misc/dbt-materialize` directory:

   ```shell
   cd misc/dbt-materialize
   ```

2. Create a virtual environment:

   ```shell
   python3 -m venv venv
   ```

3. Activate the virtual environment:

   ```shell
   . venv/bin/activate
   ```

4. Install `dbt-materialize` into the virtual environment:

   ```shell
   pip install . && pytest && dbt-tests-adapter
   ```

5. Launch `materialized` in a different terminal:

   ```
   brew install materialize/materialize/materialized
   materialized
   ```

6. Run tests:

   ```
   pytest
   ```

7. Remember to re-install (`pip install .`) if you change the `dbt-materialize`
   Python code.

### Tips and tricks

All-in-one command to run after making a change to `dbt-materialize`:

```shell
pip install . && pytest && dbt-tests-adapter
```

Run only the tests matching a filter:

```shell
pytest -k TEST-FILTER
```

List all available tests:

```shell
pytest --collect-only
```

Run tests without hiding dbt's output:

```shell
pytest -s
```

Don't drop the schema in `materialized` after a test failure, so that you can
inspect the objects that the test created:

```shell
pytest --no-drop-schema
```

Run the `dbt-materialize` test suite via [mzcompose](../../doc/developer/mzbuild.md#mzcompose)
to match how it is run in CI:

```shell
./mzcompose --dev run default
```

### Useful links

* [Guide: testing a new adapter](https://docs.getdbt.com/docs/contributing/testing-a-new-adapter)

* [Test utilities](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/tests/util.py)
