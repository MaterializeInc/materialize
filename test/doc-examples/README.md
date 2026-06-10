Documentation examples
======================

Runs the documentation examples from doc/user/data/examples as tests to make sure our documentation stays correct with respect to the actual syntax and expected results.

Every `.yml` file is turned into one testdrive file and then run on the fly.

We use these special properties in doc examples `.yml` files:

- `test_setup`: Testdrive code to run before `code`. See [Testdrive documentation](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/testdrive.md)
- `code`: SQL code to be tested
- `results`: A single result for the last query
- `test_results`: Can be a list of results for each query in `code`, or a single result for the last query. Makes `results` not be used
- `test_replacements`: A map of text replacements to run on the `code`/`test_code` and `results`/`test_results` fields
- `testable`: Whether to run this fragments' code at all
- `testable_results`: Whether the `results` should be used as the expected output of `code`

By default all `.yml` files are run recursively:
```bash
bin/mzcompose --find doc-examples run default
```
You can restrict the search to find matches:
```bash
bin/mzcompose --find doc-examples run default rbac-sm/alter_default_privileges.yml
```
