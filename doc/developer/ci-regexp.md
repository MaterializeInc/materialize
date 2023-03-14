# ci-regexp value

For CI issues (see for example [flaky CI issues](https://github.com/MaterializeInc/materialize/labels/ci-flake), but also [SQLsmith issues](https://github.com/MaterializeInc/materialize/issues?q=is%3Aissue+in%3Atitle+%5Bsqlsmith%5D+) we support a special `ci-regexp` line in the body of the issue to denote a unique error message indicating this issue. This regex is used by Python's [re module](https://docs.python.org/3/library/re.html), see the supported syntax there.

Examples:

```
ci-regexp: updating cluster status cannot fail
ci-regexp: failed to listen on address.*Address already in use (os error 98)
ci-regexp: unexpected peek multiplicity
ci-regexp: Expected identifier, found operator
```
