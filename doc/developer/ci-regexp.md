# ci-regexp value

For CI issues (see for example [flaky CI issues](https://github.com/MaterializeInc/materialize/labels/ci-flake), but also [SQLsmith issues](https://github.com/MaterializeInc/materialize/issues?q=is%3Aissue+in%3Atitle+%5Bsqlsmith%5D+) we support a special `ci-regexp` line in the body of the issue to denote a unique error message indicating this issue. This regex is used by Python's [re module](https://docs.python.org/3/library/re.html), see the supported syntax there.

Examples:

```
ci-regexp: updating cluster status cannot fail
ci-regexp: failed to listen on address.*Address already in use (os error 98)
ci-regexp: unexpected peek multiplicity
ci-regexp: Expected identifier, found operator
```

Additionally you can use the `ci-apply-to` to limit the `ci-regexp` to a specific buildkite test case:

Examples:

```
ci-apply-to: SQLsmith
ci-apply-to: sqlsmith explain
```

Use `ci-location` to specify the file or test fragment in which a test failure occurs. When the Buildkite annotation says `Unknown error in test-read-frontier-advancement`, then `test-read-frontier-advancement` is the location. When it says `Unknown error in services.log`, then `services.log` is the location.

Examples:

```
ci-location: test-read-frontier-advancement
ci-location: services.log
```

When a test issue is discovered and the GitHub issue is still open, you may want the failure to not cause a test failure, so that CI does not become flaky, but still add an annotation and record the failure into https://ci-failures.dev.materialize.com. You can use `ci-ignore-failure: true` for that.
