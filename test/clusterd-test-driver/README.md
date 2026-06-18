# `clusterd` test driver scenarios

Script-based scenario tests for the [headless `clusterd` test
driver](../../src/clusterd-test-driver). The driver speaks the cluster protocol
to a real `clusterd` directly, with no `environmentd`, so these tests control
the exact persist state, the exact commands the replica receives, and the exact
timestamps.

Tests are text scripts in [`scripts`](scripts). Each command is followed by a
`----` block holding its expected output; that block *is* the assertion. A
failing command renders as `error: <message>`, so an expected failure is just
another golden block.

Run the scenarios against a real `clusterd` under Docker:

```shell
bin/mzcompose --find clusterd-test-driver run default
```

or run the same scripts on the host, without Docker images:

```shell
bin/pyactivate test/clusterd-test-driver/run-local.py
```

Regenerate the expected `----` blocks in place by setting `REWRITE=1`. Inspect
the diff carefully before committing.

See the [design doc](../../doc/developer/design/20260612_headless_clusterd_test_driver.md)
for the full command vocabulary and architecture.
