# Headless `clusterd` test driver

A headless frontend that speaks the cluster protocol to a real `clusterd`
directly, with no `environmentd`. It hosts the persist infrastructure, drives
the command/response protocol over the wire, accesses persist directly, and
asserts on responses. This lets a test control the exact persist state, the
exact commands the replica receives, and the exact timestamps, while still
exercising the real worker process and protocol.

See the [design doc](../../doc/developer/design/20260612_headless_clusterd_test_driver.md)
for the full architecture and command vocabulary.

## Tests

Tests are text scripts in
[`test/clusterd-test-driver/scripts`](../../test/clusterd-test-driver/scripts).
Each command is followed by a `----` block holding its expected output; that
block *is* the assertion. A failing command renders as `error: <message>`, so
an expected failure is just another golden block.

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

## Layout

The crate separates a generic mechanism from the scripting layer on top:

* `driver` — the `Driver`: connects over CTP, sends any `ComputeCommand`,
  submits dataflows, and watches frontiers.
* `persist_host` — hosts the persist PubSub server and direct shard access.
* `dataflow` — builds the dataflow descriptions the scripts submit.
* `script`, `text` — parse and run the script DSL and render golden output.
* `data`, `responses`, `ctp`, `target` — data generation, response
  demultiplexing, the cluster transport, and target configuration.

Unit and integration tests (e.g. `tests/index_smoke.rs`) cover the direct-write
round trip, the lowered dataflow structure, and the script parser.
