# mzcompose Buildkite Plugin

A [Buildkite plugin] that runs mzcompose.

## Example

```yml
steps:
  - id: compose-using-step
    plugins:
      - ./ci/plugins/mzcompose:
          composition: composition-name
          run: workflow-name
          args: [--some, args]
          log-start-at-last: "--- Testing current build"
          run-timeout: 60m
```

`run-timeout` limits only the main mzcompose invocation. The command hook still
runs its cleanup handler afterward, subject to the Buildkite step timeout.

`log-start-at-last` limits error annotation to the portion of each log starting
at the last occurrence of the marker. Logs without the marker are scanned in
full. This is useful for workflows that exercise historical binaries before
testing the current build.

## Cleaning up resources outside of Docker

A composition that creates resources outside of Docker, such as a Cloud
region, can define a workflow named `ci-cleanup`. The command hook runs it
after the main workflow has exited, however it exited, and passes it the main
workflow's full argument list: the step's `args` plus any `CI_EXTRA_ARGS`. The
workflow must therefore parse with `parse_known_args` and find its target from
those arguments alone. Cancelling or timing out a job ends the main workflow
with SIGTERM, which does not run Python `finally` blocks, so a composition
must not rely on its own cleanup path for those cases.

Before the workflow runs, the hook kills (SIGKILL) the containers of the main
run's compose project, so that a command still in flight from the main run,
such as an `mz region enable` that outlived the cancelled process, cannot undo
the cleanup after it has finished; the Docker teardown proper happens
afterwards. The kill is skipped under `CI_COVERAGE_ENABLED`, where the Docker
teardown is deliberately graceful. The workflow must be idempotent: it also
runs after a successful run that already cleaned up. It has 15 minutes, so
that a hung cleanup cannot eat the cancel grace period before the artifacts
are uploaded. It writes no JUnit report, so the main workflow's report
survives. Its failure is recorded in the error annotation and fails an
otherwise green job.

The mzcompose-files lint does not count `ci-cleanup` as a workflow, so a
composition with a single main workflow can add it without looping over
`c.workflows`; a `default` that does loop must skip `ci-cleanup`.

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
