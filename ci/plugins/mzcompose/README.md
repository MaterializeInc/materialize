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
after the main workflow has exited, however it exited, before tearing down
the Docker containers, and passes it the same `args`. Cancelling or timing out
a job ends the main workflow with SIGTERM, which does not run Python `finally`
blocks, so a composition must not rely on its own cleanup path for those
cases. The workflow must be idempotent: it also runs after a successful run
that already cleaned up. Its failure is recorded in the error annotation but
does not change the job's result.

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
