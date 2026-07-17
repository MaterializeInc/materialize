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

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
