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
          run-timeout: 60m
```

`run-timeout` limits only the main mzcompose invocation. The command hook still
runs its cleanup handler afterward, subject to the Buildkite step timeout.

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
