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
```

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
