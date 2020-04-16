# mzcompose Buildkite Plugin

A [Buildkite plugin] that runs mzcompose.

## Example

```yml
steps:
  - id: compose-using-step
    plugins:
      - ./ci/plugins/mzcompose:
          config: test/mzcompose.yml
          run: container-name
```

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
