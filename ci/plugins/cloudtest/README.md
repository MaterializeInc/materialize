# mzcompose Buildkite Plugin

A [Buildkite plugin] that runs cloudtest.

## Example

```yml
steps:
  - id: cloudtest-using-step
    plugins:
      - ./ci/plugins/cloudtest:
          args: [--some, pytest, args]
```

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
