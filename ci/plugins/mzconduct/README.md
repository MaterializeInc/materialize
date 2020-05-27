# mzconduct Buildkite Plugin

A [Buildkite plugin] that runs mzconduct.

## Example

```yml
steps:
  - id: conduct-using-step
    plugins:
      - ./ci/plugins/mzconduct:
          test: the_name
          workflow: the_workflow
```

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
