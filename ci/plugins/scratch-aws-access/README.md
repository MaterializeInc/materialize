# Scratch AWS access Buildkite Plugin

A [Buildkite plugin] that assumes a role with nearly unrestricted access to a
scratch AWS account. The following environment variables are set:

* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_SESSION_TOKEN`

The credentials are valid for 12 hours.

## Example

```yml
steps:
  - id: aws-requiring-step
    plugins:
      - ./ci/plugins/scratch-aws-access
```

[Buildkite plugin]: https://buildkite.com/docs/agent/v3/plugins
