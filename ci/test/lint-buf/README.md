## Protobuf lints

We use [buf] to detect [breaking changes](https://buf.build/docs/breaking/overview) in protobuf files. The configuration is generated based on the template
located in `.src/buf.yaml.template` and ignore comments in the proto files.

The check is run in CI as part of the linting step.

### Ignore a proto file

To ignore a proto file, add the following comment to the file:
```
// buf breaking: ignore
```

It is possible to add a reason to the comment. For example:
```
// buf breaking: ignore (Ignore because of database-issues#99999.)
```

### Update the configuration
The current configuration is checked in into the repository. The linting step verifies that the configuration is
up-to-date by regenerating it and will fail otherwise. To update the configuration, run `bin/update-buf-config`.

[buf]: https://buf.build/docs/introduction
