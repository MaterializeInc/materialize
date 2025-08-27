# `mz-debug`

This tool allows us to debug a user's self-managed environment.

## Run locally:
To run locally, an example of a command is:

```shell
$ bin/mz-debug self-managed \
    --k8s-namespace materialize \
    --mz-instance-name 12345678-1234-1234-1234-123456789012 \
    --additional-k8s-namespace materialize-environment
```
