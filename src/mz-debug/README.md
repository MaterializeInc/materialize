# `mz-debug`

This tool allows us to debug a user's self-managed environment.

In self-managed mode it downloads snapshots from the debug collector the
Materialize operator runs for each instance (`mz-debug collector`, deployed
from a `MaterializeDebug` resource). Enable the collector with
`debugCollector.enabled: true` in the operator's Helm values.

## Run locally:
To run locally, an example of a command is:

```shell
$ bin/mz-debug self-managed \
    --k8s-namespace materialize-environment \
    --mz-instance-name 12345678-1234-1234-1234-123456789012
```
