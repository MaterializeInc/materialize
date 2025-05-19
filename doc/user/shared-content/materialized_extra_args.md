To use, set the instance's `environmentd_extra_env` to an array of
strings; for example:

```hc {hl_lines="4-8"}
materialize_instances = [
  {
    ...
    environmentd_extra_args = [
      "--system-parameter-default=<param>=<value>",
      "--cors-allowed-origin=<value>",
      "--bootstrap-builtin-catalog-server-cluster-replica-size=50cc", ...
    ]
  }
]
```
