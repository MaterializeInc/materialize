{{- /* Skill output: renders as a markdown table instead of raw HTML. */ -}}
For Materialize Cloud, `cc` cluster sizes have the following default resource
allocations:

| Size | CPU Limit | Memory Limit | Disk Limit | Credits/Hour |
| --- | --- | --- | --- | --- |
{{- range $.Site.Data.self_managed.self_managed_cluster_sizes.cluster_sizes }}
| `{{ .size }}` | `{{ .cpu_limit }}` | `{{ .memory_limit }}` | `{{ .disk_limit }}` | `{{ .credits_per_hour }}` |
{{- end }}
