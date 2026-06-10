### Prometheus metrics

`mz-debug` outputs snapshots of prometheus metrics per service (i.e. environmentd) if
[`--dump-prometheus-metrics`](#dump-prometheus-metrics) is `true` (the default).
Each file is stored under `prom_metrics/{service}.txt`.
