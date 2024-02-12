# Local monitoring composition

An [mzcompose] composition for running a minimal monitoring stack of Prometheus
(metrics), Tempo (distributed tracing), and Grafana locally. Metrics from all
processes run by the `bin/environmentd` script will be automatically discovered
and collected, and traces will be collected when run with `--monitoring`:

```
./bin/environmentd --monitoring
```

## Usage

```
cd misc/monitoring
./mzcompose run default
```

To access Grafana, run `./mzcompose web grafana` or navigate directly to
<http://localhost:3000> in your browser. Grafana will have datasources
for both Prometheus and Tempo by default.

If needed, to access Prometheus directly, run `./mzcompose web prometheus`
or navigate directly to <http://localhost:9090> in your browser.

Tempo does not have its own UI and can be only accessed via Grafana.
See [the tracing docs] for more info on how to filter and access tracing
data.

## Modifications

You can adjust the Prometheus and Tempo configurations by editing the
[prometheus.yml](./prometheus.yml) and [tempo.yml](./tempo.yml) files in
this directory. They are bind mounted into the containers, so edits will
be picked up the next time you restart:

```
./mzcompose down -v
./mzcompose run default
```

You can adjust the Grafana configuration through its UI. Beware that the
Grafana state is ephemeral, and will be lost the next time you run
`./mzcompose down`.

[the tracing docs]: ../../doc/developer/tracing.md#accessing-tracing-data-locally
[mzcompose]: ../../doc/developer/mzcompose.md
