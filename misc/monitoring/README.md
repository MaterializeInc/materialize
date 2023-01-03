# Local monitoring composition

An [mzcompose] composition for running Prometheus and Grafana locally. Metrics
from all processes run by the `bin/environmentd` script will be automatically
discovered and scraped.

## Usage

```
cd misc/monitoring
./mzcompose run default
```

To access Prometheus, run `./mzcompose web prometheus` or navigate directly to
<http://localhost:9090> in your browser.

To access Grafana, run `./mzcompose web grafana` or navigate directly to
<http://localhost:3000> in your browser.

## Modifications

You can adjust the Prometheus configuration by editing the
[prometheus.yml](./prometheus.yml) file in this directory. It is bind mounted
into the container, so edits will be picked up the next time you restart:

```
./mzcompose down -v
./mzcompose run default
```

You can adjust the Grafana configuration through its UI. Beware that the
Grafana state is ephemeral, and will be lost the next time you run
`./mzcompose down`.

[mzcompose]: ../../doc/developer/mzcompose.md
