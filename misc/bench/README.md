# Benchmark Utility Scripts

This directory contains utility scripts for running benchmarks.

## matrixbench - A Script for Running Many Benchmarks

There is a script, `misc/bench/matrixbench`, that will repeatedly run the benchmark most suitable
for your machine (either `benchmark` or `benchmark-medium`), starting from 1 worker thread, to 16
worker threads. You can also supply multiple git revisions if you want to compare builds against
each other. To run `matrixbench`, supply the name of a composition that you wish to run:

    ./matrixbench <composition>

This will take quite some time to run, but at the end, it will produce a CSV that looks like the
following:

    GIT_REVISION,MZ_WORKERS,INGEST_RATE,GRAFANA_DASHBOARD
    HEAD,16,522193,http://localhost:3000/d/materialize-overview/materialize-overview?from=1612572459000&to=1612573285000&tz=UTC

### Adding New Compositions

`matrixbench` expects that benchmarks, for a composition, obey the following pattern:

    
