# OpenTelemetry development environment
An [mzcompose] composition for working with OpenTelemetry traces locally.
Primarily adapted from [the opentelemetry-rust repo].

### Usage

```
$ ./mzcompose up
$ MZ_OPENTELEMETRY_ENDPOINT="http://localhost:4317" bin/materialized
```

Go to <http://localhost:16686> in your browser to browse the traces.

[mzcompose]: ../../doc/developer/mzcompose.md
[the opentelemetry-rust repo]: https://github.com/open-telemetry/opentelemetry-rust/tree/a767fd3a7f08f4d7312a1c0dbb5ac0580a108eb3/examples/basic-otlp-http
