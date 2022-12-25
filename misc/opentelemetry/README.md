# OpenTelemetry development environment

An [mzcompose] composition for working with OpenTelemetry traces locally.
Primarily adapted from the [basic-otlp-http example][otel-example] in the
opentelemetry-rust repository.

### Usage

```
$ ./mzcompose up -d
$ bin/environmentd <--reset> -- --opentelemetry-endpoint="http://localhost:4317"
```

Use `./mzcompose web jaeger` to open your web browser to the Jaeger
interface, where you can browse the traces. Alternatively, navigate
directly to <http://localhost:16686> in your browser.

[mzcompose]: ../../doc/developer/mzcompose.md
[otel-example]: https://github.com/open-telemetry/opentelemetry-rust/tree/a767fd3a7f08f4d7312a1c0dbb5ac0580a108eb3/examples/basic-otlp-http
