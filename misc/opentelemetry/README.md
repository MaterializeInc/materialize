# OpenTelemetry development environment

[OpenTelemetry] is a framework for collecting telemetry data (metrics, logs, traces,
etc) from a program for analysis and alerting. Within our Rust code we use the
[`tracing`] crate to report metrics, and then [Jaeger] to collect and visualize them.

> OpenTelemetry support is still in active development and bugs are to be expected. If
> you run into any issues please report them!

To enable tracing when running `environmentd` you need to provide two arguments, an
OpenTelemetry compatible endpoint to report the traces, and a filter to specify what
"level" of traces you want reported. You can specify these via environment variables, or
command line arguments. Run `bin/environmentd -- --help` to see all the options.

For local development the following options should suffice:
```
MZ_OPENTELEMETRY_ENDPOINT=http://localhost:4317
MZ_OPENTELEMETRY_FILTER=debug
```

Then using [mzcompose] you can start a local instance of [Jaeger] to work with
OpenTelemetry traces.

> This composition was primarily adapted from the [basic-otlp-http example]
> in the `opentelemetry-rust` repository.

### Usage


#### starting Jaeger
```
$ ./mzcompose up -d
```

#### start `environmentd`
> Note: you can omit the `--opentelemetry-*` args if you set env variables
```
$ bin/environmentd --reset -- \
$    --opentelemetry-endpoint="http://localhost:4317" \
$    --opentelemtry-filter="debug"
```

Use `./mzcompose web jaeger` to open your web browser to the Jaeger
interface, where you can browse the traces. Alternatively, navigate
directly to <http://localhost:16686> in your browser.

### `psql`
After connecting to Materialize, but before running any queries, you should set the following variable:

```
set emit_trace_id_notice=true;
```

After each query a `trace_id` will be emitted, which you can search for in
[Jaeger] and see _exactly_ what work `environmentd` did.


[OpenTelemetry]: https://opentelemetry.io/
[`tracing`]: https://docs.rs/tracing/latest/tracing/
[Jaeger]: https://www.jaegertracing.io/
[mzcompose]: ../../doc/developer/mzcompose.md
[basic-otlp-http example]: https://github.com/open-telemetry/opentelemetry-rust/tree/a767fd3a7f08f4d7312a1c0dbb5ac0580a108eb3/examples/basic-otlp-http
