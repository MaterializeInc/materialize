# Introduction

[`tracing`] is a set of crates used for _logging_ and _tracing_ of rust
binaries. It is primarily composed of the [`tracing`] crate, which is the
core way to interact with the framework, and the [`tracing-subscriber`] crate,
which provides an API to compose systems that consume tracing data.

The [tracing docs](docs.rs/tracing/latest/tracing/#overview) say it best:
> In asynchronous systems like Tokio, interpreting traditional log messages can often be quite challenging. Since individual tasks are multiplexed on the same thread, associated events and log lines are intermixed making it difficult to trace the logic flow. tracing expands upon logging-style diagnostics by allowing libraries and applications to record structured events with additional information about temporality and causality — unlike a log message, a span in tracing has a beginning and end time, may be entered and exited by the flow of execution, and may exist within a nested tree of similar spans. In addition, tracing spans are structured, with the ability to record typed data as well as textual messages.

## Logging

The most basic way to use `tracing` is to use it like other logging crates like `log`,
which additional semi-structured capabilities.


Materialize binaries are setup such that [events] are printed on stderr.

For example:
```
tracing::info!("processed {} items", items.len()")
```
produces:
```
2022-02-15T18:40:14.289898Z INFO my::module: processed 100 items
```
(See [this page for how the colors will look])

You also also add semi-structured data to the event, for example:
```
tracing::debug!("processed {} items", items.len(), ?enum_value")
```
produces:
```
2022-02-15T18:40:14.289898Z DEBUG my::module: processed 100 items _enum_value_=EnumVariant
```
because `?enum_value` means record `enum` as its `Debug` impl. This is referred to as
"field recording", see [recording fields] for more options and examples.
This just touches the surface of what you can do with the event macros,
see [`tracing`] and [`tracing-subscriber`] for more info!

### Levels and Filtering

You may have noticed that log events have [`level`]s. In materialize binaries, we only print
events that are level `INFO` or more urgent. You can control this level by using the
`MZ_LOG_FILTER` environment variable to control the level at a _per-crate_, _per-module_ granularity.

For example:
```
bin/environmentd -- --log-filter=my_interesting_module::sub_module=trace,error
```
logs ALL events in the specific module, but only `error` events for everything else. See [`Targets`] for more examples.


Note that this value is passed through to all compute and storage instances when running locally.
In the future, it is possible to add per-service filtering, if deemed useful.

## Spans

**Please see [the tracing docs](https://docs.rs/tracing/latest/tracing/index.html#spans) for more information about spans**

[`Span`]s are what makes `tracing` a "tracing" crate, and not just a logging crate. Spans are like events,
but they have a start and and end, and are organized into a tree of parents and children. There are
many tools that can analyze and visualize spans to help you determine where Materialize is spending
its time.

There are macros like the event macros for creating them:
```
let span tracing::info_span!("my_span", ?field);
```

Critically, these spans do very nothing till they are _entered_, which turns on the span.
When a span is _entered_, spans that _are created on the same thread_ inherit the entered span
_as a parent_. For example, in:
```
let s1 = tracing::debug_span!("my_span").entered();
let s2 = tracing::debug_span!("my_span2");
```
the `s2` span as `s1` as a parent.

NOTE: `entered()` and `enter()` _do not_ work with async code! See the next 2 sections
for a better way to do this!

### `[tracing::instrument(...)]`
Previous examples may feel heavyweight to operate, but `tracing` provides the [`tracing::instrument`] attribute macro
to make it easier. This macro is applied to functions (or the implementations of trait methods) (it works with `async`
functions as well, and `#[async_trait]`), and automatically creates and enters a span each time a function is called.
This makes it easy to make a span tree based on your call graph.

An example:
```
#[tracing::instrument]
fn my_sub_function(field_that_will_be_recorded: String) { ... }


#[tracing::instrument(skip(big_argument))]
fn my_function(big_argument: MyState) {
  ...
  my_sub_function(big_argument.thing);
  ...
  my_sub_function(big_argument.two);
}
```

Both the spans for the `my_sub_function` calls will be childen of the `my_function` span!

See [`tracing::instrument`] for more info.

### Spans and `async` code
**Please see [here] for more advice on how to use spans with async code**

While `#[tracing::instrument]` works fine with `async` code. `Span::enter` can be problematic, explained in the link above.
If `#[tracing::instrument]` is not an option, then import [`tracing::Instrument`] and call async functions like this:
```
let s = info_span!("important_span");
let ret = thing.call_async_method().instrument(s).await;
```

### Logging of spans

By default in materialize binaries, spans are NOT logged to stderr. However, their fields ARE inherited
```
let e = tracing::debug_span!("my_span", ?enum_value").entered();
tracing::debug!("processed {} items", items.len());
```
produces:
```
2022-02-15T18:40:14.289898Z DEBUG my::module: processed 100 items _enum_value_=EnumVariant
```

## OpenTelemetry

While events are clearly useful for logging, so far in this document, its unclear what spans offer. While
there are [many crates] that do interesting things with tracing information, the primary way we use spans in
materialize binaries is to collect [OpenTelemetry] data. In Materialize binaries, tracing spans can be
_automatically_ collected and exported to analysis tools with _no additional work required at callsites_

### Setup

- For local setup, use the instructions here:
<https://github.com/MaterializeInc/materialize/tree/main/misc/opentelemetry> to setup a local OpenTelemetry collector
and ui to view traces.
- TODO(guswynn): cloud honeycomb setup when its available
- TODO(guswynn): link to demo video

### Span visualization

OpenTelemetry UI's like the [Jaeger] one in the local setup _are extraordinarly powerful tools for debugging_. They allow you to visualize
and inspect the exact control flow graph of your service, including the latency of each operation, and contextual recorded fields. See
[Best Pratices](#best-practices) for more information.

### Distributed tracing
[OpenTelemetry] allows us to associate `tracing` spans _from distributed services_ with each other, allowing us to not only visualize
control-flow within a single service, _but across services that communicate with each other_.

See (TODO(guswynn): fill this in) for a demo on how this works.

# Best Practices

## Setup tracing for all communication between services.
TODO(guswynn): clean this up when https://github.com/MaterializeInc/materialize/issues/13019 is done.

This is important to ensure that complex interactions between services can be debugged.

## Choose the appropriate abstraction for the queue you are piloting.
Materialize has lots of queues, both in-process (typically `tokio::sync::mpsc` channels), and cross-process (grpc).
If you want to trace work being enqueued and then processed when dequeued:


### In-process
Prefer moving a [`tracing::Span`] through the queue, like so:

```
#[tracing::instrument(skip(self))]
fn create_sources(&mut self, cs: CreateSources) {
    self.commands_tx.send(
        InstrumentedCreateSources {
            cs,
            span: tracing::Span::current(),
        }
    )
}

async fn process_work(&mut self) {
    loop {
        match self.commands_rx.recv().await {
            InstrumentedCreateSources { cs, span } => {
                // Note that this will typically be done in the most general way possible for
                // Command/Response patterns
                let create_sources_span = tracing::debug_span!(parent: &span, "process_create_sources");
                self.process_create_sources(cs).instrument(create_sources_span).await;
            }
            _ => ...
        }
    }
}
```
This will improve how the trace visualization will look (the sender span will be the parent of the child and wont
close till the child is done), and will reduce load on collectors.

### Across services
Use `mz_ore::tracing::OpenTelemetryContext::obtain` to pass a context from the sending service to the receiving service,
and `mz_ore::tracing::OpenTelemetryContext::attach_as_parent()` on the receiving side. See [the docs] for more information.


### Use the appropriate `Level`:
- `ERROR`:
  - events that needs to be triaged, or something that is fatal to the binary starting correctly. Note that
  error events usually show up red on trace visualizations.
- `WARN`:
  - events that could represent future failures, or non-standard situations. Note that
  error events usually show up yellow on trace visualizations.
- `INFO`:
  - events with information used commonly enough that it should regularly show up in the stderr log.
- `DEBUG`:
  - **most spans that track the flow of commands and requests across services**.
  - events with information used to debug sub-components.
- `TRACE`:
  - exceedingly verbose information intended only for local debugging/tracing




[`tracing`]: https://docs.rs/tracing/latest/tracing
[`tracing-subscriber`]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/
[events]: https://docs.rs/tracing/latest/tracing/#events
[this page for how the colors will look]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/format/struct.Full.html
[recording fields]: https://docs.rs/tracing/latest/tracing/#recording-fields
[`level`]: https://docs.rs/tracing/latest/tracing/struct.Level.html
[`Targets`]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/targets/struct.Targets.html
[`Span`]: https://docs.rs/tracing/latest/tracing/struct.Span.html
[`tracing::instrument`]: https://docs.rs/tracing/latest/tracing/attr.instrument.html
[`tracing::Instrument`]: https://docs.rs/tracing/latest/tracing/trait.Instrument.html
[many crates]: https://docs.rs/tracing/latest/tracing/#related-crates
[OpenTelemetry]: https://opentelemetry.io/
[here]: https://docs.rs/tracing/latest/tracing/struct.Span.html#in-asynchronous-code
[Jaeger]: https://www.jaegertracing.io/
[`tracing::Span`]: https://docs.rs/tracing/latest/tracing/struct.Span.html
[the docs]: https://dev.materialize.com/api/rust/mz_ore/tracing/struct.OpenTelemetryContext.html
