# Introduction

Failpoints force instrumented code to take a particular action if a given code line is reached while the failpoint is enabled. When writing tests, you can use failpoints to force Materialize to panic or to make I/O calls return an error.

Materialize uses the [fail crate](https://docs.rs/crate/fail) to enable this functionality.

# Instrumenting the code

## Panic-only failpoints

If a failpoint needs to only cause a panic, the code needs to be instrumented with the following:

```
fail_point!("read-dir");
```

## Failpoints that can return an error

If you need the failpoint to return an error, use the following:

```
fail_point!("fileblob_set_sync", |_| {
   Err(Error::from("failpoint reached"))
});
```

# Enabling failpoints

By default, failpoints are disabled and will not fire. It is possible to enable failpoints to be triggered either statically on startup or dynamically as the Materialize binary is running.

### Via an environment variable

The `fail` crate will read the `FAILPOINTS` environment variable on startup to obtain instructions on failpoints to be enabled:

```
FAILPOINTS=fileblob_get=return target/release/materialized
```

### Dynamically via SQL

It is possible to enable a failpoint dynamically via SQL:

```
> SET failpoints = 'fileblob_set_sync=return';
```

A failpoint can also be disabled dynamically:

```
> SET failpoints = 'fileblob_set_sync=off';
```

# Failpoint actions

In addition to panicking or returning an error, a failpoint can perform other actions when reached, including:
- sleep for a specified number of milliseconds
- print a message
- sleep until unblocked

You can also specify a probability that determines how frequently a given action will trigger. For more information, see the [documentation of the crate](https://docs.rs/fail/0.4.0/fail/fn.cfg.html)

# Handling panics

If a failpoint triggers a panic, you can use a test based on [mzworkflows.md](mzworkflows.md) to restart Materialize and perform additional testing or validation.
