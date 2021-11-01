# Introduction

Failpoints are a way to force instrumented code to take a particular action if a given code line is reached while the failpoint is enabled. This is useful when writing tests -- for example, Materialize can be forced to panic or I/O calls can be made to return an error.

Materializes uses the [fail create](https://docs.rs/crate/fail) to enable this functionality.

# Intrumenting the code

## Panic-only failpoints

If a failpoint needs to only cause a panic, the code needs to be instrumented with the following:

```
fail_point!("read-dir");
```

## Failpoints that can return an error

If a failpoint needs to be able to cause an error to be returned, the following syntax can be used to supply the error:

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

In addition to panicking or returning an error, a failpoint can perform other actions when reached, incuding:
- sleep for a specified number of milliseconds
- print a message
- sleep until unblocked

A probabiltiy can also be specified to determine how frequently a given action will trigger. The complete documentation on the actions is available from the [crate documentation](https://docs.rs/fail/0.4.0/fail/fn.cfg.html)

# Handling panics

If a failpoint is used to trigger a panic, a [mzworkflows.md](mzworkflows.md) based test can be used to restart Materialize and perform additional testing or validation.
