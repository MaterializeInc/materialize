# Developer guide: `tokio-console`

This guide details how to run `tokio-console` with `materialized`

## Overview

First, install `tokio-console`:

```
cargo install tokio-console
```

Then run `materialized` with the `console-subscriber` on:

```
RUSTFLAGS="--cfg tokio_unstable" cargo run --features tokio-console -- --dev --tokio-console
```

(note that this may slow down `materialized` a lot, as it increases the amount of tracing by a lot,
and may inadvertently turn on debug logging for `rdkafka`)

The, in a different tmux pane/terminal, run:

```
tokio-console
```

This [README] has some docs on how to navigate the ui.

[README]: https://github.com/tokio-rs/console/tree/main/tokio-console

### Notes on compilation times:

`RUSTFLAGS="--cfg tokio_unstable"` forces a recompilation of our entire dependency tree.
Note that this also clashes with the compilation cache that `rust-analyzer` uses, and may
cause regular recompilations. To avoid this, you can add:

```
[build]
rustflags = ["--cfg", "tokio_unstable"]
```

to our [cargo config] temporarily, or your cargo config in `$HOME`, which can make RA and cargo use the same flags.
Please be careful to not add it under the `x86_64-unknown-linux-gnu` target section if you
aren't running on that target (i.e. you are using a macbook).

[cargo config]: https://github.com/MaterializeInc/materialize/blob/main/.cargo/config
