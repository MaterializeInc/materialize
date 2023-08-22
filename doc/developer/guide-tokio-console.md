# Developer guide: `tokio-console`

This guide details how to run `tokio-console` with Materialize.

## Overview

First, install `tokio-console`. We require support for Unix domain sockets, [which is still pending
upstream][uds-pr], so we need to install from a fork for now.

```text
cargo install tokio-console --git https://github.com/MaterializeInc/tokio-console.git
```

Then run `environmentd`:

```text
./bin/environmentd --tokio-console
```

(Note that this may slow down `environmentd` a lot, as it increases the amount of tracing by a lot,
and may inadvertently turn on debug logging for `rdkafka`.)

In the output of the above command, take note of the `tokio-console` listen addresses for the
different processes, e.g.:

```text
environmentd: [...]  INFO mz_ore::tracing: starting tokio console on http://127.0.0.1:6669
compute-cluster-u1-replica-u1: [...]  INFO mz_ore::tracing: starting tokio console on /var/folders/30/[...]3ee9
compute-cluster-s1-replica-s1: [...]  INFO mz_ore::tracing: starting tokio console on /var/folders/30/[...]f5b6
compute-cluster-s2-replica-s2: [...]  INFO mz_ore::tracing: starting tokio console on /var/folders/30/[...]7af2
```

Then, in a different tmux pane/terminal, run the `tokio-console` command with the listen address of
your process of interest:

```text
tokio-console <listen-address>
```

This [README] has some docs on how to navigate the ui.

[uds-pr]: https://github.com/tokio-rs/console/pull/388
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
