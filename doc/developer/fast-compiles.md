# Fast compiles

This document attempts to capture the various lessons learned by Materialize
engineers when optimizing machines for compiling Rust.

## Run Linux

The most important step you can take to speed up Rust compilation times is to
run Linux rather than macOS.

Your money goes much farther if you don't pay the Apple tax. For a given
price point, you can usually build a Linux machine with twice as much compute,
memory, and storage than a Mac.

A Linux machine will also allow you to run performance tests on your laptop.
Our customer's production deployments are running on Linux, not macOS. While
it's possible to get a rough sense of Materialize's performance on macOS,
there have been more than a few occasions where apparent performance defects
have disappeared when running the workload on Linux.

### Use lld

Using lld instead of the standard linker will result in an impressive
linking speedup.

#### Installation

##### Linux

On Debian-based distros, you can install lld from the standard package
repository:

```shell
sudo apt install lld
```

You'll need to hunt down the equivalent instructions for your distribution if
you don't use a Debian-based distribution.

##### macOS

lld is available on Homebrew as part of the `llvm` package.

```shell
brew install llvm
```

The `llvm` package is keg-only, so you'll have to add its bin folder to your
PATH manually. Refer to the "Caveats" section in the output of the above
`brew install` command for instructions.

#### Configuration

To tell Rust to use lld, set the following environment variable:

```shell
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"
```

Alternatively, you can configure the linker through a
[Cargo config file][cargo-config]:

```toml
[build]
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

[cargo-config]: https://doc.rust-lang.org/cargo/reference/config.html#configuration

### Disable debug info

The fastest known way to compile is with lld and disabling debug info:

```shell
export RUSTFLAGS="-C link-arg=-fuse-ld=lld -C debuginfo=0"
```

Ideally, set that in your `~/.bashrc` or equivalent so that it applies
permanently.

## Example custom builds

The easiest way to speed up compilation is to throw some money at faster
hardware. A few Materialize employees have embarked on this quest so far:

  * Nikhil built a 3970x-based desktop in spring 2020.
    See: https://pcpartpicker.com/list/LWnRyk

  * Sean built a nearly identical desktop in summer 2020.

  * Matt is building a 5950x-based desktop as soon as 5950x CPUs
    are not out of stock (early 2021).
    See: https://pcpartpicker.com/user/mjibson/saved/Wp4fvK

  * Eli is building a similar desktop to Matt (early 2021).
    See: https://pcpartpicker.com/user/elindsey_/saved/BnTk99

The jury is still out on whether more, slower cores (e.g., 3970x) is better than
less, faster cores (e.g., 5950x) for the standard `cargo build` development
cycle.

To date, no one has attempted to build a Linux laptop optimized for compilation
performance.

## Use a hosted machine

Hetzner provides reasonably-priced many-core AMD machines hosted Germany or Finland. While some may be using workstation rather than true server-grade hardware, they provide significantly faster compilation times than a standard laptop.

## Community resources

Many folks in the Rust community have tips too. Some of those are collected
here:

  * The [Compile Times][nethercote] chapter of Nicholas Nethercote's *The Rust
    Performance Book*.

  * Brian Anderson's [The Rust Compilation Model Calamity][brson] blog post,
    published on PingCAP's blog. The blog post is the start of an ongoing series
    about compile times in Rust.

  * Matthias Endler's [Tips for Faster Rust Compile Times][endler].

[brson]: https://pingcap.com/blog/rust-compilation-model-calamity
[endler]: https://endler.dev/2020/rust-compile-times/
[nethercote]: https://nnethercote.github.io/perf-book/compile-times.html
