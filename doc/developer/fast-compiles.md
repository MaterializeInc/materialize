# Fast compiles

This document attempts to capture the various lessons learned by Materialize
engineers when optimizing machines for compiling Rust.

## Run Linux

The most important step you can take to speed up Rust compilation times is to
run Linux rather than macOS.

First, your money goes much farther if you don't pay the Apple tax. For a given
price point, you can usually build a Linux machine with twice as much compute,
memory, and storage than a Mac.

Second, the linkers on Linux are faster. Apple has not invested much in speeding
up `ld` on macOS, and there are no alternatives. On Linux, you have your choice
of three production-grade linkers: the standard `ld` (bfd), gold, and lld. lld
in particular is blazing fast.

Finally—and this is somewhat of an aside—a Linux machine will allow you to run
performance tests on your laptop. Our customer's production deployments are
running on Linux, not macOS. While it's possible to get a rough sense of
Materialize's performance on macOS, there have been more than a few occasions
where apparent perfomance defects have disappeared when running the workload on
Linux.

### Use lld

On Linux, using lld instead of the standard linker will result in an impressive
linking speedup. You can install it from the standard package repository on
Debian-based distributions:

```shell
sudo apt install lld
```

You'll need to hunt down the equivalent instructions for your distribution if
you don't use a Debian-based distribution.

Then, tell Rust to use lld by setting the following environment variable:

```shell
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"
```

### Disable debug info

The fastest known way to compile on Linux is with lld and disabling debug info:

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

## Experimental Mac Linker

The LLVM clang project has an old Mach-O (macOS) linker that seems to be
generally discouraged, but about a year ago they started a [new one with an
architecture similar to the linux one touted above][macho]. It's unclear how far
along it is, but it's at least far along enough that the [Chromium project has
added directions for trying it out][chromium]. As of 2021-04-06, there's a big
experimental warning at the top of the page and a number of active known bugs,
but they seem to indicate that Chromium builds with it.

We'd absolutely use the normal macOS linker (`ld64`) for anything real, but
initial results for the edit-compile-run cycle seem promising. On Ruchir's
laptop after an initial compile:

- With the stock linker `touch src/materialized/src/bin/materialized/main.rs;
  cargo run` (basically just relinking) takes ~17s
- With the experimental one, the same takes ~9s
- With the stock linker `touch src/repr/src/lib.rs; cargo run`
  (pseudo-representitive edit-compile-run example) takes ~32s
- With the experimental one, the same takes  ~24s

I've been using it for my first couple days and haven't seen any issues so far.

There's enough recent bug fixes that you'd want to use a more recent version of
clang than is available in brew. Instructions for using a pre-built one hosted
by Chromium.

```shell
curl -s https://raw.githubusercontent.com/chromium/chromium/master/tools/clang/scripts/update.py | python - --output-dir=/tmp/clang
curl -s https://raw.githubusercontent.com/chromium/chromium/master/tools/clang/scripts/update.py | python - --output-dir=/tmp/clang --package=lld_mac
```

Then add the dir you passed to `--output-dir` followed by `/bin` (so
`/tmp/clang/bin`) to your `PATH` and `export RUSTFLAGS="-C
link-arg=-fuse-ld=lld"` as above. If anything doesn't work the first time, try a
`cargo clean`. If you stick with this, you'll want to move clang out of `/tmp`
so the system doesn't garbage collect it for you (and updated your `PATH`
accordingly).

*Note:* This seems to depend on having a new enough version of code (11.5+ is
known to work), which in turn requires Big Sur (maybe Catalina also works?). If
you do an OS upgrade, you'll definitely need a `cargo clean`.

*Note:* For some reason, the native arm64 version of clang you get by running
the above commands passes different arguments to lld. You can still use the
native arm64 version of experimental new lld with the following (`--host-os` is
intentionally omitted from the second command):

```shell
curl -s https://raw.githubusercontent.com/chromium/chromium/master/tools/clang/scripts/update.py | python - --output-dir=/tmp/clang --host-os=mac
curl -s https://raw.githubusercontent.com/chromium/chromium/master/tools/clang/scripts/update.py | python - --output-dir=/tmp/clang --package=lld_mac
```

[macho]: https://github.com/llvm/llvm-project/tree/main/lld/MachO
[chromium]: https://github.com/chromium/chromium/blob/master/docs/mac_lld.md
