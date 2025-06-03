# Bazel <img width=24px alt="bazel logo" src="https://blog.bazel.build/images/bazel-icon.svg"/>

[Official Site](https://bazel.build/)

An open source distributed build system maintained by Google, a fork of their internal build system
known as Blaze.

> **tl;dr**: These tips should get you started with Bazel:
> 1. To generate a `BUILD.bazel` file, run `bin/bazel gen`.
> 2. When running Bazel, a target is defined like `//src/catalog:mz_catalog`
     where `//` signifies the root of our repository, `src/catalog` is the path
     to a directory containing a `BUILD.bazel` file, and `:mz_catalog` is a
     named target within that `BUILD.bazel` file.
> 3. To see what targets are available you can use the `query` subcommand, e.g.
     `bin/bazel query //src/catalog/...`.

#### About Bazel

Bazel's main component for building code are "rules", which are provided by open source rule sets, e.g.
[`rules_rust`](https://github.com/bazelbuild/rules_rust). When using a rule, e.g.
[`rust_library`](https://bazelbuild.github.io/rules_rust/defs.html#rust_library), you define all of
the inputs (e.g. source files) and extra parameters (e.g. compiler flags) required to build your
target. Bazel then computes a build graph which is used to order operations and determine when
something needs to get re-built.

A common annoyance with Bazel is that it operates in a sandbox. A build that otherwise succeeds on
your machine might fail when run with Bazel because it has a different version of a compiler, or
can't find some necessary file. This is a key feature though because it makes builds hermetic and
allows Bazel to aggressively cache artifacts, which reduces build times.

**Table of contents:**

  * [Getting Started](#getting-started)
    * [Installing `bazelisk`](#installing-bazelisk)
    * [Defining your own `.bazelrc` file](#defining-your-own-bazelrc-file)
    * [Remote caching](#remote-caching)
  * [Using Bazel](#using-bazel)
    * [Building a crate](#building-a-crate)
    * [Adding a new crate](#adding-a-new-crate)
    * [Running a test](#running-a-test)
      * [Filtering tests](#filtering-tests)
  * [How Bazel Works](#how-bazel-works)
    * [`WORKSPACE`, `BUILD.bazel`, `*.bzl` files](#workspace-buildbazel-bzl-files)
    * [Generating `BUILD.bazel` files](#generating-buildbazel-files)
      * [`cargo-gazelle`](#cargo-gazelle)
      * [Supported Configurations](#supported-configurations)
    * [Platforms](#platforms)
      * [Custom Build Flags](#custom-build-flags)
    * [Toolchains](#toolchains)
      * [System Roots](#system-roots)
  * [Building Rust Code](#building-rust-code)
    * [`crates_repository`](#crates_repository)
    * [Rust `-sys` crates](#rust--sys-crates)
      * [Other C dependencies](#other-c-dependencies)
    * [Protobuf Generation](#protobuf-generation)
    * [Git Hash Versioning](#git-hash-versioning)

# Getting Started

## Installing `bazelisk`

To use `bazel` you first need to install [`bazelisk`](https://github.com/bazelbuild/bazelisk), which
is a launcher that automatically makes sure you have the correct version of Bazel installed.

> Note: We have a `.bazelversion` file in our repository that ensures everyone is using the same version.

On macOS you can do this with Homebrew:

```shell
brew install bazelisk
```

For Linux distributions you'll need to grab a binary from their [releases](https://github.com/bazelbuild/bazelisk/releases)
page and put them into your PATH as `bazel`:
```shell
chmod +x bazelisk-linux-amd64
sudo mv bazelisk-linux-amd64 /usr/local/bin/bazel
```

## Defining your own `.bazelrc` file

Bazel has numerous [command line options](https://bazel.build/reference/command-line-reference),
which can be defined in a `.bazelrc` file to create different configurations that you run Bazel
with. We have a [`.bazelrc`](../../.bazelrc) in the root of our repository that defines several
common build configurations, but it's also recommended that you create a `.bazelrc` in your home
directory (i.e. `~/.bazelrc`) to customize how you run Bazel locally. Options specified in your
home RC file will override those of the workspace RC file.

A good default to start with is:
```
# Bazel will use all but one CPU core, so your machine is still responsive.
common --local_resources=cpu="HOST_CPUS-1"

# Define a shared disk cache so builds from different Materialize repos can share artifacts.
build --disk_cache=~/.cache/bazel

# Optional. The workspace RC already sets a max disk cache size, and artifact age
# but you can override that if you have more limited disk space.
common --experimental_disk_cache_gc_max_size=40G
common --experimental_disk_cache_gc_max_age=7d
```

## Remote caching

Bazel supports reading and writing artifacts to a [remote cache](https://bazel.build/remote/caching).
We currently have two setup in [`i2`](https://github.com/MaterializeInc/i2/blob/daacaca7e04e9c30e9914b5efa084047b3e58994/i2/apps/__init__.py#L126-L141)
that are backed by S3 and running [`bazel-remote`](https://github.com/buchgr/bazel-remote).
One is accessible by developers and used by PR builds in CI, we treat this as semi-poisoned. The
other is only accessible by CI and used for builds from `main` and tagged builds.

To enable remote caching as a developer you must to the following:

1. Have Teleport setup and `tsh` installed
2. Create `~/.config/materialize/build.toml` and add the following:

```
[bazel]
remote_cache = "teleport:bazel-remote-cache"
```

When running Bazel via `bin/bazel` we will read the build config and spawn a Teleport proxy via
`tsh` if one isn't already running, then specify `--remote_cache` to `bazel` with the correct URL.

### Teleport proxy fails to start

In some cases you might see a warning printed when calling `bin/bazel` indicating the Teleport
proxy failed to start, e.g.

```
Teleport proxy failed to start, 'tsh' process already running!
  existing 'tsh' processes: [10001]
  exit code: 1
```

Generally this means there is a Teleport proxy already running that we've lost track of. You can
fix this issue by terminating the existing `tsh` process with the PID specified in the warning
message.

### S3 Bucket Layout

We maintain two remote caches in the "Materialize Core" AWS account stored under S3 buckets:

- `materialize-bazel-remote`: Used for PR builds and accessible by developers
- `materialize-bazel-remote-pa`: Used for main branch and tagged builds (CI only)

Each bucket contains two main folders `cas.v2` and `ac`.

To force Bazel to rebuild each cache from scratch, you can delete these folders. Note that you'll need the appropriate AWS permissions to perform these operations.

# Using Bazel

Bazel has been integrated into [`mzbuild`](../../doc/developer/mzbuild.md), which means you can use
it for other tools as well like `mzimage` and `mzcompose`! To enable Bazel specify the `--bazel`
flag like you would specify the `--dev` flag, e.g. `bin/mzcompose --bazel ...`.

Otherwise Bazel can be used just like `cargo`, to build individual targets and run tests. We
provide a thin wrapper around the `bazel` command in the form of [`bin/bazel`](../../misc/python/materialize/cli/bazel.py).
This sets up remote caching, and provides the `fmt` and `gen` subcommands.
Otherwise it forwards all commands onto `bazel` itself.

## Building a crate

All Rust crates in our Cargo Workspace have a `BUILD.bazel` file that define different build
targets for the crate. You don't have to write these files, they are automatically generated from
the crate's `Cargo.toml`. For more details see the [Generating `BUILD.bazel` files](#generating-buildbazel-files)
section.

> **tl;dr** to build a crate run `bin/bazel build //src/<crate-name>` from the root of the repo.

To determine what targets are available for a crate you can use the `query` subcommand, e.g.

```shell
$ bin/bazel query //src/adapter/...

//src/adapter:adapter
//src/adapter:mz_adapter
//src/adapter:mz_adapter_doc_test
//src/adapter:mz_adapter_lib_tests
//src/adapter:mz_adapter_parameters_tests
//src/adapter:mz_adapter_sql_tests
//src/adapter:mz_adapter_timestamp_selection_tests
```

Every Rust crate has at least one Bazel target, which is the name of the crate. In the example
above the "adapter" crate has the target `mz_adapter`. So you can build the `mz_adapter` crate
by running the following:

```shell
$ bin/bazel build //src/adapter:mz_adapter
```

For convenience we also alias the primary target to have the same name as the folder, in the
example above we alias `mz_adapter` to `adapter`. This allows a shorthand syntax for building a
crate:

```shell
# Builds the same target as the example above!
$ bin/bazel build //src/adapter
```

## Adding a new crate

When adding a new crate to our workspace follow the normal flow that you would
with Cargo, e.g. run `cargo new --lib my_crate`. Once it's created you'll need
to add an entry to the Bazel [`WORKSPACE`](/WORKSPACE) in the root of our
repository. In that file search for "crates_repository" and then find the
manifests section, it should look something like this:

```
crates_repository(
  name = "crates_io",

  //...

  manifests = [
    "//:Cargo.toml",
    "//:src/adapter-types/Cargo.toml",
    <add your new crate to this list>
  ],
)
```

> The `crates_repository` Bazel rule aggregates all of the third-party crates
that we use and automatically generates `BUILD.bazel` files for them.

Once your new crate is added to `crates_repository` run `bin/bazel gen` to
generate a new `BUILD.bazel` file, and you should be all set!

## Running a test

> Note: Support for running Rust tests with Bazel is still experimental. We're waiting on
  [#29266](https://github.com/MaterializeInc/materialize/pull/29266).

Defined in a crate's `BUILD.bazel` are test targets. The following targets are automatically
generated:

* `<crate_name>_lib_tests`
* `<crate_name>_doc_tests`
* `<crate_name>_<integration_test_file_name>_tests`

For example, at the time of writing the `ore` crate has three files underneath `ore/tests`,
`future.rs`, `panic.rs`, and `task.rs`. As such the `BUILD.bazel` file for the `ore` crate has the
following test targets:

* `mz_ore_lib_tests`
* `mz_ore_doc_tests`
* `mz_ore_future_tests`
* `mz_ore_panic_tests`
* `mz_ore_task_tests`

You can run the tests in `future.rs` by running the following command:

```shell
bin/bazel test //src/ore:mz_ore_future_tests
```

### Filtering Tests

You can provide arguments to the underlying test binary with the [`--test_arg`](https://bazel.build/reference/command-line-reference#flag--test_arg)
command line option. This allows you to provide a filter to Rust's test framework, e.g.

```shell
bin/bazel test //src/ore:mz_ore_future_tests --test_arg=catch_panic_async
```

Would run only the tests in `future.rs` matching the filter "catch_panic_async".

# How Bazel Works

## `WORKSPACE`, `BUILD.bazel`, `*.bzl` files

There are three kinds of files in our Bazel setup:

* `WORKSPACE`: Defines the root of our workspace, we only have one of these. This is where we load
  all of our rule sets, download remote repositories, and register toolchains.
* `BUILD.bazel`: Defines how a library/crate is built, where you use "rules". This is generally
  equivalent to a `Cargo.toml`, one per-crate.
* `*.bzl`: Used to define new functions or macros that can be used in `BUILD.bazel` files, written
  in [Starlark](https://bazel.build/rules/language). As a general developer you should rarely if
  ever need to interact with these files.

## Generating `BUILD.bazel` files

> **tl;dr** run `bin/bazel gen` from the root of the repository.

Just like `Cargo.toml`, associated with every crate is a `BUILD.bazel` file that provides targets that
Bazel can build. We auto-generate these files with [`cargo-gazelle`](../../misc/bazel/cargo-gazelle) which
developers can easily run via `bin/bazel gen`.

There are times though when `Cargo.toml` doesn't provide all of the information required to build a
crate, for example the [`std::include_str!`](https://doc.rust-lang.org/std/macro.include_str.html)
macro adds an implicit dependency on the file being included. Bazel operates in a sandbox and thus
will fail unless you tell it about the file! For these cases you can add the dependency via a
`[package.metadata.cargo-gazelle.<target>]` section in the `Cargo.toml`. For example:

```toml
[package.metadata.cargo-gazelle.lib]
compile_data = ["path/to/my/file.txt"]
```

This will add `"path/to/my/file.txt"` to the `compile_data` attribute on the
resulting [`rust_library`](http://bazelbuild.github.io/rules_rust/defs.html#rust_library) Bazel target.

### `cargo-gazelle`

[`gazelle`](https://github.com/bazel-contrib/bazel-gazelle) is a semi-official `BUILD.bazel` file
generator that supports Golang and protobuf. There exists a [`gazelle_rust`](https://github.com/Calsign/gazelle_rust)
plugin, but it's not yet mature enough to fit our needs. Still, it's important for producivity that
developers who don't want to interact with Bazel shouldn't have to, so generating a `BUILD.bazel`
file from a `Cargo.toml` is quite important.

Thus we decided to write our own generator, `cargo-gazelle`! It's not a plugin for the existing
`gazelle` tool but theoretically could be. It's designed to be fully generic with very few (if any)
Materialize specific configurations built in.

### Supported Configurations

`cargo-gazelle` supports the following configuration in a `Cargo.toml` file.

```toml
# Configuration for the crate as a whole.
[package.metadata.cargo-gazelle]
# Will skip generating a BUILD.bazel entirely.
#
# If you specify this setting please include a reason at the top of the
# BUILD.bazel file explaining why we skip generating.
skip_generating = (true | false)
# Concatenate the specified string at the end of the generated BUILD.bazel file.
#
# This is largely an escape hatch and should be avoided if possible.
additive_content = "String"


# Configuration for the library target of the crate.
[package.metadata.cargo-gazelle.lib]
# Skip generating the library target.
skip = (true | false)
# Extra data that will be provided to the Bazel target at compile time.
compile_data = ["String Array"]
# Extra data that will be provided to the Bazel target at compile and run time.
data = ["String Array"]
# Extra flags for rustc.
rustc_flags = ["String Array"]
# Environment variables to set for rustc.
[package.metadata.cargo-gazelle.lib.rustc_env]
var1 = "my_value"

# By default Bazel enables all features of a crate, if provided we will
# _override_ that set with this list.
features_override = ["String Array"]
# Extra dependencies to include for the target.
extra_deps = ["String Array"]
# Extra proc-macro dependencies to include for the target.
extra_proc_macro_deps = ["String Array"]


# Configuration for the crate's build script.
[package.metadata.cargo-gazelle.build]
# Skip generating the library target.
skip = (true | false)
# Extra data that will be provided to the Bazel target at compile time.
compile_data = ["String Array"]
# Extra data that will be provided to the Bazel target at compile and run time.
data = ["String Array"]
# Extra flags for rustc.
rustc_flags = ["String Array"]
# Environment variables to set for rustc.
[package.metadata.cargo-gazelle.build.rustc_env]
var1 = "my_value"

# Environment variables to set for the build script.
build_script_env = ["String Array"]
# Skip the automatic search for protobuf dependencies.
skip_proto_search = (true | false)


# Configuration for test targets in the crate.
#
# * Library tests are named "lib"
# * Doc tests are named "doc"
#
[package.metadata.cargo-gazelle.test.<name>]
# Skip generating the library target.
skip = (true | false)
# Extra data that will be provided to the Bazel target at compile time.
compile_data = ["String Array"]
# Extra data that will be provided to the Bazel target at compile and run time.
data = ["String Array"]
# Extra flags for rustc.
rustc_flags = ["String Array"]
# Environment variables to set for rustc.
[package.metadata.cargo-gazelle.test.<name>.rustc_env]
var1 = "my_value"

# Bazel test size.
#
# See <https://bazel.build/reference/be/common-definitions#common-attributes-tests>.
size = "String"
# Environment variables to set for test execution.
[package.metadata.cargo-gazelle.test.<name>.env]
var1 = "my_value"


# Configuration for binary targets of the crate.
[package.metadata.cargo-gazelle.binary.<name>]
# Skip generating the library target.
skip = (true | false)
# Extra data that will be provided to the Bazel target at compile time.
compile_data = ["String Array"]
# Extra data that will be provided to the Bazel target at compile and run time.
data = ["String Array"]
# Extra flags for rustc.
rustc_flags = ["String Array"]
# Environment variables to set for rustc.
[[package.metadata.cargo-gazelle.binary.<name>.rustc_env]]
var1 = "my_value"

# Environment variables to set for test execution.
[package.metadata.cargo-gazelle.binary.<name>.env]
var1 = "my_value"
```

If all else fails, the code that handles this configuration lives in [`misc/bazel/cargo-gazelle`](../bazel/cargo-gazelle/src/config.rs)!


## Platforms

[Official Documentation](https://bazel.build/extending/platforms)

Bazel is designed to be run on a variety of hardware, operating systems, and system configurations.
To manage this complexity Bazel has a concept of "constraints" to allow conditional configuration
of rules, and "platforms" to manage hardware differences. There are three roles that a platform can
serve:

1. Host: the platform that Bazel is invoked from, generally a developers local machine.
2. Execution: the platform that Bazel is using to execute actions or compile code. For Materialize
   this is always the same as the "Host platform" since we don't utilize distributed builds.
3. Target: the platform we are building for.

The platforms that we build for are defined in [`/platforms/BUILD.bazel`](../../misc/bazel/platforms/BUILD.bazel).

A common way to configure a build based on platform is to use the
[`select`](https://bazel.build/reference/be/functions#select) function. This allows you to return
different values depending on the platform we're targetting.

### Custom Build Flags

Not necessarily related to platforms, but still defined in
[`/platforms/BUILD.bazel`](../../misc/bazel/platforms/BUILD.bazel) are our custom build flags.
Currently we have custom build settings for the following features:

1. Sanitizers like AddressSanitizer (ASan).
2. Cross language LTO

While most build settings can get defined in the `.bazelrc` these features require slightly more
complex configuration. For example, if we're building with a sanitizer we need to disable
`jemalloc`, this is because sanitizers commonly have their own allocator. To do this we create a
new build flag with the [`string_flag`](https://github.com/bazelbuild/bazel-skylib/blob/454b25912a8ddf3d90eb47f25260befd5ee274a8/docs/common_settings_doc.md)
rule from the Bazel Skylib rule set and match on this using the [`config_setting`](https://bazel.build/docs/configurable-attributes)
rule that is built in to Bazel. The [`config_setting`] is then what we can match on in our
`BUILD.bazel` files with a `select({ ... })` function.

## Toolchains

[Official Documentation](https://bazel.build/extending/toolchains)

Bazel has a specific framework to manage compiler toolchains. For example, instead of having to
specify a Rust toolchain every time you use the `rust_library` rule, you instead register a global
Rust toolchain that rules resolve during analysis.

Toolchains are defined and registered in the [`WORKSPACE`](/WORKSPACE) file. We currently use
Clang/LLVM to build C/C++ code (via the [`toolchains_llvm`](https://github.com/bazel-contrib/toolchains_llvm) ruleset)
where the version is defined by the `LLVM_VERSION` constant. For Rust we support both stable and
nightly, where the versions defined by the `RUST_VERSION` and `RUST_NIGHTLY_VERSION` constants
respectively.

Both [`toolchains_llvm`](https://github.com/bazel-contrib/toolchains_llvm/blob/0d3594c3edbe216e4734d52aa1e305049f877ea5/toolchain/osx_cc_wrapper.sh.tpl)
and [`rules_rust`](https://github.com/bazelbuild/rules_rust/tree/e38fa8c2bc0990debceaf28daa4fcb2c57dcdc1c/util/process_wrapper) have "process wrappers".
These are small wrappers around `clang` and `rustc` that are able to inspect the absolute path they
are being invoked from. Bazel does not expose absolute paths _at all_ so these wrappers are how
arguments like [`--remap-path-prefix`](https://doc.rust-lang.org/stable/rustc/command-line-arguments.html#--remap-path-prefix-remap-source-names-in-output)
get set. These wrappers are helpful but can also cause issues like [`toolchains_llvm#421`](https://github.com/bazel-contrib/toolchains_llvm/issues/421).

The upstream [LLVM toolchains](https://github.com/llvm/llvm-project/releases) are very large and
built for bespoke CPU architectures. While maybe not ideal, we build our own LLVM toolchains which
live in the [MaterializeInc/toolchains](https://github.com/MaterializeInc/toolchains) repo. This
ensures we're using the same version of `clang` across all architectures we support and greatly
improves the speed of cold builds.

> Note: The upstream LLVM toolchains are ~1 GiB and compressed with gzip, end-to-end they took
about 3 minutes to download and setup. Our toolchains are ~80MiB and compressed with zstd which
end-to-end take less than 30 seconds to download and setup.

### System Roots

Along with a C-toolchain we also provide a system root for our builds. A system root contains
things like `libc`, `libm`, and `libpthread`, as well as their associated header files. Our system
roots also live in the [MaterializeInc/toolchains](https://github.com/MaterializeInc/toolchains/blob/22e21deac9cec196c6adfcca811722882f92f941/sysroot/Dockerfile) repo.

# Building Rust Code

For building Rust code we use [`rules_rust`](https://github.com/bazelbuild/rules_rust). It's
primary component is the [`crates_repository`](http://bazelbuild.github.io/rules_rust/crate_universe.html#crates_repository)
rule.

## `crates_repository`

Normally when building a Rust library you define external dependencies in a `Cargo.toml`, and
`cargo` handles fetching the relevant crates, generally from `crates.io`. The [`crates_repository`](https://bazelbuild.github.io/rules_rust/crate_universe_workspace.html#crates_repository)
rule does the same thing, we define a set of manifests (`Cargo.toml` files), it will analyze them
and create a Bazel [repository](https://bazel.build/external/overview#repository) containing all of
the necessary external dependencies.

Then to build our crates, e.g. [`mz-adapter`](../../src/adapter/), we use the handy
[`all_crate_deps`](http://bazelbuild.github.io/rules_rust/crate_universe.html#all_crate_deps)
macro. When using this macro in a `BUILD.bazel` file, it determines which package we're in (e.g.
`mz-adapter`) and expands to all of the necessary external dependencies. Unfortunately it does not
include dependencies from within our own workspace, so we still need to do a bit of manual work
of specifying dependencies when writing our `BUILD.bazel` files.

In the [`WORKSPACE`](../../WORKSPACE) file we define a "root" `crates_repository` named `crates_io`.

## Rust `-sys` crates

There are some Rust crates that are wrappers around C libraries, like
[`decnumber-sys`](https://crates.io/crates/decnumber-sys) is a wrapper around
[`libdecnumber`](https://speleotrove.com/decimal/). `cargo-gazelle` will generate a Bazel target
for the crate's build script, but it's likely this build script will fail because it can't find
tools like `cmake`, our system root, or implicitly depends on some other C library.

The general approach we've used to get these crates to build is to duplicate the logic from the
`-sys` crate's `build.rs` script into a Bazel target. See
[bazel/c_deps/rust-sys](../../misc/bazel/c_deps/rust-sys) for some examples. Once you write a
`BUILD.bazel` file for the C dependency we add a `crate.annotation` in our [`WORKSPACE`](https://github.com/MaterializeInc/materialize/blob/5f2f45a162c44e4c6a03ba017f8b7d1d00c3775b/WORKSPACE#L464-L469)
file that appends your newly written `BUILD.bazel` file to the one generated for the Rust crate.

Duplicating logic is never great, but having Bazel explicitly build these C dependencies provides
better caching and more control over the process which unlocks features like cross language LTO.

### Other C dependencies

There are a few C dependencies which are used both by a Rust `-sys` crate and another C dependency.
For example `zstd` is used by both the `zstd-sys` Rust crate and the `rocksdb` C library. For these
cases instead of depending on the version included via the Rust `-sys` crate, we "manually" include
them by downloading the source files as an [`http_archive`](https://bazel.build/rules/lib/repo/http).
All cases of external C dependencies live in [`bazel/c_deps/repositories.bzl`](../../misc/bazel/c_deps/repositories.bzl).

## Protobuf Generation

Nearly all of our Rust build scripts do a single thing, and that's generate Rust bindings to
protobuf definitions. `rules_rust` includes [rules for generating protobuf bindings](https://bazelbuild.github.io/rules_rust/rust_prost.html)
when using Prost and Tonic, but they don't interact with Cargo Build Scripts very well. Instead we
added a new crate called [`build-tools`](../../src/build-tools) whose purpose is to abstract over
whatever build system you're using and provide the tool a build script might need, like `protoc`.

For Bazel we provide the necessary tools via ["runfiles"](https://bazel.build/rules/lib/builtins/runfiles), which are defined in the [`data` field](https://github.com/MaterializeInc/materialize/blob/5f2f45a162c44e4c6a03ba017f8b7d1d00c3775b/src/build-tools/BUILD.bazel#L30-L33)
of the `rust_library` target. Bazel "runfiles" are a set of files that are provided at runtime
execution. So in your build script to get the current path of the `protoc` executable you would
call `mz_build_tools::protoc` ([example](https://github.com/MaterializeInc/materialize/blob/5f2f45a162c44e4c6a03ba017f8b7d1d00c3775b/src/persist/build.rs#L14))
which returns a [different path depending on the build system](https://github.com/MaterializeInc/materialize/blob/5f2f45a162c44e4c6a03ba017f8b7d1d00c3775b/src/build-tools/src/lib.rs#L38-L64)
currently being used.

## Git Hash Versioning

Development builds of Materialize include the current git hash in their version number. The sandbox
that Bazel creates when building a Rust library does not include any git info, so attempts to get
the current hash will fail.

But! Bazel has a concept of "stamping" builds which allows you to provide local system information
as part of the build process, this information is known as the [workspace status](https://bazel.build/docs/user-manual#workspace-status).
Generating the workspace status and providing it to Rust libraries requires a few steps, all of
which are described in the [`bazel/build-info/BUILD.bazel`](https://github.com/MaterializeInc/materialize/blob/5f2f45a162c44e4c6a03ba017f8b7d1d00c3775b/misc/bazel/build-info/BUILD.bazel#L12-L29) file.

Unfortunately this isn't the whole story though. It turns out workspace status and stamping builds
causes poor remote cache performance. On a new build Bazel will regenerate the `volatile-status.txt`
file used in workspace stamping which causes any stamped libraries to not be fetched from the
remote cache, see [`bazelbuild#10075`](https://github.com/bazelbuild/bazel/issues/10075). For us
this caused a pretty serious regression in build times so we came up with a workaround:

1. When [building in release mode but not a tagged build](https://github.com/MaterializeInc/materialize/blob/e427108049dd97f55b8b6d634ae01b025b8520b5/misc/python/materialize/mzbuild.py#L156-L163),
   (e.g. a PR) `mzbuild.py` will [write out the current git hash to a temporary file](https://github.com/MaterializeInc/materialize/blob/e427108049dd97f55b8b6d634ae01b025b8520b5/misc/python/materialize/bazel.py#L39-L64).
2. Our `build-info` Rust crate knows to [read from this temporary file](https://github.com/MaterializeInc/materialize/blob/main/src/build-info/src/lib.rs#L123-L141)
   in a non-hermetic/side-channel way to get the git hash into the current build without
   invalidating the remote cache.

While definitely hacky, our side-channel for the git hash does provide a substantial improvement in
build times, while providing similar guarantees to the Cargo build with respect to when the hash
gets re-computed.
