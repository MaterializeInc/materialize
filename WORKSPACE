# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

"""
Materialize's Bazel Workspace.

This `WORKSPACE` file defines all of the rule sets and remote repositories that
we depend on. It's important to note that ordering matters. If an object is
declared more than once it's the _first_ invocation that gets used. This can be
especially tricky when rule sets have their own dependencies, generally
included by calling a `*_dependencies()` macro.
"""

# `bazel-lib`/`bazel-skylib`
#
# Provides generic rules for Bazel to help make things fit together.
#
# For example, Rust build scripts might require files live in a certain
# location, but the dependent `c_library` can't specify an output location.
# `bazel-lib` provides rules to copy files into a new directory that we can
# then provide to the Rust rule.
#
# Note: In an ideal world the two rule sets would be combined into one.

BAZEL_SKYLIB_VERSION = "1.6.1"

BAZEL_SKYLIB_INTEGRITY = "sha256-nziIakBUjG6WwQa3UvJCEw7hGqoGila6flb0UR8z5PI="

maybe(
    http_archive,
    name = "bazel_skylib",
    integrity = BAZEL_SKYLIB_INTEGRITY,
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/{0}/bazel-skylib-{0}.tar.gz".format(BAZEL_SKYLIB_VERSION),
        "https://github.com/bazelbuild/bazel-skylib/releases/download/{0}/bazel-skylib-{0}.tar.gz".format(BAZEL_SKYLIB_VERSION),
    ],
)

ASPECT_BAZEL_LIB_VERSION = "2.7.0"

ASPECT_BAZEL_LIB_INTEGRITY = "sha256-NX2tnSEjJ8NdkkQZDvAQqtMV5z/6G+0aKeIMNy+co0Y="

maybe(
    http_archive,
    name = "aspect_bazel_lib",
    integrity = ASPECT_BAZEL_LIB_INTEGRITY,
    strip_prefix = "bazel-lib-{0}".format(ASPECT_BAZEL_LIB_VERSION),
    url = "https://github.com/aspect-build/bazel-lib/releases/download/v{0}/bazel-lib-v{0}.tar.gz".format(ASPECT_BAZEL_LIB_VERSION),
)

load("@aspect_bazel_lib//lib:repositories.bzl", "aspect_bazel_lib_dependencies", "aspect_bazel_lib_register_toolchains")

# Required bazel-lib dependencies
aspect_bazel_lib_dependencies()

# Register bazel-lib toolchains
aspect_bazel_lib_register_toolchains()

# `rules_cc`
#
# Rules for building C/C++ projects. These are slowly being upstreamed into the
# Bazel source tree, but some projects (e.g. protobuf) still depend on this
# rule set.

RULES_CC_VERSION = "0.0.9"

RULES_CC_INTEGRITY = "sha256-IDeHW5pEVtzkp50RKorohbvEqtlo5lh9ym5k86CQDN8="

maybe(
    http_archive,
    name = "rules_cc",
    integrity = RULES_CC_INTEGRITY,
    strip_prefix = "rules_cc-{0}".format(RULES_CC_VERSION),
    urls = [
        "https://github.com/bazelbuild/rules_cc/releases/download/{0}/rules_cc-{0}.tar.gz".format(RULES_CC_VERSION),
    ],
)

# `rules_pkg`
#
# Rules for building archives, e.g. `tar` or `zip`, for packages.

RULES_PKG_VERSION = "0.7.0"

RULES_PKG_INTEGRITY = "sha256-iimOgydi7aGDBZfWT+fbWBeKqEzVkm121bdE1lWJQcI="

maybe(
    http_archive,
    name = "rules_pkg",
    integrity = RULES_PKG_INTEGRITY,
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/{0}/rules_pkg-{0}.tar.gz".format(RULES_PKG_VERSION),
        "https://github.com/bazelbuild/rules_pkg/releases/download/{0}/rules_pkg-{0}.tar.gz".format(RULES_PKG_VERSION),
    ],
)

# `rules_foreign_cc`
#
# Rules for building C/C++ projects that use foreign build systems, e.g. Make. One case that we use
# this for is building `openssl`.
#
# TODO(parkmycar): We maintain a fork with the following fixes applied to the `mz-fixes` branch:
#
# 1. Versions of `make` >4.3 are buggy and have a segfault, we hit this segfault when building jemalloc.
#    See: <https://github.com/bazelbuild/rules_foreign_cc/issues/898>
#    See: <https://github.com/MaterializeInc/rules_foreign_cc/commit/8e28ba0bbbf7e73d70333b625bfec4a65114d8be>
#
# 2. Some libraries, e.g. jemalloc, preprocess and compile code in two separate steps, so we need
#    to make sure the sysroot is provided in CPPFLAGS, if it's set in CFLAGS.
#    See: <https://github.com/bazelbuild/rules_foreign_cc/pull/1023>
#    See: <https://github.com/MaterializeInc/rules_foreign_cc/commit/2199b1c304140fa959c3703b0b7e9cbf7d39c4c2>
#
# 3. Specify the AR tool to use when bootstrapping `make`. On macOS explicitly set the path to
#    be llvm-ar which we know exists in our toolchain.
#    See: <https://github.com/MaterializeInc/rules_foreign_cc/commit/e94986f05edf95fff025b6aeb995e09be8889b89>
#
# 4. `make` 4.2 fails to compile on Linux because of unrecognized symbols so we patch the source.
#    See: <https://github.com/MaterializeInc/rules_foreign_cc/commit/de4a79280f54d8796e86b7ab0b631939b7b44d05>
#
# 5. Mirror the GNU source code for `make` from the `MaterializeInc/toolchains` repository. We've
#    previously seen the upstream GNU FTP server go down which causes CI to break.
#    See: <https://github.com/MaterializeInc/rules_foreign_cc/commit/c994a0d6a86480d274dc1937d8861a56e6011cf0>

RULES_FOREIGN_CC_VERSION = "c994a0d6a86480d274dc1937d8861a56e6011cf0"

RULES_FOREIGN_CC_INTEGRITY = "sha256-kFSk41S84sVupSj7p+OxlHV5wXKoo67PvBy2vlXiQsg="

maybe(
    http_archive,
    name = "rules_foreign_cc",
    integrity = RULES_FOREIGN_CC_INTEGRITY,
    strip_prefix = "rules_foreign_cc-{0}".format(RULES_FOREIGN_CC_VERSION),
    url = "https://github.com/MaterializeInc/rules_foreign_cc/archive/{0}.tar.gz".format(RULES_FOREIGN_CC_VERSION),
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies(make_version = "4.2")

# `clang`/`llvm`
#
# Normally Bazel will use the system's version of clang as the default C
# toolchain. This prevents the builds from being hermetic though so we include
# our own.
#
# All of the clang related tools are provided under the `@llvm_toolchain_llvm`
# repo. To see what's available run `bazel query @llvm_toolchain_llvm//...`.

# System roots that we use, this is where clang will search for things like libc.

_SYSROOT_DARWIN_BUILD_FILE = """
filegroup(
    name = "sysroot",
    srcs = glob(
        include = ["**"],
        exclude = ["**/*:*"],
    ),
    visibility = ["//visibility:public"],
)
"""

DARWIN_SYSROOT_VERSION = "14.5"

DARWIN_SYSROOT_INTEGRITY = "sha256-JCleYWOca1Z/B4Li1iwDKGEP/IkWWgKqXoygtJXyNTU="

http_archive(
    name = "sysroot_darwin_universal",
    build_file_content = _SYSROOT_DARWIN_BUILD_FILE,
    integrity = DARWIN_SYSROOT_INTEGRITY,
    strip_prefix = "MacOSX{0}.sdk-min".format(DARWIN_SYSROOT_VERSION),
    urls = ["https://github.com/MaterializeInc/toolchains/releases/download/macos-sysroot-sdk-{0}-1/MacOSX{0}.sdk-min.tar.zst".format(DARWIN_SYSROOT_VERSION)],
)

_LINUX_SYSROOT_BUILD_FILE = """
filegroup(
    name = "sysroot",
    srcs = glob(["*/**"]),
    visibility = ["//visibility:public"],
)
"""

# Format is <KERNEL_VERSION-GLIBC_VERSION-LIBSTDCXX_VERSION>
LINUX_SYSROOT_VERSION = "5_10-2_35-11_4_0"

LINUX_SYSROOT_X86_64_INTEGRITY = "sha256-H2q1ti0l6vETl6QBOIvwuizpAJrvKCMObTuw/0Gedy0="

http_archive(
    name = "linux_sysroot-x86_64",
    build_file_content = _LINUX_SYSROOT_BUILD_FILE,
    integrity = LINUX_SYSROOT_X86_64_INTEGRITY,
    strip_prefix = "sysroot",
    urls = ["https://github.com/MaterializeInc/toolchains/releases/download/linux-sysroot-{0}/linux-sysroot-x86_64.tar.zst".format(LINUX_SYSROOT_VERSION)],
)

LINUX_SYSROOT_AARCH64_INTEGRITY = "sha256-kUasOepnmvdoJlI2JHm9T5o3alaS17xS4avwKDyaxMQ="

http_archive(
    name = "linux_sysroot-aarch64",
    build_file_content = _LINUX_SYSROOT_BUILD_FILE,
    integrity = LINUX_SYSROOT_AARCH64_INTEGRITY,
    strip_prefix = "sysroot",
    urls = ["https://github.com/MaterializeInc/toolchains/releases/download/linux-sysroot-{0}/linux-sysroot-aarch64.tar.zst".format(LINUX_SYSROOT_VERSION)],
)

# Version of clang/llvm we use.
#
# We build our own clang toolchain, see the <https://github.com/MaterializeInc/toolchains> repository.
LLVM_VERSION = "19.1.6"

# We have a few variants of our clang toolchain, either improving how it's built or adding new tools.
LLVM_VERSION_SUFFIX = "1"

# Version of the "toolchains_llvm" rule set, _not_ the version of clang/llvm.
#
# We depend on a commit that includes <https://github.com/bazel-contrib/toolchains_llvm/pull/438>.
TOOLCHAINS_LLVM_VERSION = "9f0a7cb0f752ffd430a5c80d749a2e84cb348876"

TOOLCHAINS_LLVM_INTEGRITY = "sha256-9SY8+RwP3KPfaLtjQGzJmknOcxEpTkmu/h1ntaljYdw="

maybe(
    http_archive,
    name = "toolchains_llvm",
    integrity = TOOLCHAINS_LLVM_INTEGRITY,
    strip_prefix = "toolchains_llvm-{0}".format(TOOLCHAINS_LLVM_VERSION),
    url = "https://github.com/bazel-contrib/toolchains_llvm/archive/{0}.tar.gz".format(TOOLCHAINS_LLVM_VERSION),
)

load("@toolchains_llvm//toolchain:deps.bzl", "bazel_toolchain_dependencies")

bazel_toolchain_dependencies()

load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")

llvm_toolchain(
    name = "llvm_toolchain",
    llvm_version = LLVM_VERSION,
    sha256 = {
        "darwin-aarch64": "94ed965925dbdc25b29e6fcfa9a84b28d915d5c9da7c71405fc20bbcf8396bd1",
        "darwin-x86_64": "9395b07fd5018816bcaee84522d9c9386fdbefe62fdf8afff89b57e1b7095463",
        "linux-aarch64": "24fd3405f65ccbc39f0d14a5126ee2edb5904d7a9525ae483f34a510a1bdce3e",
        "linux-x86_64": "bad3d776c222c99056eba8b64c085a1e08edd783cb102e1b6eba43b78ce2fe2b",
    },
    sysroot = {
        "darwin-aarch64": "@sysroot_darwin_universal//:sysroot",
        "darwin-x86_64": "@sysroot_darwin_universal//:sysroot",
        "linux-x86_64": "@linux_sysroot-x86_64//:sysroot",
        "linux-aarch64": "@linux_sysroot-aarch64//:sysroot",
    },
    urls = {
        "darwin-aarch64": ["https://github.com/MaterializeInc/toolchains/releases/download/clang-{0}-{1}/darwin_aarch64.tar.zst".format(LLVM_VERSION, LLVM_VERSION_SUFFIX)],
        "darwin-x86_64": ["https://github.com/MaterializeInc/toolchains/releases/download/clang-{0}-{1}/darwin_x86_64.tar.zst".format(LLVM_VERSION, LLVM_VERSION_SUFFIX)],
        "linux-aarch64": ["https://github.com/MaterializeInc/toolchains/releases/download/clang-{0}-{1}/linux_aarch64.tar.zst".format(LLVM_VERSION, LLVM_VERSION_SUFFIX)],
        "linux-x86_64": ["https://github.com/MaterializeInc/toolchains/releases/download/clang-{0}-{1}/linux_x86_64.tar.zst".format(LLVM_VERSION, LLVM_VERSION_SUFFIX)],
    },
)

load("@llvm_toolchain//:toolchains.bzl", "llvm_register_toolchains")

llvm_register_toolchains()

# `rules_perl`
#
# Provides a `perl` toolchain which is required to build some libraries (e.g. openssl).

RULES_PERL_VERISON = "0.1.0"

RULES_PERL_INTEGRITY = "sha256-XO+tvypJvzQh7eAJ8sWiyYNquueSYg7S/5kYQTN1UyU="

maybe(
    http_archive,
    name = "rules_perl",
    integrity = RULES_PERL_INTEGRITY,
    strip_prefix = "rules_perl-{0}".format(RULES_PERL_VERISON),
    urls = [
        "https://github.com/bazelbuild/rules_perl/archive/refs/tags/{0}.tar.gz".format(RULES_PERL_VERISON),
    ],
)

load("@rules_perl//perl:deps.bzl", "perl_register_toolchains", "perl_rules_dependencies")

perl_rules_dependencies()

perl_register_toolchains()

# Extra setup.
#
# Some libraries (e.g. protobuf) require bespoke rule sets. Because they're
# specific to a single library we move their definitions into separate files
# to avoid cluter.
load("//misc/bazel/c_deps:extra_setup.bzl", "protoc_setup")

protoc_setup()

# C Repositories
#
# Loads all of the C dependencies that we rely on.
load("//misc/bazel/c_deps:repositories.bzl", "c_repositories")

c_repositories()

# `rules_rust`
#
# Rules for building Rust crates, and several convienence macros for building all transitive
# dependencies.

RULES_RUST_VERSION = "0.54.2"

RULES_RUST_INTEGRITY = "sha256-6hXbBNIbd6EvuBc/B5RMmAEhxLy5cs8EGuHpjqwjcz4="

maybe(
    http_archive,
    name = "rules_rust",
    integrity = RULES_RUST_INTEGRITY,
    strip_prefix = "rules_rust-v{0}".format(RULES_RUST_VERSION),
    urls = [
        "https://github.com/MaterializeInc/rules_rust/releases/download/v{0}/rules_rust-v{0}.tar.zst".format(RULES_RUST_VERSION),
    ],
)

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies")

rules_rust_dependencies()

# `rustc`
#
# Fetch and register the relevant Rust toolchains. We use a custom macro that
# depends on `rules_rust` but cuts down on bloat from their defaults.

RUST_VERSION = "1.85.0"

RUST_NIGHTLY_VERSION = "nightly/2025-02-19"

load("//misc/bazel/toolchains:rust.bzl", "bindgen_toolchains", "rust_toolchains")

rust_toolchains(
    [
        RUST_VERSION,
        RUST_NIGHTLY_VERSION,
    ],
    {
        "aarch64-apple-darwin": {
            "stable": {
                "cargo": "94c5901a75dff8570c5132c13d91ae916cc177fab56a50a170f24d7e9993e6e5",
                "clippy": "d5262efc5f704990686fd91755a3d9eb15fbcab2ba4bd441fa0ea6892fcff0cb",
                "llvm-tools": "71ab42f01b7147883b9f4c3676127f6676bb4e96cabaeb0aed4ba49687567fca",
                "rust-std": "b546d4f9006ceabf4f0950dfb7bde83cbe2b85f068cad80890dce20175741821",
                "rustc": "97b961f2d942f7ff3c5e6d52e5284c0dec8387d1d26190a01b60f7af56f8b827",
            },
            "nightly": {
                "cargo": "17f7f038fdc7cf22429eff4e86c3300c8573af46910f4a9cce463bb8da3c83e1",
                "clippy": "bf04ea058c503c6560d3ac030c7485f1e591d330e7d62429cc0231505d18d1b8",
                "llvm-tools": "80b01f1997eb952cc0da9e811ee527c82055dbbfe73f787475db49475a13a914",
                "rust-std": "7f44c5fe8a87ccc21a4a2dfb5fc70ba5458ae60c32effe8ea6751f496edec751",
                "rustc": "826d16d5001f8e8619d37e29572fe063fc492ae63bbb98ae00a37de83b7cf47b",
            },
        },
        "aarch64-unknown-linux-gnu": {
            "stable": {
                "cargo": "00409bd57913f3bf2ed61b28695fb1410083a0a9b882f3c7d5fd4968dfcfa521",
                "clippy": "818001c963ffb95ae477f6f778fe86ebc988daabb402fa5dddf428e488a2a15b",
                "llvm-tools": "23ef4e07e762db2c8c772d92a2f8c1f4b82d2197625410fea5e36db8380a39b6",
                "rust-std": "0f917aff8d5c8d5caf29c3652d3a17c1bff7aaf39434b14067310a7cb70302f7",
                "rustc": "c6c27908b4cae5cc2986869e17d49b7dc91629dc3d20cf057986663d85b71439",
            },
            "nightly": {
                "cargo": "4e1244357ddd822f5e71499d547ca68950ce10138615f13ceb289e592dcf2f1e",
                "clippy": "d88dd593b7504cab70ee840472c08d77ef279f5e4e081e0bc094f336f7f0a93f",
                "llvm-tools": "3ca18fc5cf61d3d8bf29199bd93b90745f4ec2bcabb9078ac4da176e768111e5",
                "rust-std": "065d45b55eb604f22ed68bda2f865eb008991c044c6a63537cafaee4f2d24565",
                "rustc": "68fcd77a50f621479f6d0d79c0983d6b3ce92d7783d65924a1e5c5c5fe81506e",
            },
        },
        "x86_64-apple-darwin": {
            "stable": {
                "cargo": "b23b247753ab403758cba28ffa6004eaacc66ee5ec526a5383cccaad0d9156bb",
                "clippy": "9989e8fb0bf9ed6bb4dfe3c370dadc1c49a80c3de67fac7e6b373faac8ea71f4",
                "llvm-tools": "f90172dfd06119f464be7abfc3f6418509a41decd8bc10b02f22d9dd10be29c5",
                "rust-std": "c23207604a9c87e0fae47b363e7839eb7dcacbd3b3a777ce9cc583d26d557488",
                "rustc": "453d3ac759c1adc0aaa6d6ff9b14f6f509adc8666abae7b0df5258b40ef203ac",
            },
            "nightly": {
                "cargo": "b5de6d1b5e33cf1dc6ef0ef2a8011bd0e87c382b0671ccef9da3845817265e63",
                "clippy": "36a21af139e8917afd210f7c3bc4bb282bd6ae59cb0764de5e54206fcf7a4fd7",
                "llvm-tools": "5121de1a45479c15937dba9a252fe47ca95ae0cb3f65cace1ee3fe32db17ca14",
                "rust-std": "a012d35efa5752823175ca7a6331cbcddf259fc624e5ee51dc02077837fcbbf7",
                "rustc": "0c19bc064499f722798921d72531fee84e0cf891fa073c50c5d1d8b1449a1abb",
            },
        },
        "x86_64-unknown-linux-gnu": {
            "stable": {
                "cargo": "a430a7399dc3d44153b9229ca38beb4a68f531a39bd7bb5232c0e21d1ed128e7",
                "clippy": "90d877253fc811291c8609962ff241aeb3068dccd72087a0f7483b059b8156cb",
                "llvm-tools": "7db586b2284951e838d384176f2df23761af4b969e6132a11689a895e8cd8dfd",
                "rust-std": "d2cc3c6ed8d88f83c4296624bc5d07402127cb239f5b53760ef415fb1b96dec7",
                "rustc": "2c3d50461bddb62d3a18a0bf9aa89192784bf61fc355589c9288b689d79bca17",
            },
            "nightly": {
                "cargo": "1d2390a573ee85f63413ae8362ffef287a9d0b340428310501efa97a7ad12184",
                "clippy": "fe322a6f1aa003a0b20916b3e080284b1e0e30644327780b8f7caa6243dff75f",
                "llvm-tools": "396ca57edaf223066b3fe05f0e15e15d8fc05ca07c3b12f12abf54b75ea75756",
                "rust-std": "d85990d1b5f5009c784e7ca082118fa405f0d1f28d8af0a9df0da226244017cd",
                "rustc": "52e79fdb467259c4aecbbb442509abe0289af966cd319856faff455a77060b44",
            },
        },
    },
)

# Rust `bindgen`
#
# Rules and Toolchains for running [`bindgen`](https://github.com/rust-lang/rust-bindgen)
# a tool for generating Rust FFI bindings to C.

load("@rules_rust//bindgen:repositories.bzl", "rust_bindgen_dependencies")

rust_bindgen_dependencies()

bindgen_toolchains(
    "{0}-{1}".format(LLVM_VERSION, LLVM_VERSION_SUFFIX),
    {
        "darwin_aarch64": "sha256-wni7a1Wu6qGeNVOZOjc6ks1ACXf+RBoXu6YcSVkleos=",
        "darwin_x86_64": "sha256-MKjPkNE2g2nw75SkOvjnieKnTtubUKyE3/o7olQm8j0=",
        "linux_aarch64": "sha256-BvzsXMuiObNStcP86QwgBRDcTVBRsWUYio1iRCMhgxo=",
        "linux_x86_64": "sha256-9PgulfHhsOd03ZhEO7ljp2EuDafIbME1oCJ/Rj/R7pU=",
    },
)

# Load all dependencies for crate_universe.
load("@rules_rust//crate_universe:repositories.bzl", "crate_universe_dependencies")

crate_universe_dependencies()

load("@rules_rust//crate_universe:defs.bzl", "crate", "crates_repository")

crates_repository(
    name = "crates_io",
    annotations = {
        # `crates_repository` fails to automatically add the depenency on `tracing` when
        # `tokio_unstable` is enabled, so we manually specify it.
        "tokio": [
            crate.annotation(
                rustc_flags = ["--cfg=tokio_unstable"],
                deps = ["@crates_io//:tracing"],
            ),
        ],
        "decnumber-sys": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.decnumber.bazel",
            gen_build_script = False,
            # Note: This is a target we add from the additive build file above.
            deps = [":decnumber"],
        )],
        "librocksdb-sys": [crate.annotation(
            # Note: The below targets are from the additive build file.
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.rocksdb.bazel",
            compile_data = [":out_dir"],
            gen_build_script = False,
            rustc_env = {
                "OUT_DIR": "$(execpath :out_dir)",
            },
            deps = [
                ":bindings",
                ":rocksdb",
            ],
        )],
        "tikv-jemalloc-sys": [crate.annotation(
            gen_build_script = False,
            deps = ["@jemalloc"],
        )],
        "rdkafka-sys": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.librdkafka.bazel",
            gen_build_script = False,
            # Note: This is a target we add from the additive build file above.
            deps = [":librdkafka"],
        )],
        "libz-sys": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.libz.bazel",
            gen_build_script = False,
            # Note: This is a target we add from the additive build file above.
            deps = [":zlib"],
        )],
        # TODO(parkmycar): Refactor this to build the version of zlib from the `bzip2-sys` crate.
        "bzip2-sys": [crate.annotation(
            gen_build_script = False,
            deps = ["@bzip2"],
        )],
        "lzma-sys": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.lzma-sys.bazel",
            gen_build_script = False,
            # Note: This is a target we add from the additive build file above.
            deps = [":xz"],
        )],
        "openssl-sys": [crate.annotation(
            build_script_data = [
                "@openssl//:openssl_lib",
                "@openssl//:openssl_include",
            ],
            build_script_data_glob = ["build/**/*.c"],
            build_script_env = {
                "OPENSSL_STATIC": "true",
                "OPENSSL_NO_VENDOR": "1",
                "OPENSSL_LIB_DIR": "$(execpath @openssl//:openssl_lib)",
                "OPENSSL_INCLUDE_DIR": "$(execpath @openssl//:openssl_include)",
            },
            compile_data = ["@openssl//:openssl_lib"],
        )],
        "protobuf-src": [crate.annotation(
            # Note: We shouldn't ever depend on protobuf-src, but if we do, don't try to bootstrap
            # `protoc`.
            gen_build_script = False,
            rustc_env = {"INSTALL_DIR": "fake"},
        )],
        "protobuf-native": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.protobuf-native.bazel",
            gen_build_script = False,
            deps = [":protobuf-native-bridge"],
        )],
        "psm": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.psm.bazel",
            gen_build_script = False,
            # Note: All of the targets we build for support switching stacks, if we ever want to
            # support Windows we'll have to revist this.
            rustc_flags = [
                "--check-cfg=cfg(switchable_stack,asm,link_asm)",
                "--cfg=asm",
                "--cfg=link_asm",
                "--cfg=switchable_stack",
            ],
            # Note: This is a target we add from the additive build file above.
            deps = ["psm_s"],
        )],
        "launchdarkly-server-sdk": [crate.annotation(
            build_script_env = {
                "CARGO_PKG_AUTHORS": "LaunchDarkly",
                "CARGO_PKG_DESCRIPTION": "",
                "CARGO_PKG_HOMEPAGE": "https://docs.launchdarkly.com/sdk/server-side/rust",
                "CARGO_PKG_LICENSE": "Apache-2.0",
                "CARGO_PKG_REPOSITORY": "https://github.com/launchdarkly/rust-server-sdk",
                "RUSTDOC": "",
            },
        )],
        # Compile the backtrace crate and its dependencies with all optimizations, even in dev
        # builds, since otherwise backtraces can take 20s+ to symbolize. With optimizations
        # enabled, symbolizing a backtrace takes less than 1s.
        "addr2line": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "adler": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "backtrace": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "gimli": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "miniz_oxide": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "object": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "rustc-demangle": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "timely": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "differential-dataflow": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
    },
    cargo_config = "//:.cargo/config.toml",
    cargo_lockfile = "//:Cargo.lock",
    generator_sha256s = {
        "aarch64-apple-darwin": "8204746334a17823bd6a54ce2c3821b0bdca96576700d568e2ca2bd8224dc0ea",
        "x86_64-apple-darwin": "2ee14b230d32c05415852b7a388b76e700c87c506459e5b31ced19d6c131b6d0",
        "aarch64-unknown-linux-gnu": "3792feb084bd43b9a7a9cd75be86ee9910b46db59360d6b29c9cca2f8889a0aa",
        "x86_64-unknown-linux-gnu": "2b9d07f34694f63f0cc704989ad6ec148ff8d126579832f4f4d88edea75875b2",
    },
    generator_urls = {
        "aarch64-apple-darwin": "https://github.com/MaterializeInc/rules_rust/releases/download/v{0}/cargo-bazel-aarch64-apple-darwin".format(RULES_RUST_VERSION),
        "x86_64-apple-darwin": "https://github.com/MaterializeInc/rules_rust/releases/download/v{0}/cargo-bazel-x86_64-apple-darwin".format(RULES_RUST_VERSION),
        "aarch64-unknown-linux-gnu": "https://github.com/MaterializeInc/rules_rust/releases/download/v{0}/cargo-bazel-aarch64-unknown-linux-gnu".format(RULES_RUST_VERSION),
        "x86_64-unknown-linux-gnu": "https://github.com/MaterializeInc/rules_rust/releases/download/v{0}/cargo-bazel-x86_64-unknown-linux-gnu".format(RULES_RUST_VERSION),
    },
    # When `isolated` is true, Bazel will create a new `$CARGO_HOME`, i.e. it
    # won't use `~/.cargo`, when re-pinning. This is nice but not totally
    # necessary, and it makes re-pinning painfully slow, so we disable it.
    isolated = False,
    manifests = [
        "//:Cargo.toml",
        "//:src/adapter-types/Cargo.toml",
        "//:src/adapter/Cargo.toml",
        "//:src/alloc-default/Cargo.toml",
        "//:src/alloc/Cargo.toml",
        "//:src/arrow-util/Cargo.toml",
        "//:src/audit-log/Cargo.toml",
        "//:src/avro/Cargo.toml",
        "//:src/aws-secrets-controller/Cargo.toml",
        "//:src/aws-util/Cargo.toml",
        "//:src/balancerd/Cargo.toml",
        "//:src/build-info/Cargo.toml",
        "//:src/build-tools/Cargo.toml",
        "//:src/catalog-debug/Cargo.toml",
        "//:src/catalog-protos/Cargo.toml",
        "//:src/catalog/Cargo.toml",
        "//:src/ccsr/Cargo.toml",
        "//:src/cloud-api/Cargo.toml",
        "//:src/cloud-provider/Cargo.toml",
        "//:src/cloud-resources/Cargo.toml",
        "//:src/cluster-client/Cargo.toml",
        "//:src/cluster/Cargo.toml",
        "//:src/clusterd/Cargo.toml",
        "//:src/compute-client/Cargo.toml",
        "//:src/compute-types/Cargo.toml",
        "//:src/compute/Cargo.toml",
        "//:src/controller-types/Cargo.toml",
        "//:src/controller/Cargo.toml",
        "//:src/durable-cache/Cargo.toml",
        "//:src/dyncfg/Cargo.toml",
        "//:src/dyncfg-launchdarkly/Cargo.toml",
        "//:src/dyncfgs/Cargo.toml",
        "//:src/environmentd/Cargo.toml",
        "//:src/expr-parser/Cargo.toml",
        "//:src/expr-test-util/Cargo.toml",
        "//:src/expr/Cargo.toml",
        "//:src/fivetran-destination/Cargo.toml",
        "//:src/frontegg-auth/Cargo.toml",
        "//:src/frontegg-client/Cargo.toml",
        "//:src/frontegg-mock/Cargo.toml",
        "//:src/http-util/Cargo.toml",
        "//:src/interchange/Cargo.toml",
        "//:src/kafka-util/Cargo.toml",
        "//:src/lowertest-derive/Cargo.toml",
        "//:src/lowertest/Cargo.toml",
        "//:src/lsp-server/Cargo.toml",
        "//:src/materialized/Cargo.toml",
        "//:src/metabase/Cargo.toml",
        "//:src/metrics/Cargo.toml",
        "//:src/mysql-util/Cargo.toml",
        "//:src/mz/Cargo.toml",
        "//:src/npm/Cargo.toml",
        "//:src/orchestrator-kubernetes/Cargo.toml",
        "//:src/orchestrator-process/Cargo.toml",
        "//:src/orchestrator-tracing/Cargo.toml",
        "//:src/orchestratord/Cargo.toml",
        "//:src/orchestrator/Cargo.toml",
        "//:src/ore-build/Cargo.toml",
        "//:src/ore-proc/Cargo.toml",
        "//:src/ore/Cargo.toml",
        "//:src/persist-cli/Cargo.toml",
        "//:src/persist-client/Cargo.toml",
        "//:src/persist-proc/Cargo.toml",
        "//:src/persist-types/Cargo.toml",
        "//:src/persist/Cargo.toml",
        "//:src/pgcopy/Cargo.toml",
        "//:src/pgrepr-consts/Cargo.toml",
        "//:src/pgrepr/Cargo.toml",
        "//:src/pgtest/Cargo.toml",
        "//:src/pgtz/Cargo.toml",
        "//:src/pgwire-common/Cargo.toml",
        "//:src/pgwire/Cargo.toml",
        "//:src/postgres-client/Cargo.toml",
        "//:src/postgres-util/Cargo.toml",
        "//:src/prof-http/Cargo.toml",
        "//:src/prof/Cargo.toml",
        "//:src/proto/Cargo.toml",
        "//:src/regexp/Cargo.toml",
        "//:src/repr-test-util/Cargo.toml",
        "//:src/repr/Cargo.toml",
        "//:src/rocksdb-types/Cargo.toml",
        "//:src/rocksdb/Cargo.toml",
        "//:src/s3-datagen/Cargo.toml",
        "//:src/secrets/Cargo.toml",
        "//:src/segment/Cargo.toml",
        "//:src/self-managed-debug/Cargo.toml",
        "//:src/server-core/Cargo.toml",
        "//:src/service/Cargo.toml",
        "//:src/sql-lexer/Cargo.toml",
        "//:src/sql-parser/Cargo.toml",
        "//:src/sql-pretty/Cargo.toml",
        "//:src/sql-server-util/Cargo.toml",
        "//:src/sql/Cargo.toml",
        "//:src/sqllogictest/Cargo.toml",
        "//:src/ssh-util/Cargo.toml",
        "//:src/storage-client/Cargo.toml",
        "//:src/storage-controller/Cargo.toml",
        "//:src/storage-operators/Cargo.toml",
        "//:src/storage-types/Cargo.toml",
        "//:src/storage/Cargo.toml",
        "//:src/testdrive/Cargo.toml",
        "//:src/timely-util/Cargo.toml",
        "//:src/timestamp-oracle/Cargo.toml",
        "//:src/tls-util/Cargo.toml",
        "//:src/tracing/Cargo.toml",
        "//:src/transform/Cargo.toml",
        "//:src/txn-wal/Cargo.toml",
        "//:src/walkabout/Cargo.toml",
        "//:src/workspace-hack/Cargo.toml",
        "//:test/metabase/smoketest/Cargo.toml",
        "//:test/test-util/Cargo.toml",
        "//:misc/bazel/cargo-gazelle/Cargo.toml",
    ],
    rust_version = RUST_VERSION,
    # Restricting the set of platform triples we support _greatly_ reduces the
    # time it takes to "Splice Cargo Workspace" because it reduces the amount
    # of metadata that needs to be collected.
    #
    # Feel free to add more targets if need be but try to keep this list small.
    supported_platform_triples = [
        "aarch64-unknown-linux-gnu",
        "x86_64-unknown-linux-gnu",
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "wasm32-unknown-unknown",
    ],
    # Only used if developing rules_rust.
    # generator = "@cargo_bazel_bootstrap//:cargo-bazel",
)

load("@crates_io//:defs.bzl", "crate_repositories")

crate_repositories()

crate_universe_dependencies()

load("@rules_rust//cargo:deps.bzl", "cargo_dependencies")

cargo_dependencies()

# Third-Party Rust Tools
#
# A few crates bind to an external C/C++ library using `cxx`. To build these
# with Bazel we need to include the `cxx` command line binary.

load("//misc/bazel/rust_deps:repositories.bzl", "rust_repositories")

rust_repositories()

# Load and include any dependencies the third-party Rust binaries require.
#
# Ideally we would call `load(...)` from `rust_repositories()`, but load
# statements can only be called from the top-level WORKSPACE, so we must do it
# here.
#
# TODO(parkmycar): This should get better when we switch to bzlmod.

load("@cxxbridge//:defs.bzl", cxxbridge_cmd_deps = "crate_repositories")

cxxbridge_cmd_deps()

# git Submodules
#
# We include any git Submodules as local Bazel repositories so we can access
# their contents or build them.

new_local_repository(
    name = "fivetran_sdk",
    build_file = "//misc/bazel:git_submodules/BUILD.fivetran_sdk.bazel",
    path = "misc/fivetran-sdk",
)

# tools
#
# Extra non-critical tools (e.g. linters or formatters) that are used as part of the development
# cycle.
load("//misc/bazel/tools:repositories.bzl", "tools_repositories")

tools_repositories()
