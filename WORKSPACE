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

RULES_FOREIGN_CC_VERSION = "de4a79280f54d8796e86b7ab0b631939b7b44d05"

RULES_FOREIGN_CC_INTEGRITY = "sha256-WwRg/GJuUjT3SMJEagTni2ZH+g3szIkHaqGgbYCN1u0="

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

# Version of the "toolchains_llvm" rule set, _not_ the version of clang/llvm.
TOOLCHAINS_LLVM_VERSION = "1.0.0"

TOOLCHAINS_LLVM_INTEGRITY = "sha256-6RxDYfmQEaVIFOGvvlxDbg0ymHEUajzVjCOitK+1Bzc="

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

DARWIN_SYSROOT_INTEGRITY = "sha256-k8OxF+DSVq0L1dy1S9TPqhFxDHF/bT32Ust3a1ldat8="

http_archive(
    name = "sysroot_darwin_universal",
    build_file_content = _SYSROOT_DARWIN_BUILD_FILE,
    integrity = DARWIN_SYSROOT_INTEGRITY,
    strip_prefix = "MacOSX{0}.sdk".format(DARWIN_SYSROOT_VERSION),
    urls = ["https://github.com/MaterializeInc/toolchains/releases/download/macos-sysroot-sdk-{0}/MacOSX{0}.sdk.tar.zst".format(DARWIN_SYSROOT_VERSION)],
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
LLVM_VERSION = "18.1.8"

# We have a few variants of our clang toolchain, either improving how it's built or adding new tools.
LLVM_VERSION_SUFFIX = "4"

maybe(
    http_archive,
    name = "toolchains_llvm",
    canonical_id = "{0}".format(TOOLCHAINS_LLVM_VERSION),
    integrity = TOOLCHAINS_LLVM_INTEGRITY,
    strip_prefix = "toolchains_llvm-{0}".format(TOOLCHAINS_LLVM_VERSION),
    url = "https://github.com/bazel-contrib/toolchains_llvm/releases/download/{0}/toolchains_llvm-{0}.tar.gz".format(TOOLCHAINS_LLVM_VERSION),
)

load("@toolchains_llvm//toolchain:deps.bzl", "bazel_toolchain_dependencies")

bazel_toolchain_dependencies()

load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")

llvm_toolchain(
    name = "llvm_toolchain",
    llvm_version = LLVM_VERSION,
    sha256 = {
        "darwin-aarch64": "41d8dea52d18c4e8b90c4fcd31965f9f297df9f40a38a33d60748dbe7f8330b8",
        "darwin-x86_64": "291b8dd844aa896b98393c5d3beaee57f294768039eacdf9ef5e96ed9d3f62d7",
        "linux-aarch64": "fe8f9e283ab43e963daf9ffb18742e134ad239b56078d61ef9a289ff642784ed",
        "linux-x86_64": "8b725ec14e48bc1cb3698309506e29cd94ff3b823976ebb306e9c3ef84480c16",
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

RULES_RUST_VERSION = "0.49.3"

RULES_RUST_INTEGRITY = "sha256-3QBrdyIdWeTRQSB8DnrfEbH7YNFEC4/KA7+SVheTKmA="

maybe(
    http_archive,
    name = "rules_rust",
    integrity = RULES_RUST_INTEGRITY,
    urls = [
        "https://github.com/bazelbuild/rules_rust/releases/download/{0}/rules_rust-v{0}.tar.gz".format(RULES_RUST_VERSION),
        "https://mirror.bazel.build/bazelbuild/rules_rust/releases/download/{0}/rules_rust-v{0}.tar.gz".format(RULES_RUST_VERSION),
    ],
)

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies")

rules_rust_dependencies()

# `rustc`
#
# Fetch and register the relevant Rust toolchains. We use a custom macro that
# depends on `rules_rust` but cuts down on bloat from their defaults.

RUST_VERSION = "1.81.0"

RUST_NIGHTLY_VERSION = "nightly/2024-09-28"

load("//misc/bazel/toolchains:rust.bzl", "rust_toolchains")

rust_toolchains(
    [
        RUST_VERSION,
        RUST_NIGHTLY_VERSION,
    ],
    {
        "aarch64-apple-darwin": {
            "stable": {
                "rustc": "b30363732d3130a91ff1f01bac4bd0ca24b0141faaba248a3daa5d62f8bfbbbe",
                "clippy": "53a9eaa19bd48ced7df5ea3f9ecbcb4eb3ff721f23b093fe8323f9bba484a9bb",
                "cargo": "d13872782e17b553f3364301931a7fc8580dbba4d67fcd1f7234b76ec255459c",
                "llvm-tools": "40ea6accc990351ea613759979c7f6af2eefe827c8661f988752d680b9c87613",
                "rust-std": "5dfe2282027eda4d330e90a3beee370460a992657a1173223608ddace1f4c727",
            },
            "nightly": {
                "rustc": "6b8326f5079d8cc4e0b19f605b514c6a44d0d50bb537f20d3687ef0f9038abcd",
                "clippy": "e0de5bd9fc2355f4e557f1fca3d0cde2a028624c6f7739d1e1381f02ae308f31",
                "cargo": "daedf9ea56eadd150c8fc7786ef37fb3e576d181c2a89c899ad3eb071a2a87e9",
                "llvm-tools": "6a9dd1bcc53c1299d0bc5813c42b612b69e35203c4863cabab8fc56975c8e56e",
                "rust-std": "fcaa21c80a3ccd5bdded5d64ba55d0ecb3d95e18ae8ff7c39304a7452c51ad2f",
            },
        },
        "aarch64-unknown-linux-gnu": {
            "stable": {
                "rustc": "16d5fd7d49cbafcdc9353ec65051fdc62e7c7a60422863c4f207ee67d6634a99",
                "clippy": "c1cf8e81a3033840416a4c157682a5100edeb3915433670465fd9ebe2ee29149",
                "cargo": "7435a5b710372236afcc019aeb37aa5e9ad480ae0967a7cbe17685b150530c8a",
                "llvm-tools": "78728a68726757b2c149d9adef856b8f957072729b7b42c7a5b223767487eecd",
                "rust-std": "f6f86d120f8da456787af3ff26d190d80d3661ae0fa37abb9ea7a4cf3ee3f58d",
            },
            "nightly": {
                "rustc": "21e7bc28f7d72e1d8338b5db8e4e3c1b547b8a10c7fbc81139a8b1742b0735de",
                "clippy": "043e0a42cab4f22a1dbb99ce12938abd1a22a4d1f6e20e05c786b2fa8402da62",
                "cargo": "080aa9149181591ca442b3d1f13d98eece642026864c03c99afb72e5435d3bcc",
                "llvm-tools": "d9e4bc0f5aa0a06b79e6968d0ac92b4e0ecfbd623d906af4c8d4747a276631cc",
                "rust-std": "2adb192e749d6d41c651125397a6746462006b082cebb4f944e5e6693a09ed93",
            },
        },
        "x86_64-unknown-linux-gnu": {
            "stable": {
                "rustc": "b64841b18eac5c973f292e94c3811ff86966a1d104bf0c8d12028ebe8404e066",
                "clippy": "fe29221d139887635eebb19ae2573d06a44f3f9a47b9f333787e14ed7d26a02c",
                "cargo": "2ac2e67fb28be728d4c8e9bb4f61d91d6110c54496884970a2555ab3998c5024",
                "llvm-tools": "206ace6919c2ef66fed30cdc36bf3853b03e50a5a1496a6ad9cfbb6d7ed25431",
                "rust-std": "93746c2ecc3bcb8ba1227f6093e5a2e95826a3a6a07b8c9e341ffd094ba9c252",
            },
            "nightly": {
                "rustc": "6a8d7356c59ef4b8bbc40db86f81316b648e8dc2b5ef5144ef39b7fa2984a346",
                "clippy": "34d966c493bf35ae0f1a785e688f7f8a361af2dabee4c154c78daa18d0d812a3",
                "cargo": "becc71bce584fa7affc3070b59cda01adb72dc1a54c9d18f2bcbe39976f9ad4a",
                "llvm-tools": "36ab4f68f33eb4c99bc8bb0436e7aa81e1f832ee58c6cedb466e0bb9b363a87c",
                "rust-std": "2c5165e77d830a15a57ce9a4ecb92b7bf58d8b8ff54ee9d4f3f01e7b826de4f5",
            },
        },
        "x86_64-apple-darwin": {
            "stable": {
                "rustc": "55d7a8d5f04d2973f2e66b4ffdb7f9370244ae90f472f326918dcd295f712ea3",
                "clippy": "587bf88f8ba7785d3c1898ff4bc4b648bd25e388d05a4742434543ac36dd0c7c",
                "cargo": "ae8fcebfe69901c2a6b80eba05d98bb816d0714187be149332c5cd842bbd101f",
                "llvm-tools": "62c530ce9c92de648284e73fea711b6abc440b01dcc4521f14f79d1a10bb5ccb",
                "rust-std": "20ee4286f3581253a13579cb42ef126f10108209ecc53bf86632da5e09656a9d",
            },
            "nightly": {
                "rustc": "2bbf5f312f8cc015f982eb2ecceae9285d924449bacd9c6f1e35e5a9aa051b05",
                "clippy": "8119fc8525b99c2fc480cd4b01f8efd3a2b9ebf7f5205d9000103b91b411c6a3",
                "cargo": "18f7ee92de402515f3edd87969ca7e2fd13316776800c440bcd54c29c744eaaf",
                "llvm-tools": "839c1009161ca464d5cee4d85b2e7e7bf3d5811e809b3a7db18c17f65268a733",
                "rust-std": "3d693eb4a72ae084ed371c360ece5485394a0562b14925180e9c3f2a2ac5cd63",
            },
        },
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
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.rocksdb.bazel",
            # Note: The below targets are from the additive build file.
            #
            # HACK(parkmycar): The `librocksdb-sys` build script runs bindgen for us, and to
            # support cross compiling we need to provide the sysroot to the build script so
            # bindgen can find it. Providing the sysroot and relying on the raw paths is quite
            # fragile, the fix is to use `@rules_rust//bindgen/...` rules with our Clang toolchain.
            build_script_data = [
                ":rocksdb_lib",
                ":rocksdb_include",
                ":snappy_lib",
                "@linux_sysroot-aarch64//:sysroot",
                "@linux_sysroot-x86_64//:sysroot",
            ],
            build_script_env = {
                "ROCKSDB_STATIC": "true",
                "ROCKSDB_LIB_DIR": "$(execpath :rocksdb_lib)",
                "ROCKSDB_INCLUDE_DIR": "$(execpath :rocksdb_include)",
                "SNAPPY_STATIC": "true",
                "SNAPPY_LIB_DIR": "$(execpath :snappy_lib)",
                "BINDGEN_EXTRA_CLANG_ARGS_aarch64-unknown-linux-gnu": "--sysroot=external/linux_sysroot-aarch64",
                "BINDGEN_EXTRA_CLANG_ARGS_x86_64-unknown-linux-gnu": "--sysroot=external/linux_sysroot-x86_64",
            },
            compile_data = [
                ":rocksdb_lib",
                ":rocksdb_include",
                ":snappy_lib",
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
            gen_build_script = False,
            deps = ["@zlib"],
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
    },
    cargo_config = "//:.cargo/config.toml",
    cargo_lockfile = "//:Cargo.lock",
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
        "//:src/catalog/Cargo.toml",
        "//:src/ccsr/Cargo.toml",
        "//:src/cloud-api/Cargo.toml",
        "//:src/cloud-resources/Cargo.toml",
        "//:src/cluster-client/Cargo.toml",
        "//:src/cluster/Cargo.toml",
        "//:src/clusterd/Cargo.toml",
        "//:src/compute-client/Cargo.toml",
        "//:src/compute-types/Cargo.toml",
        "//:src/compute/Cargo.toml",
        "//:src/controller-types/Cargo.toml",
        "//:src/controller/Cargo.toml",
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
        "//:src/metabase/Cargo.toml",
        "//:src/metrics/Cargo.toml",
        "//:src/mysql-util/Cargo.toml",
        "//:src/mz/Cargo.toml",
        "//:src/npm/Cargo.toml",
        "//:src/orchestrator-kubernetes/Cargo.toml",
        "//:src/orchestrator-process/Cargo.toml",
        "//:src/orchestrator-tracing/Cargo.toml",
        "//:src/orchestrator/Cargo.toml",
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
        "//:src/server-core/Cargo.toml",
        "//:src/service/Cargo.toml",
        "//:src/sql-lexer/Cargo.toml",
        "//:src/sql-parser/Cargo.toml",
        "//:src/sql-pretty/Cargo.toml",
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
