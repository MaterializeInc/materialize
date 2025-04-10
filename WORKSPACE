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

BAZEL_SKYLIB_VERSION = "1.7.1"

BAZEL_SKYLIB_INTEGRITY = "sha256-vCg8381SalLDIBJ5zaS8KYZS76iYsQtNsIN9xRZSdW8="

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

# C Repositories
#
# Loads all of the C dependencies that we rely on.
load("//misc/bazel/c_deps:repositories.bzl", "c_repositories")

c_repositories()

# `rules_cc`
#
# Rules for building C/C++ projects. These are slowly being upstreamed into the
# Bazel source tree, but some projects (e.g. protobuf) still depend on this
# rule set.

RULES_CC_VERSION = "0.1.1"

RULES_CC_INTEGRITY = "sha256-cS13hosxUt1hjE1k+q3e/MWWX5D13m5t0dXdzQvoLUI="

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

# `rules_rust`
#
# Rules for building Rust crates, and several convienence macros for building all transitive
# dependencies.

RULES_RUST_VERSION = "0.59.3"

RULES_RUST_INTEGRITY = "sha256-pPPz9Yewxoqs6ZhcQAaI8AeFCy/S/ipQTEkTbZ3syz4="

maybe(
    http_archive,
    name = "rules_rust",
    integrity = RULES_RUST_INTEGRITY,
    strip_prefix = "rules_rust-mz-{0}".format(RULES_RUST_VERSION),
    urls = [
        "https://github.com/MaterializeInc/rules_rust/releases/download/mz-{0}/rules_rust-mz-{0}.tar.zst".format(RULES_RUST_VERSION),
    ],
)

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies")

rules_rust_dependencies()

# `rustc`
#
# Fetch and register the relevant Rust toolchains. We use a custom macro that
# depends on `rules_rust` but cuts down on bloat from their defaults.

RUST_VERSION = "1.86.0"

RUST_NIGHTLY_VERSION = "nightly/2025-04-02"

load("//misc/bazel/toolchains:rust.bzl", "bindgen_toolchains", "rust_toolchains")

rust_toolchains(
    [
        RUST_VERSION,
        RUST_NIGHTLY_VERSION,
    ],
    {
        "aarch64-apple-darwin": {
            "stable": {
                "cargo": "707781ebdbee7ac3622acb32d6757cf9c08dab2c2f49b6f22978f0571e3682e1",
                "clippy": "40ab3c3cd20f946f3fc79f121d5fec8c1e1a0f3e2f21308e21c752c2152b2a80",
                "llvm-tools": "ac5c07d387302b8bec2a4abb88ca23dfa2d55982558918e451258b388ba9a864",
                "rust-std": "04f3b73e5bcbd5dfabed96726c0b5ba22c9e5fec6952932e01e3b89f0798f7d9",
                "rustc": "2fd62ce8a9d0c58e92a248820d5a22a0277bdc5efd200ce731cc87e9816ec5e2",
            },
            "nightly": {
                "cargo": "0e8376dc79c840063a8cdeeff0f53e255fb91ec4ead2ca5109a5ec9c3f13cf54",
                "clippy": "d1c40f30ccba46ce9802c10ff5e02034e8209ad2033f7b2a3a549f5c63ee7aed",
                "llvm-tools": "e036730779756e79c73db930e3fbf4e1f11a9a6d30041205ab906ffff15cc203",
                "rust-std": "937ee38916b93e1d6309f7ef709da0120aa3ee62c8e39f0241d90187aafa1df3",
                "rustc": "6a78b11a4740b7a90926dae1f2c8c43259077599ac50b8dd752814cea6959f96",
            },
        },
        "aarch64-unknown-linux-gnu": {
            "stable": {
                "cargo": "73e3e8a96b044de32fc242129fef9a4d332f4a0fe7265eff4272d3f3d9abde93",
                "clippy": "0c938032791951d5fce10d53ec35e12fba3e341b9b2cfd94bf6b6ce9582d2257",
                "llvm-tools": "462fdcbe9d2bc048d8cf9bf6d3b7fe168471ed39500d70111c874e6f32fff4b4",
                "rust-std": "aa0c2aff7bb69fb970e34d194830a3e9692b1299731b33228dee5f730d42e94e",
                "rustc": "113e4123cb4a3903d72dbf44db91938e7f2c37f3e73b04d10359145e9e395eeb",
            },
            "nightly": {
                "cargo": "8f0a8b12effea97fd1930747dbd70857225a5465037844bb39ed9d17e01abfdd",
                "clippy": "0a4bce6ad740405802d4c56d7ab9935825cc60bcc5f04bd1cc8896554a29833b",
                "llvm-tools": "331d5dd0330447734e05d184ba59b310d075c889b2aa1a3003b7b24db30572e4",
                "rust-std": "8f87d8e883cdca26233ead24eca0c62a39d0ff14b9813e8846f91f189ae635c2",
                "rustc": "f5e2ad3ec85851fc9b24f331bde0d4985ab99f42c12a995853baed0bae64ef3c",
            },
        },
        "x86_64-apple-darwin": {
            "stable": {
                "cargo": "85560e557f81b329407a8143281fa309e6a9d01a8104de6742081a2771f49df1",
                "clippy": "e38499209dd7da03990cdbbf4b7abebc084e4b0191b7252be238a17f96b87c42",
                "llvm-tools": "98e0408a0dc18bd9ba394ef40980775ab54e9b138dddd5d2141d2f0634676010",
                "rust-std": "e4c344cbb7d6e7088941a153fd48d57cc22041905b38552e7c7dd97f5bf68259",
                "rustc": "ff50df0bc0d1ab517746b2bf742afd13cb543cdeebb1b847f7c069cb6b3feb19",
            },
            "nightly": {
                "cargo": "d7c58810f851cb34110d5434a4f66f8bf444e896eb951e9cb744a7bef815c9a1",
                "clippy": "e8e38063a4b66393c5c3b4da95651bcb2e0ba8bbb25ac944f72713de2daabf1f",
                "llvm-tools": "93c5e0e64363b3e8a222df2703096731431abf1b08023a0737354e567c90988c",
                "rust-std": "67c0251a6d798a0990f7700f672a4c86b09ffad24d215860f84cfa32c4c81a5c",
                "rustc": "3c1228f40db684108cb5ff2e0b12adf5f1b5a1c3fa930bcbc6465eeacd55f3a9",
            },
        },
        "x86_64-unknown-linux-gnu": {
            "stable": {
                "cargo": "9dccd8fb635701a173fa74335d91837a264c0a1d7009d5f047851de217ddeeab",
                "clippy": "9a34aefbbdf8b5bc6dbaad8555cf12577d9ef60b167211515feadbd16afdcb28",
                "llvm-tools": "fbcc69f9c140a5fe0621c280628372233644818eb1826baadb303acdbc6e6395",
                "rust-std": "348e96d4d5261895d6014c428b54b40fd4c210f453b889b4180e19916c6b594e",
                "rustc": "c2c7f2b80ddd43834e53c25840ba68491671ac52af25f076059011677d922083",
            },
            "nightly": {
                "cargo": "74dc885d2f1374ca9999c094ab411b3d0abc9616ad2ea39c5dab429f0321e15d",
                "clippy": "ecafbe40e08b2c4557494544d1402364e3cfc6e729435fe41188e0ea147dc093",
                "llvm-tools": "4fb508d652140eb8d607f8d65f99f0dacc93b444645f634b02d9d18cae425cf1",
                "rust-std": "dfc87df7be3ed8ee35500272e0b87c472b1331578bf5c24f73f5344aa426c294",
                "rustc": "7207b8a852d82c3c396bf4374db808b5fa1bfec268cf02183db2a564f14d41af",
            },
        },
    },
)

# Rust `bindgen`
#
# Rules and Toolchains for running [`bindgen`](https://github.com/rust-lang/rust-bindgen)
# a tool for generating Rust FFI bindings to C.

maybe(
    http_archive,
    name = "rules_rust_bindgen",
    integrity = RULES_RUST_INTEGRITY,
    strip_prefix = "rules_rust-mz-{0}/extensions/bindgen".format(RULES_RUST_VERSION),
    urls = [
        "https://github.com/MaterializeInc/rules_rust/releases/download/mz-{0}/rules_rust-mz-{0}.tar.zst".format(RULES_RUST_VERSION),
    ],
)

load("@rules_rust_bindgen//:repositories.bzl", "rust_bindgen_dependencies")

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
        "aws-lc-sys": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.aws-lc.bazel",
            gen_build_script = False,
            # Note: This is a target we add from the additive build file above.
            deps = [":aws_lc"],
        )],
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
            compile_data_glob_excludes = ["rocksdb/**"],
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
        "insta": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
        "similar": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
    },
    cargo_config = "//:.cargo/config.toml",
    cargo_lockfile = "//:Cargo.lock",
    generator_sha256s = {
        "aarch64-apple-darwin": "c38c9c0efc11fcf9c32b9e0f4f4849df7c823f207c7f5ba5f6ab1e0e2167693d",
        "aarch64-unknown-linux-gnu": "5bdc9a10ec5f17f5140a81ce7cb0c0ce6e82d4d862d3ce3a301ea23f72f20630",
        "x86_64-unknown-linux-gnu": "abcd8212d64ea4c0f5e856af663c05ebeb2800a02c251f6eb62061f4e8ca1735",
    },
    generator_urls = {
        "aarch64-apple-darwin": "https://github.com/MaterializeInc/rules_rust/releases/download/mz-{0}/cargo-bazel-aarch64-apple-darwin".format(RULES_RUST_VERSION),
        "aarch64-unknown-linux-gnu": "https://github.com/MaterializeInc/rules_rust/releases/download/mz-{0}/cargo-bazel-aarch64-unknown-linux-gnu".format(RULES_RUST_VERSION),
        "x86_64-unknown-linux-gnu": "https://github.com/MaterializeInc/rules_rust/releases/download/mz-{0}/cargo-bazel-x86_64-unknown-linux-gnu".format(RULES_RUST_VERSION),
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
        "//:src/auth/Cargo.toml",
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
        "//:src/expr-derive/Cargo.toml",
        "//:src/expr-derive-impl/Cargo.toml",
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
        "//:src/license-keys/Cargo.toml",
        "//:src/lowertest-derive/Cargo.toml",
        "//:src/lowertest/Cargo.toml",
        "//:src/lsp-server/Cargo.toml",
        "//:src/materialized/Cargo.toml",
        "//:src/metabase/Cargo.toml",
        "//:src/metrics/Cargo.toml",
        "//:src/mysql-util/Cargo.toml",
        "//:src/mz/Cargo.toml",
        "//:src/mz-debug/Cargo.toml",
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
