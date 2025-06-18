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
LLVM_VERSION = "20.1.6"

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
        "darwin-aarch64": "02fb0842f75ec34f3df2162c27d01773bd122cf27e01e9e051bb56f9edec66f1",
        "darwin-x86_64": "4b382146c4483cf68b730df3329a975505205dc73dc86cfe9f3bacad06e22d49",
        "linux-aarch64": "46af59ce8c53338d90b72c401b5560d44bbb02df118f0f0daf08bf897a05f1a9",
        "linux-x86_64": "acf8c45aaa3f012d1f140893b5760daafd5965635941a4999c68d2d04a8aae4f",
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

RUST_VERSION = "1.87.0"

RUST_NIGHTLY_VERSION = "nightly/2025-06-04"

load("//misc/bazel/toolchains:rust.bzl", "bindgen_toolchains", "rust_toolchains")

rust_toolchains(
    [
        RUST_VERSION,
        RUST_NIGHTLY_VERSION,
    ],
    {
        "aarch64-apple-darwin": {
            "stable": {
                "cargo": "2ca1a3985075bc8fa1638a8a7eb04905af417e3c4833604d52e012f186a4c56d",
                "clippy": "1b15eeccb24141ab14f1aebbd7bdd33012dd6f2bbf43d947f2e0939800df42b8",
                "llvm-tools": "cdeb364a8b4781452ec96572467b9eed81d4211cd35940b9f7f397a114af7cd2",
                "rust-std": "6140cd97ec645291fa93ae227eb112a5a30e9b9757565d395fc93b57b30d8a1a",
                "rustc": "bdf07b74624a35f41b6d4455062231136528ca2bc1ee2b516868ae013e281d30",
            },
            "nightly": {
                "cargo": "f6d8abf91b0aad5ced38bddf42042f76ddd3d191f59a4490a709a7d6149f21f4",
                "clippy": "0560eb77cc1574cdbd3a281b5c752f4bd22e5960d93edfefeaa0704b4b5f7e58",
                "llvm-tools": "f4caae6c32115f7409c8131e86fc602552cd7a6b759d8c3e9fa374a652dd9635",
                "rust-std": "5ffd6aba88685980c0f6fadcf6d073c8b0293afc05d9aa781ad0ef9ed677200f",
                "rustc": "1174cdaa16831009e72c95b5c7aab0e1537d8bf3fed934c6f0ae18230f462bd0",
            },
        },
        "aarch64-unknown-linux-gnu": {
            "stable": {
                "cargo": "ee961f7e7ea9ea48d6d50269abfbba65ecb68ef95ffeb73cef2fc1aa73b75d06",
                "clippy": "1cdf15a797a796a4513743d2b0d1bde54275509761cf8bfcc869c9a8d0210bb7",
                "llvm-tools": "c723b7b1d7df57b3a175fa442454db1f764f2233d5bf8a69116e0777471e2709",
                "rust-std": "cbbe24a9830f07d564813d629594dd9b8893ee8db2f3a02d67b4747d695cd91b",
                "rustc": "4523e003a77db00da419685e1429315ebfc1fec8b2ab5ceaad90971e9be96b49",
            },
            "nightly": {
                "cargo": "0c1ef992b461962902c2128206b30ff8432e8942becec48e112257de5717744a",
                "clippy": "bdac515d1da8a11ec648456fc5d9ad0fbc8f2dd5b58d074b5227dbfb09f56c1e",
                "llvm-tools": "f7ffbfae0cbcf2b4818f1421cdbaea3a0e4a1bc4d582c4f65e51a3e4938320ff",
                "rust-std": "01032e1657b5ad397be7bc88ff41ac53f08ec4d5d0eb5817045abfc3e72e29cf",
                "rustc": "1c9a4568b073a4722bc4e1a788093b0830bee094ab79ac83e942959349128321",
            },
        },
        "x86_64-apple-darwin": {
            "stable": {
                "cargo": "cf12c42e436e3960ee55a19b90b575bb1f4ce73f2820871e53c21b13ceacb3b0",
                "clippy": "47b62e75295f8a327063c5ad11c0a2d32f590f85920da7211396d267b29f5cac",
                "llvm-tools": "aaa8fe29e325a7a7ae792b1bcf601411b89ca76dfef44e1147c23c4c18305b70",
                "rust-std": "1595f38b5bffce3554c6603252157e2017a20f14ef16520da3bea22a9a6527a6",
                "rustc": "0823c207629121f95087fe366ddfb0d96016cc3b50f7d1ea9e9f2d478ff391d5",
            },
            "nightly": {
                "cargo": "0d5ed7f93a482e8cc0d90592e1d2e893946079a7aa897f9ce281ccdf1be97ee1",
                "clippy": "3c124247341985c50d9b4e9c414e1b165e48f7b21c298c78b6b922dd1715cace",
                "llvm-tools": "52f0a178ccff6ecd693849bf2e6a4dda329ae0a08997a9ad78d75a90137afc08",
                "rust-std": "d1d2feeb1c19f2e37a83aa0547305e848b066248d960c2e7258d26ab679eb92d",
                "rustc": "05e0932f511d547ae4d320163e61762cfe98d203b30d7a84ecc5883312c70c0f",
            },
        },
        "x86_64-unknown-linux-gnu": {
            "stable": {
                "cargo": "f1fb338417952ea7ee7b5c4150f05b7f2d9d47d92499cd7a8859d4c78823f710",
                "clippy": "56cc2ce095cf6fc2b680ed2527b7bf6fbe45819edb4a7258483f521f5d37c971",
                "llvm-tools": "52cbd5a6ebedf55cc27751b1014ef636ff245f030b7fe3549bfb1660823df3da",
                "rust-std": "f4dff0fe501a8c8e1d3fed5213096e18a8c4fefbbdfd78457a3638a449f99fdd",
                "rustc": "923bcb7be267579a79ae9a11126739619f180a6544bcc1c0c3a752c1aecd6e7a",
            },
            "nightly": {
                "cargo": "ab913b6c917006457eae204d8bbf4d5b82b83de5523e07c82bf56a6d1a0ac220",
                "clippy": "4b0b69758ff1178d6986ff1ffc898a13ef83095006e176be1b0fb9a6c325c20c",
                "llvm-tools": "331db10a12c2145b480aa3945c48e4b7b1a935a42d1782b42c82a7d0cfb1655a",
                "rust-std": "11d0e186cdad492347448b96710c97060f9f4bc38789b0c51601b5b17a2e837c",
                "rustc": "2d2a74da9ab9ca8e77600c76dad5d04e1e004ffc3f0557a4670a1536e1150b24",
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
        "darwin_aarch64": "sha256-QxhHE2vvRcbe1ppF9ZonKWa4Y6AwnP8cVsCO0xLkZvU=",
        "darwin_x86_64": "sha256-PcZ4VlQ8vxZP3mffLXj3vxh7VImDfIUvFdqR9z+uFNc=",
        "linux_aarch64": "sha256-W8GBaB49peXQunPziisAWm8K6xLF/WcOijOHXlHe6Bk=",
        "linux_x86_64": "sha256-8TXbBywRoUcZ/Aulm2Ps+FOU2RBKWA32AQDKCPoOyUw=",
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
        "adler2": [crate.annotation(rustc_flags = ["-Copt-level=3"])],
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
        "//:src/authenticator/Cargo.toml",
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
        "//:src/dyncfg-file/Cargo.toml",
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
