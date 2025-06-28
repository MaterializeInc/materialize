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

RUST_VERSION = "1.88.0"

RUST_NIGHTLY_VERSION = "nightly/2025-06-28"

load("//misc/bazel/toolchains:rust.bzl", "bindgen_toolchains", "rust_toolchains")

rust_toolchains(
    [
        RUST_VERSION,
        RUST_NIGHTLY_VERSION,
    ],
    {
        "aarch64-apple-darwin": {
            "stable": {
                "cargo": "bf7cdf06cbed243f4ed0f9c852a6aa080797d1350a455282033091ec2526d7cc",
                "clippy": "a1ebfd7eac1dd1e879cde7d4a775689e5bd7d80c576930604c7589576d4fe2fe",
                "llvm-tools": "8d864504e0f4e5e37ad81840f5a353f5029128a713e2907d8c13da2f3cfba4c3",
                "rust-std": "00c1bb3ddb383c9aaace344332f2e523552464abf5133645a49764b3c02b0421",
                "rustc": "d278edbf97698d63779dfe8116c252cd957d25f284ae282a2d435a49710b6a35"
            },
            "nightly": {
                "cargo": "efe388459b9cb6dc2d0bf3cd75ccd6acc9dbfa61928691af3bd46d892e078ae4",
                "clippy": "8fc88c6421e02c2b9afb7b9ab6dfd981a5a9df189f80fd6f580ee8fbd053aa66",
                "llvm-tools": "c19f937183373639e0cd5f69f784a889cb3e0424943c20c90ee486b95f1e5777",
                "rust-std": "71745eb4d21138fa4e1fd900101423887e32f1ac8c4464c1e09068a04e668486",
                "rustc": "e77d89b08325f8e48b444887a63685be5cfba007aa421468cdaafc7ad11ccadf"
            },
        },
        "aarch64-unknown-linux-gnu": {
            "stable": {
                "cargo": "e3877f6e13076924335d4a55edfa4d0f0d26affde0208cd6e84289eeb98cd142",
                "clippy": "1872bb486bfd146982aa9d62aa3fe71dc9c0085eb810988e8a38a17d40015f80",
                "llvm-tools": "db8e48dfaf01617b1504d1993c143305034abb0823a3da0920d9e66b3f6bbdee",
                "rust-std": "525d0e622e8832c997c6deb9d9fbcbde73376c0c3a3a9325d7ecee17d6a781dc",
                "rustc": "db3ba9ea32ad6fc0073011ddeaa22fa0f54effd455c7c189bba6edc65fa3be02",
            },
            "nightly": {
                "cargo": "59cd66c433362868848e9006f212e9dc8cafb2c5fd18125dd0c4d578af518f0d",
                "clippy": "e53cf91e18431babd154fd90673120f104d86596131fe7368679f3e411f4a195",
                "llvm-tools": "f9c3de5cb8be8a188c8cd0fbd930587d08de53e84959e251d89d508df9517245",
                "rust-std": "f07461eb88cf74a30e65074ead447cf6241c2d2b517e6d7e975611af8e496205",
                "rustc": "0978bc7c2e30cb86bdab482adda80f1463382e8a8de9e7f8f4193411e95aeacc",
            },
        },
        "x86_64-apple-darwin": {
            "stable": {
                "cargo": "2fce4753d4b1db641ba8d3a6b88c8aa32c4e647025edcecaf56509ff25167e28",
                "clippy": "d8485a21f968ea3c8d376165245fe4c108e19bf78bb6ebbb36a19d590aaee9a5",
                "llvm-tools": "62b95c0193be2bc185089c2d9595ddde76fbeeb366a851688fc0701e478321ca",
                "rust-std": "54939bc8868d358471d6c75edbc3699b1c533b9016fb883bc40b94f5a387a39d",
                "rustc": "9ef9e30341e1ffba8911d7c0932820398ec40d18070140b1b5ae3b45505421b1",
            },
            "nightly": {
                "cargo": "a386f271be4a0e3b1ebcabd21bcd952d254742aa186d244c4147b2fa7c23db97",
                "clippy": "f11376ea76263c869d61b5c5167fdc4e5e22df4c05acbec5a844639131fee84f",
                "llvm-tools": "a4637e5fdd6ffffc578785584bfc763239a72793049a8e82169b0ced5aea5ccb",
                "rust-std": "82bfd882298bb7c3f3d411e6802e299819de1611c5f8911fd21fdd4e02cce8b1",
                "rustc": "a1e936b584003ed3349087114a259c9863f503e9086dea55924bc6f164e2d020",
            },
        },
        "x86_64-unknown-linux-gnu": {
            "stable": {
                "cargo": "19d17e0823d028f6724894aafb448604182e306315178862cbd9adaedbf0c4d4",
                "clippy": "9c1b6dc33b7cc5176b09a27f7aba3fcda6d6001001e7e9b840fbd11ad745ca5f",
                "llvm-tools": "f7fa5ec5d30ab7d5719721200872b40cd3a6fc662853f0fec90119737917edba",
                "rust-std": "3690f22a19fb1e967c5547c00212b3574a1d711f81644e910248ea09488de9d0",
                "rustc": "c3be3242b48b07e75608f52f3621d469e0f35d2e7091344fcfc4adcb51dda3e3",
            },
            "nightly": {
                "cargo": "df68f8a59aeb24ee59d8be66397ed69adb0ba1301f5246dd281a2efdeaa50521",
                "clippy": "0e1174d5962f50f3c27c3e3b67254c8e3e4f5496a98d49e55014f79ab38ecf29",
                "llvm-tools": "ac13f78c6437e7b29fc51aaa8263773f58b68e57c2656e41e96529fb0e374fca",
                "rust-std": "86da8256ed210aa387ad7188939450123024f62cacc2b91651a49a6c5b429a36",
                "rustc": "112ab1bd953f60e80cce587378596d314285113b5722da850365946c907a3523",
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
