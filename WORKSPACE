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

# TODO:
# * Set the relevant `CARGO_*` env variables for crates that get build info.
# * Should we statically link our built version of `protobuf` to protobuf-native?
# * Statically link libz-sys instead of letting its build script run
# * Provide our own sysroot to the llvm_toolchain for cross compilation
#

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
        "https://mirror.bazel.build/bazelbuild/rules_cc/releases/download/{0}/rules_cc-{0}.tar.gz".format(RULES_CC_VERSION),
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

# Version of clang/llvm we use.
#
# At the moment this is the newest version that is pre-compiled for all of the platforms we support.
LLVM_VERSION = "16.0.0"

maybe(
    http_archive,
    name = "toolchains_llvm",
    integrity = TOOLCHAINS_LLVM_INTEGRITY,
    strip_prefix = "toolchains_llvm-{0}".format(TOOLCHAINS_LLVM_VERSION),
    canonical_id = "{0}".format(TOOLCHAINS_LLVM_VERSION),
    url = "https://github.com/bazel-contrib/toolchains_llvm/releases/download/{0}/toolchains_llvm-{0}.tar.gz".format(TOOLCHAINS_LLVM_VERSION),
)

load("@toolchains_llvm//toolchain:deps.bzl", "bazel_toolchain_dependencies")
bazel_toolchain_dependencies()

load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")
llvm_toolchain(
    name = "llvm_toolchain",
    llvm_version = LLVM_VERSION,
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
        "https://mirror.bazel.build/bazelbuild/rules_perl/archive/refs/tags/{0}.tar.gz".format(RULES_PERL_VERISON),
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

RULES_RUST_VERSION = "0.44.0"
RULES_RUST_INTEGRITY = ""

maybe(
    http_archive,
    name = "rules_rust",
    integrity = RULES_RUST_INTEGRITY,
    urls = [
        "https://mirror.bazel.build/bazelbuild/rules_rust/releases/download/{0}/rules_rust-v{0}.tar.gz".format(RULES_RUST_VERSION),
        "https://github.com/bazelbuild/rules_rust/releases/download/{0}/rules_rust-v{0}.tar.gz".format(RULES_RUST_VERSION),
    ],
)
# http_archive(
#     name = "rules_rust",
#     sha256 = "",
#     strip_prefix = "rules_rust-1854e99de1d668612dbe44afdd1f1270cf8e402c",
#     urls = ["https://github.com/ParkMyCar/rules_rust/archive/1854e99de1d668612dbe44afdd1f1270cf8e402c.tar.gz"],
# )

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies")
rules_rust_dependencies()



# `rustc`
#
# Fetch and register the relevant Rust toolchains. We use a custom macro that
# depends on `rules_rust` but cuts down on bloat from their defaults.

RUST_VERSION = "1.77.1"

load("//misc/bazel/toolchains:rust.bzl", "rust_toolchains")
rust_toolchains(
    RUST_VERSION,
    [
        "aarch64-apple-darwin",
        "aarch64-unknown-linux-gnu",
        "x86_64-unknown-linux-gnu",
        "x86_64-apple-darwin",
    ]
)

# Load all dependencies for crate_universe.
load("@rules_rust//crate_universe:repositories.bzl", "crate_universe_dependencies")
crate_universe_dependencies()

load("@rules_rust//crate_universe:defs.bzl", "crates_repository", "crate")
crates_repository(
    name = "crate_index",
    cargo_lockfile = "//:Cargo.lock",
    lockfile = "//:Cargo.Bazel.lock",
    annotations = {
        "librocksdb-sys": [crate.annotation(
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.rocksdb.bazel",
            # Note: The below targets are from the additive build file.
            build_script_env = {
                "ROCKSDB_STATIC": "true",
                "ROCKSDB_LIB_DIR": "$(execpath :rocksdb_lib)",
                "ROCKSDB_INCLUDE_DIR": "$(execpath :rocksdb_include)",
            },
            build_script_data = [
                ":rocksdb_lib",
                ":rocksdb_include",
            ],
            compile_data = [":rocksdb_lib"],
        )],
        "tikv-jemalloc-sys": [crate.annotation(
            build_script_env = {
                "JEMALLOC_OVERRIDE": "$(execpath @jemalloc//:libjemalloc)",
            },
            build_script_data = ["@jemalloc//:libjemalloc"],
            compile_data = ["@jemalloc//:libjemalloc"],
        )],
        "rdkafka-sys": [crate.annotation(
            gen_build_script = False,
            additive_build_file = "@//misc/bazel/c_deps:rust-sys/BUILD.librdkafka.bazel",
            # Note: This is a target we add from the additive build file above.
            deps = [":librdkafka"],
            compile_data = [":rdkafka_lib"],
            rustc_flags = [
                "-Lnative=$(execpath :rdkafka_lib)",
                "-lstatic=rdkafka",
            ]
        )],
        "launchdarkly-server-sdk": [crate.annotation(
            gen_build_script = False,
            additive_build_file = "@//misc/bazel/third_party/cargo:launchdarkly-server-sdk/include.BUILD.bazel",
            # Note: This is a target we add from the additive build file above.
            compile_data = [":launchdarky_server_sdk_out_dir"],
            rustc_env = {
                "OUT_DIR": "$(location :launchdarky_server_sdk_out_dir)",
            },
        )],
        "protobuf-src": [crate.annotation(
            # Note: We shouldn't ever depend on protobuf-src, but if we do, don't try to bootstrap
            # `protoc`.
            gen_build_script = False,
            rustc_env = {
                "INSTALL_DIR": "fake",
            },
        )],
        "protobuf-native": [crate.annotation(
            gen_build_script = False,
            additive_build_file = "@//misc/bazel/third_party/cargo:protobuf-native/include.BUILD.bazel",
            deps = [
                ":compiler-sys",
                ":compiler-bridge",
                ":io-sys",
                ":io-bridge",
                ":lib-sys",
                ":lib-bridge",
                ":internal-bridge",
            ],
            # build_script_data = ["@//misc/bazel/c_deps/rust-sys:dep_protobuf_src_root"],
            # build_script_env = {
            #     "CARGO_TARGET_DIR": "/tmp",
            #     "DEP_PROTOBUF_SRC_ROOT": "$(execpath @//misc/bazel/c_deps/rust-sys:dep_protobuf_src_root)",
            # },
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
    },
    packages = {
        "cxx": crate.spec(
            version = "1.0.109",
        ),
    },
    manifests = [
        "//:Cargo.toml",
        "//:src/adapter-types/Cargo.toml",
        "//:src/adapter/Cargo.toml",
        "//:src/alloc/Cargo.toml",
        "//:src/alloc-default/Cargo.toml",
        "//:src/arrow-util/Cargo.toml",
        "//:src/audit-log/Cargo.toml",
        "//:src/avro/Cargo.toml",
        "//:src/aws-secrets-controller/Cargo.toml",
        "//:src/aws-util/Cargo.toml",
        "//:src/balancerd/Cargo.toml",
        "//:src/build-info/Cargo.toml",
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
        "//:src/persist-cli/Cargo.toml",
        "//:src/persist-client/Cargo.toml",
        "//:src/persist-proc/Cargo.toml",
        "//:src/persist-txn/Cargo.toml",
        "//:src/persist-types/Cargo.toml",
        "//:src/persist/Cargo.toml",
        "//:src/pgcopy/Cargo.toml",
        "//:src/pgrepr-consts/Cargo.toml",
        "//:src/pgrepr/Cargo.toml",
        "//:src/pgtest/Cargo.toml",
        "//:src/pgtz/Cargo.toml",
        "//:src/pgwire-common/Cargo.toml",
        "//:src/pgwire/Cargo.toml",
        "//:src/pid-file/Cargo.toml",
        "//:src/postgres-client/Cargo.toml",
        "//:src/postgres-util/Cargo.toml",
        "//:src/proc/Cargo.toml",
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
        "//:src/walkabout/Cargo.toml",
        "//:src/workspace-hack/Cargo.toml",
        "//:test/metabase/smoketest/Cargo.toml",
        "//:test/test-util/Cargo.toml",
        "//src/ore:Cargo.toml",
        "//src/ore-proc:Cargo.toml",
        "//src/proto:Cargo.toml",
        "//src/build-tools:Cargo.toml",
    ],
    # Useful when iterating.
    isolated = False,
    # Only used if developing rules_rust.
    # generator = "@cargo_bazel_bootstrap//:cargo-bazel",
)

load("@crate_index//:defs.bzl", "crate_repositories")
crate_repositories()

load("@rules_rust//crate_universe:repositories.bzl", "crate_universe_dependencies")
crate_universe_dependencies()

# `cxx`
#
# A few crates bind to an external C/C++ library using `cxx`.
#
# TODO(parkmycar): A more detailed explanation as to why we build `cxxbridge`.

maybe(
    http_archive,
    name = "cxxbridge-cmd",
    build_file_content = """
load("@rules_rust//rust:defs.bzl", "rust_binary")
load("@cxxbridge_cmd_deps//:defs.bzl", "aliases", "all_crate_deps")

rust_binary(
    name = "cxxbridge-cmd",
    srcs = glob(["src/**/*.rs"]),
    aliases = aliases(),
    data = [
        "src/gen/include/cxx.h",
    ],
    edition = "2021",
    visibility = ["//visibility:public"],
    deps = all_crate_deps(
        normal = True,
    ),
)
    """,
    sha256 = "d93600487d429c8bf013ee96719af4e62e809ac57fc4cac24f17cf58e4526009",
    strip_prefix = "cxxbridge-cmd-1.0.109",
    type = "tar.gz",
    urls = ["https://crates.io/api/v1/crates/cxxbridge-cmd/1.0.109/download"],
)

crates_repository(
    name = "cxxbridge_cmd_deps",
    cargo_lockfile = "@cxxbridge-cmd//:Cargo.lock",
    # lockfile = "@cxxbridge-cmd//:cargo-bazel-lock.json",
    manifests = ["@cxxbridge-cmd//:Cargo.toml"],
    # Only used if developing rules_rust.
    # generator = "@cargo_bazel_bootstrap//:cargo-bazel",
)
load("@cxxbridge_cmd_deps//:defs.bzl", cxxbridge_cmd_deps = "crate_repositories")
cxxbridge_cmd_deps()

# `bindgen-cli`
#
# TODO(parkmycar): A better explanation here.

# maybe(
#     http_archive,
#     name = "bindgen-cli",
#     build_file_content = """
# load("@rules_rust//rust:defs.bzl", "rust_binary")
# load("@bindgen_cli_deps//:defs.bzl", "aliases", "all_crate_deps")

# rust_binary(
#     name = "bindgen-cli",
#     srcs = glob(["src/**/*.rs"]),
#     crate_root = "main.rs",
#     deps = all_crate_deps(normal = True),
#     crate_features = ["runtime"],
#     rustc_flags = ["--cap-lints=allow"],
#     aliases = aliases(),
#     edition = "2021",
#     visibility = ["//visibility:public"],
# )
# """,
#     integrity = "sha256-XTFHn0TSye7TaOX5RctIRtLIMTvZH+sWtq6TeoC3rgE=",
#     strip_prefix = "bindgen-cli-0.69.4",
#     type = "tar.gz",
#     urls = ["https://crates.io/api/v1/crates/bindgen-cli/0.69.4/download"]
# )

# crates_repository(
#     name = "bindgen_cli_deps",
#     cargo_lockfile = "@bindgen-cli//:Cargo.lock",
#     # lockfile = "@bindgen-cli//:cargo-bazel-lock.json",
#     manifests = ["@bindgen-cli//:Cargo.toml"],
#     # Only used if developing rules_rust.
#     # generator = "@cargo_bazel_bootstrap//:cargo-bazel",
# )
# load("@bindgen_cli_deps//:defs.bzl", bindgen_cli_deps = "crate_repositories")
# bindgen_cli_deps()
