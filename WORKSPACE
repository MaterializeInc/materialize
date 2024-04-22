# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

# `clang`/`llvm`
#
# Normally Bazel will use the system's version of clang as the default C
# toolchain. This prevents the builds from being hermetic though so we include
# our own.
#
# All of the clang related tools are provided under the `@llvm_toolchain_llvm`
# repo. To see what's available run `bazel query @llvm_toolchain_llvm//...`.

TOOLCHAINS_LLVM_VERSION = "1.0.0"
TOOLCHAINS_LLVM_SHA256 = "e91c4361f99011a54814e1afbe5c436e0d329871146a3cd58c23a2b4afb50737"

http_archive(
    name = "toolchains_llvm",
    sha256 = TOOLCHAINS_LLVM_SHA256,
    strip_prefix = "toolchains_llvm-{0}".format(TOOLCHAINS_LLVM_VERSION),
    canonical_id = "{0}".format(TOOLCHAINS_LLVM_VERSION),
    url = "https://github.com/bazel-contrib/toolchains_llvm/releases/download/{0}/toolchains_llvm-{0}.tar.gz".format(TOOLCHAINS_LLVM_VERSION),
)

load("@toolchains_llvm//toolchain:deps.bzl", "bazel_toolchain_dependencies")
bazel_toolchain_dependencies()

load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")
llvm_toolchain(
    name = "llvm_toolchain",
    llvm_version = "16.0.0",
)

load("@llvm_toolchain//:toolchains.bzl", "llvm_register_toolchains")
llvm_register_toolchains()

# `rules_foreign_cc`
#
# Rules for building C/C++ projects that use foreign build systems, e.g. Make. One case that we use
# this for is building `openssl`.
#
# TODO(parkmycar): Move off of my fork.

RULES_FOREIGN_CC_VERSION = "b2b2825e8800c982b8b2d4355047987c26d8b1ad"
RULES_FOREIGN_CC_SHA256 = "f6f5dc552231fa3399dacfc0b221d6461da7bbfbf5e11b8cd93ce4e63f17b585"

http_archive(
    name = "rules_foreign_cc",
    sha256 = RULES_FOREIGN_CC_SHA256,
    strip_prefix = "rules_foreign_cc-{0}".format(RULES_FOREIGN_CC_VERSION),
    url = "https://github.com/ParkMyCar/rules_foreign_cc/archive/{0}.tar.gz".format(RULES_FOREIGN_CC_VERSION),
)
load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

# `bazel-lib`
#
# Provides generic rules for Bazel to help make things fit together.
#
# For example, Rust build scripts might require files live in a certain
# location, but the dependent `c_library` can't specify an output location.
# `bazel-lib` provides rules to copy files into a new directory that we can
# then provide to the Rust rule.
#
# Note: There is also `bazel-skylib` which serves a similar purpose, but
# provides less functionality.

ASPECT_BAZEL_LIB_VERSION = "2.7.0"
ASPECT_BAZEL_LIB_SHA256 = "357dad9d212327c35d9244190ef010aad315e73ffa1bed1a29e20c372f9ca346"

http_archive(
    name = "aspect_bazel_lib",
    sha256 = ASPECT_BAZEL_LIB_SHA256,
    strip_prefix = "bazel-lib-{0}".format(ASPECT_BAZEL_LIB_VERSION),
    url = "https://github.com/aspect-build/bazel-lib/releases/download/v{0}/bazel-lib-v{0}.tar.gz".format(ASPECT_BAZEL_LIB_VERSION),
)

load("@aspect_bazel_lib//lib:repositories.bzl", "aspect_bazel_lib_dependencies", "aspect_bazel_lib_register_toolchains")

# Required bazel-lib dependencies
aspect_bazel_lib_dependencies()

# Register bazel-lib toolchains
aspect_bazel_lib_register_toolchains()

# `openssl`
#
# Load additional toolchains and dependencies required for building openssl.

load("//bazel/third_party/openssl:openssl_repositories.bzl", "openssl_repositories")
openssl_repositories()

load("//bazel/third_party/openssl:openssl_setup.bzl", "openssl_setup")
openssl_setup()
