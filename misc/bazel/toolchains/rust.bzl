# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_rust//rust:repositories.bzl", "rust_repository_set", "DEFAULT_TOOLCHAIN_TRIPLES")

def rust_toolchains(version, targets):
    """
    Macro that registers Rust toolchains for the provided targets.

    `rules_rust` provides a `rust_register_toolchains` macro but it fetches
    more Rust toolchains than we need, i.e. Linux, macOS, FreeBSD, and WASM.
    Instead the alternative of `rust_repostitory_set` is verbose enough that we
    abstract it away into this macro.
    """

    for (target, resources) in targets.items():
        # Support cross compiling to all other targets we specify.
        extra_targets = [t for t in targets.keys()]
        extra_targets.remove(target)

        integrity = {}
        for (resource, sha256) in resources.items():
            key = "{0}-{1}-{2}".format(resource, version, target)
            integrity[key] = sha256

        rust_repository_set(
            name = DEFAULT_TOOLCHAIN_TRIPLES[target],
            edition = "2021",
            exec_triple = target,
            extra_target_triples = extra_targets,
            versions = [version],
            sha256s = integrity,
            urls = [
                "https://github.com/MaterializeInc/toolchains/releases/download/rust-{VERSION}/{{}}.tar.zst".format(VERSION=version)
            ],
        )
