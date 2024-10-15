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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def tools_repositories():
    """Fetch binaries for `buildifier`, the Bazel formatting tool."""

    BUILDIFIER_VERSION = "7.3.1"
    BUILDIFIER_TARGETS = {
        "darwin-amd64": "sha256-N1+CMQPQFiCq7CCgwpxsvKmfT9ByWuMLk2VcZwT0TXE=",
        "darwin-arm64": "sha256-Wmr8asegn1RVuguJvZnVriO0F03F3J1sDtXOjKrD+BM=",
        "linux-amd64": "sha256-VHTMUSinToBng9VAgfWBZixL6K5lAi9VfpKB7V3IgAk=",
        "linux-arm64": "sha256-C/hsS//69PCO7Xe95bIILkrlA5oR4uiwOYTBc8NKVhw=",
    }
    buildifier(BUILDIFIER_VERSION, BUILDIFIER_TARGETS)

def buildifier(version, targets):
    """
    Macro that downloads a pre-built `buildifier` binary for all of the specified targets.

    [`buildifier`](https://github.com/bazelbuild/buildtools)
    """

    for (target, integrity) in targets.items():
        maybe(
            http_file,
            name = "buildifier-{0}".format(target),
            executable = True,
            integrity = integrity,
            urls = [
                "https://github.com/MaterializeInc/toolchains/releases/download/buildifier-{VERSION}/buildifier-{TARGET}".format(VERSION = version, TARGET = target),
                "https://github.com/bazelbuild/buildtools/releases/download/v{VERSION}/buildifier-{TARGET}".format(VERSION = version, TARGET = target),
            ],
        )
