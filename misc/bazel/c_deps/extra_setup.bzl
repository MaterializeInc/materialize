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

load("@//misc/bazel/rules:utils.bzl", "maybe_github_archive")

def protoc_setup():
    """
    Extra setup required to build `protoc`.

    Nothing prevents these rules from being defined in `WORKSPACE`, but because
    they're specific to building `protoc` we split them out to reduce noise in
    that relatively crowded file.

    Note: We could use "@protobuf//:protobuf_deps.bzl", but that pulls in
    unneccessary toolchains that we don't need, like Java, so we manually
    specify dependencies here.
    """

    maybe_github_archive(
        name = "com_google_absl",
        repo = "https://github.com/abseil/abseil-cpp",
        commit = "4a2c63365eff8823a5221db86ef490e828306f9d",  # Abseil LTS 20240116.0
        sha256 = "2926ae3b70cb9a4cd4f6bb73eac2f16b7c02fa709a87a32a89634eaecc3ac208",
    )

    maybe_github_archive(
        name = "rules_ruby",
        repo = "https://github.com/protocolbuffers/rules_ruby",
        commit = "b7f3e9756f3c45527be27bc38840d5a1ba690436",
        sha256 = "44da36d57fe9f6c94e745f75d852e9acb9f1bd91cc5c14f16940c61669b66f1f",
    )

    maybe_github_archive(
        name = "utf8_range",
        repo = "https://github.com/protocolbuffers/utf8_range",
        commit = "d863bc33e15cba6d873c878dcca9e6fe52b2f8cb",
        sha256 = "c56f0a8c562050e6523a3095cf5610d19c190cd99bac622cc3e5754be51aaa7b",
    )
