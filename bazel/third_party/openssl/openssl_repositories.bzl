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

"""A module defining the third party dependencies of OpenSSL"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def openssl_repositories():
    # Note: We need to make sure this stays in-sync with the version of openssl-src that we use.
    OPENSSL_VERSION = "1.1.1w"
    OPENSSL_INTEGRITY = "sha256-zzCYlQy02FOtlcCEHx+cbT3BAtzPys1SHZOSUgi3asg="

    maybe(
        http_archive,
        name = "openssl",
        build_file = Label("//bazel/third_party/openssl:BUILD.openssl.bazel"),
        integrity = OPENSSL_INTEGRITY,
        strip_prefix = "openssl-{0}".format(OPENSSL_VERSION),
        urls = [
            "https://www.openssl.org/source/openssl-{0}.tar.gz".format(OPENSSL_VERSION),
            "https://github.com/openssl/openssl/releases/download/openssl-{0}/openssl-{0}.tar.gz".format(OPENSSL_VERSION),
            "https://mirror.bazel.build/www.openssl.org/source/openssl-{0}.tar.gz".format(OPENSSL_VERSION),
        ],
    )

    RULES_PERL_VERISON = "0.1.0"
    RULES_PERL_SHA256 = "5cefadbf2a49bf3421ede009f2c5a2c9836abae792620ed2ff99184133755325"

    # Note: OpenSSL's uses Perl to configure its make file.
    maybe(
        http_archive,
        name = "rules_perl",
        sha256 = RULES_PERL_SHA256,
        strip_prefix = "rules_perl-{0}".format(RULES_PERL_VERISON),
        urls = [
            "https://github.com/bazelbuild/rules_perl/archive/refs/tags/{0}.tar.gz".format(RULES_PERL_VERISON),
        ],
    )
