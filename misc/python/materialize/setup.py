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

from setuptools import setup

if __name__ == "__main__":
    setup(
        name="materialize",
        version="0.2.0",
        description="Materialize test base.",
        author="Materialize, Inc.",
        author_email="support@materialize.com",
        packages=[
            "materialize",
            "materialize.cloudtest",
            "materialize.cloudtest.app",
            "materialize.cloudtest.k8s",
            "materialize.cloudtest.k8s.api",
            "materialize.cloudtest.util",
            "materialize.mzcompose",
        ],
        package_dir={"materialize": "."},
        install_requires=["pg8000", "semver", "sqlparse", "kubernetes"],
    )
