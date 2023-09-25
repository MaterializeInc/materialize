# Copyright 2020 Josh Wills. All rights reserved.
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

from pathlib import Path

from setuptools import find_namespace_packages, setup

README = Path(__file__).parent / "README.md"

setup(
    name="dbt-materialize",
    # This adapter's minor version should match the required dbt-postgres version,
    # but patch versions may differ.
    # If you bump this version, bump it in __version__.py too.
    version="1.5.1",
    description="The Materialize adapter plugin for dbt.",
    long_description=README.read_text(),
    long_description_content_type="text/markdown",
    author="Materialize, Inc.",
    author_email="support@materialize.com",
    url="https://github.com/MaterializeInc/materialize/blob/main/misc/dbt-materialize",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=["dbt-postgres~=1.5.0"],
    extras_require={
        "dev": ["dbt-tests-adapter~=1.5.0"],
    },
)
