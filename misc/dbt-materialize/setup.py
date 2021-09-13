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

from distutils.core import setup
from pathlib import Path

from setuptools import find_packages

# This adapter's version, and its required dbt-postgres version, track the
# target dbt version. If you need to release a new version of this adapter
# without bumping the dbt-postgres version, change version_suffix to ".post1",
# ".post2", etc. Reset version_suffix back to "" when bumping the
# dbt_postgres_version.
dbt_postgres_version = "0.20.2"
version_suffix = ""

setup(
    name="dbt-materialize",
    version=f"{dbt_postgres_version}{version_suffix}",
    description="The Materialize adapter plugin for dbt (data build tool).",
    long_description=(Path(__file__).parent / "README.md").open().read(),
    long_description_content_type="text/markdown",
    author="Materialize, Inc.",
    author_email="support@materialize.io",
    url="https://github.com/MaterializeInc/dbt-materialize",
    packages=find_packages(),
    package_data={
        "dbt": [
            "include/materialize/dbt_project.yml",
            "include/materialize/macros/*.sql",
            "include/materialize/macros/**/*.sql",
        ]
    },
    install_requires=["dbt-postgres=={}".format(dbt_postgres_version)],
)
