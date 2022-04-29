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

setup(
    name="dbt-materialize",
    # This adapter's minor version should match the required dbt-postgres version,
    # but patch versions may differ.
    # If you bump this version, bump it in __version__.py too.
    version="1.0.5",
    description="The Materialize adapter plugin for dbt (data build tool).",
    long_description=(Path(__file__).parent / "README.md").open().read(),
    long_description_content_type="text/markdown",
    author="Materialize, Inc.",
    author_email="support@materialize.com",
    url="https://github.com/MaterializeInc/dbt-materialize",
    packages=find_packages(),
    package_data={
        "dbt": [
            "include/materialize/dbt_project.yml",
            "include/materialize/macros/*.sql",
            "include/materialize/macros/**/*.sql",
        ]
    },
    install_requires=["dbt-postgres>=1.0,<1.2"],
    extras_require={
        "dev": ["pytest-dbt-adapter==0.6.0"],
    },
)
