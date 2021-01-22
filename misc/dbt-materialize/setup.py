# Copyright 2020 Josh Wills. All rights reserved.
# Copyright Materialize, Inc. All rights reserved.
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

from setuptools import find_packages
from distutils.core import setup
import os

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

package_name = "dbt-materialize"
package_version = "0.18.1"
description = """The Materialize adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Josh Wills",
    author_email="joshwills+dbt@gmail.com",
    url="https://github.com/MaterializeInc/dbt-materialize",
    packages=find_packages(),
    package_data={
        "dbt": [
            "include/materialize/dbt_project.yml",
            "include/materialize/macros/*.sql",
            "include/materialize/macros/**/*.sql",
        ]
    },
    install_requires=["dbt-postgres=={}".format(package_version)],
)
