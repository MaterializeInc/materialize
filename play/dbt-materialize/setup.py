#!/usr/bin/env python
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
