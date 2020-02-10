# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from setuptools import find_packages, setup

setup(
    name="civiz",
    version="0.1.0",
    description="CI visualization for materialize",
    packages=find_packages(),
    install_requires=["alembic", "flask", "psycopg2", "sqlalchemy", "uwsgi"],
)
