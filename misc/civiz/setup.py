# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

from setuptools import find_packages, setup

setup(
    name="civiz",
    version="0.1.0",
    description="CI visualization for materialize",
    packages=find_packages(),
    install_requires=["alembic", "flask", "psycopg2", "sqlalchemy", "uwsgi"],
)
