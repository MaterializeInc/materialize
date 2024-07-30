# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize.test_analytics.config.mz_db_config import MzDbConfig

# Note that after changing database structures, one will need to run
# > GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN DATABASE test_analytics_playground TO "infra+qacanaryload@materialize.io";


def create_local_dev_config_for_sandbox() -> MzDbConfig:
    hostname = os.environ["MATERIALIZE_PROD_SANDBOX_HOSTNAME"]
    username = os.environ["MATERIALIZE_PROD_SANDBOX_USERNAME"]
    app_password = os.environ["MATERIALIZE_PROD_SANDBOX_APP_PASSWORD"]

    database = "test_analytics_playground"
    search_path = "public"
    cluster = "test_analytics"

    return MzDbConfig(
        hostname=hostname,
        username=username,
        app_password=app_password,
        database=database,
        search_path=search_path,
        cluster=cluster,
        enabled=True,
        application_name="sandbox",
    )
