# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

MATERIALIZE_PROD_SANDBOX_HOSTNAME = os.environ["MATERIALIZE_PROD_SANDBOX_HOSTNAME"]
MATERIALIZE_PROD_SANDBOX_USERNAME = os.environ["MATERIALIZE_PROD_SANDBOX_USERNAME"]
MATERIALIZE_PROD_SANDBOX_APP_PASSWORD = os.environ[
    "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD"
]
DATABASE = "test_analytics"
SEARCH_PATH = "public"
