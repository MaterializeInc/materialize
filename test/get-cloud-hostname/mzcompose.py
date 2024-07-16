# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize.mz_env_util import get_cloud_hostname
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.mz import Mz

SERVICES = [
    Mz(app_password=""),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--app-password-env-var", type=str)
    args = parser.parse_args()

    app_password = os.environ[args.app_password_env_var]

    hostname = get_cloud_hostname(c, app_password=app_password, quiet=True)

    print(hostname)
