# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mz_env_util import get_cloud_hostname
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized")

    parser.add_argument("--app-password", type=str)
    args = parser.parse_args()

    c.up("materialized")

    hostname = get_cloud_hostname(c, app_password=args.app_password)

    print(hostname)
