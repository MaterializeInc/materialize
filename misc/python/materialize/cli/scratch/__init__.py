# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

# Sane defaults for internal Materialize use in the scratch account
DEFAULT_SUBNET_ID = "subnet-0b47df5733387582b"
DEFAULT_SG_ID = "sg-0f2d62ae0f39f93cc"
DEFAULT_INSTPROF_NAME = "ssm-instance-profile"


def check_required_vars() -> None:
    """Set reasonable default values for the
    environment variables necessary to interact with AWS."""
    if not os.environ.get("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = "mz-scratch-admin"
    if not os.environ.get("AWS_DEFAULT_REGION"):
        os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
