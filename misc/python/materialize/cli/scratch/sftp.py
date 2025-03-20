# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

from materialize.cli.scratch import check_required_vars
from materialize.scratch import get_instance, msftp


def configure_parser(parser: argparse.ArgumentParser) -> None:
    check_required_vars()

    parser.add_argument(
        "instance",
        help="The ID of the instance to connect to, or 'mine' to specify your only live instance",
    )


def run(args: argparse.Namespace) -> None:
    instance = get_instance(args.instance)
    msftp(instance)
