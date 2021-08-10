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
from materialize.scratch import launched_by, tags, whoami, print_instances

import boto3


def main() -> None:
    check_required_vars()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "who",
        nargs="*",
        help="Whose instances to show (defaults to yourself)",
        default=[whoami()],
    )
    parser.add_argument("--all", help="Show all instances", action="store_true")

    args = parser.parse_args()

    filter = (lambda _i: True) if args.all else (lambda i: launched_by(tags(i)) in args.who)

    ists  = [i for i in boto3.resource('ec2').instances.all() if filter(i)]

    print_instances(ists)


if __name__ == "__main__":
    main()
