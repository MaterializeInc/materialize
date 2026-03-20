# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

from materialize import spawn
from materialize.cli.scratch import check_required_vars


def configure_parser(parser: argparse.ArgumentParser) -> None:
    pass


def run(args: argparse.Namespace) -> None:
    provider = getattr(args, "provider", None) or "aws"

    if provider == "hetzner":
        from materialize.scratch_hetzner import get_token, whoami

        try:
            token = get_token()
            identity = whoami()
            print(f"Hetzner Cloud token is configured. Identity: {identity}")
            print(f"Token: {token[:8]}...{token[-4:]}")
        except RuntimeError as e:
            print(str(e))
    else:
        check_required_vars("aws")
        spawn.runv(["aws", "sso", "login"])
