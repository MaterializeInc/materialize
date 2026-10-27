# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_failures.py - Search historical CI failures.

import argparse
import json
import os
import sys
from datetime import date
from typing import Any

import requests

API_URL = "https://ci.dev.materialize.com/api/failures"
TOKEN_URL = "https://ci.dev.materialize.com/tokens"


def fetch_failures(
    *,
    filters: list[dict[str, str]],
    search: str | None,
    start: int,
    size: int,
    start_date: str | None,
    end_date: str | None,
    token: str,
) -> Any:
    params: dict[str, str | int] = {
        "start": start,
        "size": size,
        "sorting": json.dumps(
            [{"id": "build_date", "desc": True}], separators=(",", ":")
        ),
    }
    if filters:
        params["filters"] = json.dumps(filters, separators=(",", ":"))
    if search:
        params["globalFilter"] = search
    if start_date:
        params["dateMin"] = start_date
    if end_date:
        params["dateMax"] = end_date

    response = requests.get(
        API_URL,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def parse_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from error
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ci-failures",
        description="Search historical Materialize CI failures. All filters are ANDed.",
    )
    parser.add_argument(
        "pattern", nargs="?", help="case-insensitive failure content pattern"
    )
    parser.add_argument(
        "--search",
        help="search build, test, issue, content, and branch",
    )
    parser.add_argument("--branch", help="only return failures from this branch")
    parser.add_argument("--version", help="only return failures from this version")
    parser.add_argument("--issue", help="only return failures with this issue")
    parser.add_argument("--test", help="only return failures from this test suite")
    parser.add_argument("--build", help="only return failures from this build")
    parser.add_argument(
        "--start-date", type=parse_date, help="earliest build date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=parse_date, help="latest build date (YYYY-MM-DD)"
    )
    parser.add_argument("--start", type=int, default=0, help="result offset")
    parser.add_argument(
        "--size", type=int, default=31, help="number of results, from 1 to 100"
    )
    args = parser.parse_args(argv)

    if args.start < 0:
        parser.error("--start must be nonnegative")
    if not 1 <= args.size <= 100:
        parser.error("--size must be between 1 and 100")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("--end-date must not be earlier than --start-date")

    token = os.getenv("CI_DASHBOARD_TOKEN")
    if not token:
        parser.error(
            "CI_DASHBOARD_TOKEN is not set. "
            f"Create a token manually at {TOKEN_URL}, then export it as "
            "CI_DASHBOARD_TOKEN."
        )

    filters = []
    if args.pattern:
        filters.append({"id": "content", "value": args.pattern})
    for argument, column in (
        (args.branch, "branch"),
        (args.version, "mz_version"),
        (args.issue, "issue"),
        (args.test, "test_suite"),
        (args.build, "build_identifier"),
    ):
        if argument:
            filters.append({"id": column, "value": argument})

    try:
        failures = fetch_failures(
            filters=filters,
            search=args.search,
            start=args.start,
            size=args.size,
            start_date=args.start_date,
            end_date=args.end_date,
            token=token,
        )
    except requests.RequestException as error:
        print(f"ci-failures: request failed: {error}", file=sys.stderr)
        return 1

    json.dump(failures, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
