# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_ledger.py - Read and review the CI ledger of failures on main and releases.

import argparse
import json
import os
import re
import sys
from typing import Any

import requests

BASE_URL = os.getenv("CI_DASHBOARD_URL", "https://ci.dev.materialize.com")
TOKEN_URL = "https://ci.dev.materialize.com/tokens"

DESCRIPTION = """\
Read and review the CI ledger: every failed job on main and on each release
(its release candidates included), grouped into patterns that are reviewed
once. A pattern's review is shared by main and every release it fails in.

Entries are named by their id, or by a Buildkite job id or job URL, which
stands for every entry the job failed with. Output is JSON. Editing needs a
CI_DASHBOARD_TOKEN created with "ledger:write" access, and is recorded under
the token creator's name. All fields: {base}/docs
"""

# A ledger entry id: 16 hex digits.
ENTRY_ID_RE = re.compile(r"^[0-9a-f]{16}$")
JOB_ID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# What `list` shows of an entry; `show` prints all of it.
SUMMARY_FIELDS = (
    "id",
    "scope",
    "effective_status",
    "failure_type",
    "cause",
    "bug",
    "fix",
    "title",
    "tests",
    "failures",
    "last_seen",
    "updated_by",
)


class LedgerError(Exception):
    pass


class Ledger:
    def __init__(self, token: str, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.session.request(
            method, f"{self.base_url}/api/ledger{path}", timeout=60, **kwargs
        )
        if response.status_code == 403 and method != "GET":
            raise LedgerError(
                "this token cannot edit the ledger: create one with "
                f'"ledger:write" access at {TOKEN_URL}'
            )
        if not response.ok:
            try:
                detail = response.json()["detail"]
            except (ValueError, KeyError, TypeError):
                detail = response.text[:500]
            raise LedgerError(f"{response.status_code}: {detail}")
        return response.json()

    def entries(self, **params: str | None) -> list[dict[str, Any]]:
        params = {k: v for k, v in params.items() if v is not None}
        return self.request("GET", "/entries", params=params)["data"]

    def resolve(self, ref: str) -> list[str]:
        """The entry ids `ref` names: an entry id, or a Buildkite job id or URL,
        which names every entry the job failed with."""
        if ENTRY_ID_RE.match(ref):
            return [ref]
        if not JOB_ID_RE.search(ref.lower()):
            raise LedgerError(
                f"{ref!r} is neither a ledger entry id nor a Buildkite job id or URL"
            )
        found = self.entries(job=ref)
        if not found:
            raise LedgerError(
                f"no ledger entry for job {ref}: the ledger only has failed jobs "
                "of main and release builds, and not before it started"
            )
        return [entry["id"] for entry in found]

    def update(self, ids: list[str], changes: dict[str, Any]) -> list[dict[str, Any]]:
        if len(ids) == 1:
            return [self.request("PATCH", f"/entries/{ids[0]}", json=changes)]
        return self.request("PATCH", "/entries", json={"ids": ids, "changes": changes})[
            "data"
        ]


def summary(entry: dict[str, Any]) -> dict[str, Any]:
    result = {field: entry.get(field) for field in SUMMARY_FIELDS}
    for field in ("bug", "fix"):
        value = result[field]
        if isinstance(value, dict):
            result[field] = value.get("value")
    result["latest_job"] = (entry.get("latest") or {}).get("job_url")
    return result


def update_changes(args: argparse.Namespace) -> dict[str, Any]:
    """The fields `update` sets: only those given. A fix implies `fixed`."""
    changes: dict[str, Any] = {}
    for field, value in (
        ("status", args.status),
        ("failure_type", args.type),
        ("cause", args.cause),
        ("bug", args.bug),
        ("fix", args.fix),
        ("linked", args.linked),
    ):
        if value is not None:
            changes[field] = value
    if args.fix and args.status is None:
        changes["status"] = "fixed"
    return changes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ci-ledger",
        description=DESCRIPTION.format(base=BASE_URL),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)

    list_parser = commands.add_parser(
        "list", help="list entries, unreviewed first, newest failure first within"
    )
    list_parser.add_argument(
        "--scope", default="all", help="main, a release (v26.50.0), or all"
    )
    list_parser.add_argument(
        "--status",
        choices=("unreviewed", "reviewed", "fixed"),
        help="unreviewed includes fixed entries that failed again",
    )
    list_parser.add_argument("--search", help="text in titles, errors, tests, causes")
    list_parser.add_argument("--job", help="only what this Buildkite job failed with")
    list_parser.add_argument(
        "--full", action="store_true", help="every field, not a summary per entry"
    )

    show_parser = commands.add_parser(
        "show", help="an entry with its failed jobs, errors, and review history"
    )
    show_parser.add_argument(
        "ref",
        metavar="ENTRY",
        help="entry id, or Buildkite job id or URL of a job that failed with one entry",
    )

    update_parser = commands.add_parser(
        "update",
        help="set the review of entries: status, type, cause, bug, fix",
        description="Set the review of entries. Fields not given stay as they "
        'are, "" clears one. A fixed entry that fails again in a later build of '
        "its release (or main) counts as unreviewed there.",
    )
    update_parser.add_argument(
        "refs",
        nargs="+",
        metavar="ENTRY",
        help="entry id, or Buildkite job id or URL for every entry the job failed with",
    )
    update_parser.add_argument(
        "--status",
        choices=("unreviewed", "reviewed", "fixed"),
        help="fixed needs a fix, given now or before",
    )
    update_parser.add_argument(
        "--type",
        choices=("deterministic", "flaky", "infrastructure"),
        help="flaky: fails sometimes; infrastructure: CI itself failed",
    )
    update_parser.add_argument("--cause", help="what makes it fail, in a sentence")
    update_parser.add_argument(
        "--bug", help="related bug: SQL-123, database-issues/123, or a URL"
    )
    update_parser.add_argument(
        "--fix",
        help="the PR that fixes it, e.g. its GitHub URL; implies --status fixed",
    )
    link = update_parser.add_mutually_exclusive_group()
    link.add_argument(
        "--unlink",
        dest="linked",
        action="store_false",
        default=None,
        help="give this release (or main) a review of its own",
    )
    link.add_argument(
        "--link",
        dest="linked",
        action="store_true",
        default=None,
        help="use the review shared with main and the other releases again",
    )

    args = parser.parse_args(argv)

    token = os.getenv("CI_DASHBOARD_TOKEN")
    if not token:
        parser.error(
            "CI_DASHBOARD_TOKEN is not set. "
            f"Create a token manually at {TOKEN_URL}, then export it as "
            'CI_DASHBOARD_TOKEN. Editing needs one with "ledger:write" access.'
        )

    ledger = Ledger(token)
    try:
        if args.command == "list":
            entries = ledger.entries(
                scope=args.scope, status=args.status, q=args.search, job=args.job
            )
            output: Any = entries if args.full else [summary(e) for e in entries]
        elif args.command == "show":
            ids = ledger.resolve(args.ref)
            if len(ids) > 1:
                raise LedgerError(
                    f"job {args.ref} failed with {len(ids)} entries, show one by its "
                    f"id: {', '.join(ids)}"
                )
            output = ledger.request("GET", f"/entries/{ids[0]}")
        else:
            changes = update_changes(args)
            if not changes:
                parser.error(
                    "nothing to update: give --status, --type, --cause, --bug or --fix"
                )
            ids = list(
                dict.fromkeys(i for ref in args.refs for i in ledger.resolve(ref))
            )
            output = [summary(e) for e in ledger.update(ids, changes)]
    except (LedgerError, requests.RequestException) as error:
        print(f"ci-ledger: {error}", file=sys.stderr)
        return 1

    json.dump(output, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
