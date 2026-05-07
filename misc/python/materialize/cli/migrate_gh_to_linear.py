# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import json
import os
import re
import subprocess
import sys
from typing import Any

import requests

from materialize import spawn

GITHUB_REPO = "MaterializeInc/database-issues"
LINEAR_API_URL = "https://api.linear.app/graphql"

LABEL_MAP = {
    "ci-flake": "CI Flake",
    "test-blocker": "Test Blocker",
    "release-blocker": "Release Blocker",
    "T-security": "Security",
    "C-bug": "Bug",
    "C-feature": "Feature",
}

LABEL_TO_TEAM = {
    "A-sql": "SQL",
    "A-optimization": "SQL",
    "A-coordinator": "SQL",
    "A-ADAPTER": "SQL",
    "A-storage": "SS",
    "T-sources": "SS",
    "T-sinks": "SS",
    "T-kafka": "SS",
    "T-PG": "SS",
    "T-mysql": "SS",
    "T-debezium": "SS",
    "T-s3": "SS",
    "T-ICEBERG": "SS",
    "T-subsource": "SS",
    "T-sqlserver": "SS",
    "A-webhooks": "SS",
    "A-compute": "CLU",
    "A-dataflow": "CLU",
    "A-CLUSTER": "CLU",
    "A-PERSIST": "DB",
    "A-console": "CNS",
    "A-infra": "QAR",
    "A-build": "QAR",
    "T-testing": "QAR",
    "A-monitoring": "DB",
    "T-observability": "DB",
    "A-tracing": "DB",
    "A-docs": "DEX",
    "A-backend": "DB",
    "A-controller": "DB",
    "A-platform": "DB",
}


GH_TO_LINEAR_USER = {
    "aljoscha": "9b290678-4120-4ad4-a69f-df4f1e8e9c64",
    "Alphadelta14": "2ddf091f-20aa-4db7-9c09-6efa78470d5f",
    "antiguru": "0dcfe0c6-0631-4b4a-b409-91cea985a27b",
    "bosconi": "2721aa79-f4f4-43bc-b5d1-0fd4717f6e23",
    "DAlperin": "f69e5038-fec2-4b57-9512-7fac8e7387de",
    "def-": "6d6e4bba-365d-406e-98a1-2538ffd1a882",
    "doy-materialize": "21a8819b-aa96-4c70-946c-ff4af431352d",
    "ggevay": "3fd3b7f5-038d-422f-9682-bf38185102d4",
    "jubrad": "ba122e7c-6b3f-4daf-98e8-91373cd8bf77",
    "martykulma": "18e88ba3-4257-4df5-89e7-a011b27eaa38",
    "mgree": "7d8fbcc5-713b-4162-91f2-9f9f5b188fb6",
    "mtabebe": "fb996665-32a5-4e2a-903b-8b67c010d62c",
    "patrickwwbutler": "dbfeb9d9-b913-4f10-820d-cc3de0acba35",
    "petrosagg": "713e3034-6a5d-4c42-8676-f8298d3af904",
    "SangJunBak": "41416df0-176c-49fb-a872-1d0e50f9cecc",
}

LANG_ALIASES = {
    "python3": "python",
    "py": "python",
    "rs": "rust",
    "js": "javascript",
    "ts": "typescript",
    "sh": "bash",
    "zsh": "bash",
}


def normalize_code_blocks(text: str) -> str:
    def replace_lang(m: re.Match[str]) -> str:
        lang = m.group(1)
        return f"```{LANG_ALIASES.get(lang, lang)}"

    return re.sub(r"```(\w+)", replace_lang, text)


def gh_fetch_issue(repo: str, number: int) -> dict[str, Any]:
    data = json.loads(spawn.capture(["gh", "api", f"repos/{repo}/issues/{number}"]))
    return {
        "number": data["number"],
        "title": data["title"],
        "state": data["state"],
        "url": data["html_url"],
        "body": data.get("body") or "",
        "labels": [l["name"] for l in data.get("labels", [])],
        "assignees": [a["login"] for a in data.get("assignees", [])],
        "author": data["user"]["login"],
    }


def gh_fetch_comments(repo: str, number: int) -> list[dict[str, Any]]:
    raw = spawn.capture(
        ["gh", "api", f"repos/{repo}/issues/{number}/comments", "--paginate"]
    )
    return [
        {
            "author": c["user"]["login"],
            "date": c["created_at"][:10],
            "body": c.get("body") or "",
        }
        for c in json.loads(raw)
    ]


def linear_gql(
    api_key: str, query: str, variables: dict[str, Any] | None = None
) -> dict[str, Any]:
    resp = requests.post(
        LINEAR_API_URL,
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError(f"Linear API error: {data['errors']}")
    return data["data"]


def linear_find_team(api_key: str, team_key: str) -> dict[str, Any]:
    data = linear_gql(
        api_key,
        "query($f:TeamFilter){teams(filter:$f){nodes{id name key}}}",
        {"f": {"key": {"eq": team_key}}},
    )
    teams = data["teams"]["nodes"]
    if not teams:
        raise RuntimeError(f"No Linear team with key '{team_key}'")
    return teams[0]


def linear_find_label(api_key: str, team_id: str, label_name: str) -> str | None:
    q = "query($f:IssueLabelFilter){issueLabels(filter:$f){nodes{id}}}"
    for filt in [
        {"name": {"eq": label_name}, "team": {"id": {"eq": team_id}}},
        {"name": {"eq": label_name}, "team": {"null": True}},
    ]:
        nodes = linear_gql(api_key, q, {"f": filt})["issueLabels"]["nodes"]
        if nodes:
            return nodes[0]["id"]
    return None


def linear_fetch_team_states(api_key: str, team_id: str) -> dict[str, str]:
    data = linear_gql(
        api_key,
        "query($f:WorkflowStateFilter){workflowStates(filter:$f){nodes{id name}}}",
        {"f": {"team": {"id": {"eq": team_id}}}},
    )
    return {s["name"]: s["id"] for s in data["workflowStates"]["nodes"]}


def linear_create_issue(api_key: str, inp: dict[str, Any]) -> dict[str, Any]:
    data = linear_gql(
        api_key,
        "mutation($i:IssueCreateInput!){issueCreate(input:$i){success issue{id identifier url}}}",
        {"i": inp},
    )
    if not data["issueCreate"]["success"]:
        raise RuntimeError(f"Failed to create issue: {inp.get('title')}")
    return data["issueCreate"]["issue"]


def linear_add_attachment(api_key: str, issue_id: str, url: str, title: str) -> None:
    linear_gql(
        api_key,
        "mutation($i:AttachmentCreateInput!){attachmentCreate(input:$i){success}}",
        {"i": {"issueId": issue_id, "url": url, "title": title}},
    )


def linear_add_comment(api_key: str, issue_id: str, body: str) -> None:
    linear_gql(
        api_key,
        "mutation($i:CommentCreateInput!){commentCreate(input:$i){success}}",
        {"i": {"issueId": issue_id, "body": body}},
    )


def migrate_issue(api_keys: dict[str, str], gh: dict[str, Any], dry_run: bool) -> None:
    api_key = api_keys.get(gh["author"], api_keys["default"])
    labels = gh["labels"]
    team_key = next((LABEL_TO_TEAM[l] for l in labels if l in LABEL_TO_TEAM), "DB")
    mapped = [LABEL_MAP[l] for l in labels if l in LABEL_MAP]
    unmapped = [l for l in labels if l not in LABEL_MAP and l not in LABEL_TO_TEAM]

    label_info = f"labels=[{', '.join(labels) or 'none'}] -> team={team_key}"
    if mapped:
        label_info += f", linear=[{', '.join(mapped)}]"
    if unmapped:
        label_info += f", skipped=[{', '.join(unmapped)}]"
    assignee_info = ""
    if gh["assignees"]:
        assignee_info = f", assignees=[{', '.join(gh['assignees'])}]"
    print(f"#{gh['number']}: {gh['title']} ({label_info}{assignee_info})", flush=True)
    if gh["state"] == "closed":
        print("  skipped: already closed", flush=True)
        return
    if dry_run:
        print("  [dry-run] would create Linear issue and close GH issue", flush=True)
        return

    team = linear_find_team(api_key, team_key)
    label_ids = []
    for l in labels:
        if l in LABEL_MAP:
            lid = linear_find_label(api_key, team["id"], LABEL_MAP[l])
            if lid:
                label_ids.append(lid)
            else:
                print(f"  warning: Linear label '{LABEL_MAP[l]}' not found", flush=True)

    state_id = linear_fetch_team_states(api_key, team["id"]).get("Backlog")
    desc = normalize_code_blocks(gh["body"])
    inp: dict[str, Any] = {
        "teamId": team["id"],
        "title": gh["title"],
        "description": desc,
    }
    if label_ids:
        inp["labelIds"] = label_ids
    if state_id:
        inp["stateId"] = state_id
    for assignee in gh["assignees"]:
        if assignee in GH_TO_LINEAR_USER:
            inp["assigneeId"] = GH_TO_LINEAR_USER[assignee]
            break
        else:
            print(f"  warning: no Linear user for GH assignee '{assignee}'", flush=True)

    issue = linear_create_issue(api_key, inp)
    linear_add_attachment(api_key, issue["id"], gh["url"], "GitHub Issue")
    print(f"  -> {issue['identifier']} {issue['url']}", flush=True)

    comments = gh_fetch_comments(GITHUB_REPO, gh["number"])
    if comments:
        body = (
            f"**Migrated comments from GitHub#{gh['number']}:**\n\n---\n\n"
            + "\n\n---\n\n".join(
                f"**{c['author']}** ({c['date']}):\n\n{normalize_code_blocks(c['body'])}"
                for c in comments
            )
        )
        linear_add_comment(api_key, issue["id"], body)
        print(f"  -> migrated {len(comments)} comment(s)", flush=True)

    subprocess.run(
        [
            "gh",
            "issue",
            "close",
            str(gh["number"]),
            "--repo",
            GITHUB_REPO,
            "--reason",
            "not planned",
            "--comment",
            f"Migrated to Linear: {issue['url']}",
        ],
        check=True,
        capture_output=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate GitHub issues to Linear")
    parser.add_argument("issue_id", nargs="+", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_keys: dict[str, str] = {}
    api_keys["default"] = os.environ.get("LINEAR_API_KEY", "")
    if os.environ.get("LINEAR_API_KEY_DEF"):
        api_keys["def-"] = os.environ["LINEAR_API_KEY_DEF"]
    if not api_keys["default"] and not args.dry_run:
        print("Error: set LINEAR_API_KEY environment variable", file=sys.stderr)
        sys.exit(1)

    for num in args.issue_id:
        migrate_issue(api_keys, gh_fetch_issue(GITHUB_REPO, num), args.dry_run)


if __name__ == "__main__":
    main()
