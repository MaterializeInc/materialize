#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Score the docs with afdocs, and post the result to Slack.

afdocs (https://afdocs.dev) checks a docs site against the Agent-Friendly
Documentation Spec (https://agentdocsspec.com/spec/web/). It maps llms.txt
links back to pages by stripping ".md", which does not fit the docs'
markdown-docs/<path>/index.md links, so this script passes it every tenth page
listed in llms.txt, as HTML URLs. llms.txt may list the pages itself or link one
llms.txt per section; both are read.

Scoring and posting are separate commands, so that afdocs and its npm
dependencies never run in a process that holds a secret:

- `score` runs afdocs from the pinned install in ci/test/afdocs, with a
  scrubbed environment, and writes the afdocs JSON, the Slack message and the
  thread reply to a directory. It prints the message and the reply, so a dry
  run is a `score` without a `post`.
- `post` reads the message and the reply from that directory and posts them to
  a Slack channel with the bot token in $SLACK_TOKEN. It runs no third-party
  code.

Example usages:

    $ npm ci --ignore-scripts --prefix ci/test/afdocs
    $ ci/test/afdocs-report.py score https://materialize.com/docs --out afdocs-report
    $ SLACK_TOKEN=... ci/test/afdocs-report.py post afdocs-report --slack-channel docs
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

AFDOCS_DIR = Path(__file__).resolve().parent / "afdocs"
LINK_RE = re.compile(r"^- \[[^\]]*\]\(([^)\s]+)\)", re.MULTILINE)
REPORT_FILE = "report.json"
MESSAGE_FILE = "message.txt"
THREAD_FILE = "thread.txt"


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read().decode()


def sample_urls(base_url: str) -> list[str]:
    """Return every tenth page in llms.txt, as HTML URLs."""
    root = fetch(f"{base_url}/llms.txt")
    links = LINK_RE.findall(root)
    if links and all(link.endswith("/llms.txt") for link in links):
        links = [page for index in links for page in LINK_RE.findall(fetch(index))]
    pages = [
        link.replace("/markdown-docs/", "/", 1).removesuffix("index.md")
        for link in links
        if "/markdown-docs/" in link
    ]
    if not pages:
        sys.exit(f"{base_url}/llms.txt lists no markdown-docs pages to score")
    return pages[::10]


def afdocs_version() -> str:
    lock = json.loads((AFDOCS_DIR / "package-lock.json").read_text())
    return lock["packages"]["node_modules/afdocs"]["version"]


def run_afdocs(base_url: str, urls: list[str]) -> dict:
    afdocs = AFDOCS_DIR / "node_modules" / ".bin" / "afdocs"
    if not afdocs.exists():
        sys.exit(
            f"{afdocs} is missing; run: npm ci --ignore-scripts --prefix {AFDOCS_DIR}"
        )
    # Pass afdocs only what it needs to run, so that no token or credential in
    # this process's environment reaches it or its dependencies.
    env = {name: os.environ[name] for name in ("PATH", "HOME") if name in os.environ}
    result = subprocess.run(
        [
            str(afdocs),
            "check",
            base_url,
            "--urls",
            ",".join(urls),
            "--sampling",
            "deterministic",
            "--format",
            "json",
            "--score",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    # afdocs exits non-zero when a check fails; only missing output is an error.
    if not result.stdout.strip():
        sys.exit(f"afdocs produced no report:\n{result.stderr}")
    return json.loads(result.stdout)


def escape(text: str) -> str:
    """Escape the characters Slack mrkdwn treats as markup."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def failing(report: dict) -> list[dict]:
    return [c for c in report["results"] if c["status"] in ("fail", "warn")]


def message_text(report: dict, run_url: str) -> str:
    """Return the one-line summary, the same at any score but for its emoji."""
    score = report["scoring"]["overall"]
    emoji = ":white_check_mark:" if score == 100 else ":x:"
    link = f" <{run_url}|Workflow run>." if run_url else ""
    return (
        f"{emoji} The docs score {score} / 100 for agent-friendliness on"
        f" <https://afdocs.dev|afdocs>, over {report['testedPages']} pages:"
        f" {len(failing(report))} of {len(report['results'])} checks fail or warn.{link}"
    )


def thread_text(report: dict, version: str) -> str:
    """Return the thread reply: the failing checks, then the full report."""
    lines = []
    for check in failing(report):
        lines.append(
            f"• `{check['id']}` ({check['status']}): {escape(check['message'])}"
        )
    report_lines = [
        f"{report['url']}: {report['scoring']['overall']} / 100"
        f" ({report['scoring']['grade']}), afdocs {version},"
        f" {report['testedPages']} pages",
        "",
    ]
    for category, score in report["scoring"]["categoryScores"].items():
        report_lines.append(f"{score['score']:>4}  {category}")
    report_lines.append("")
    for check in report["results"]:
        report_lines.append(
            f"{check['status'].upper():<5} {check['id']}: {check['message']}"
        )
    lines.append("```\n" + escape("\n".join(report_lines)) + "\n```")
    return "\n".join(lines)


def post_message(
    token: str, channel: str, text: str, thread_ts: str | None = None
) -> str:
    body: dict[str, object] = {"channel": channel, "text": text, "unfurl_links": False}
    if thread_ts:
        body["thread_ts"] = thread_ts
    request = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        reply = json.load(response)
    if not reply.get("ok"):
        sys.exit(f"Slack chat.postMessage failed: {reply.get('error')}")
    return reply["ts"]


def score(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")
    version = afdocs_version()
    report = run_afdocs(base_url, sample_urls(base_url))
    message = message_text(report, args.run_url)
    thread = thread_text(report, version)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / REPORT_FILE).write_text(json.dumps(report, indent=2))
    (args.out / MESSAGE_FILE).write_text(message)
    (args.out / THREAD_FILE).write_text(thread)
    print(message)
    print()
    print(thread)
    return 0


def post(args: argparse.Namespace) -> int:
    token = os.environ.get("SLACK_TOKEN")
    if not token:
        sys.exit("SLACK_TOKEN is not set")
    message = (args.dir / MESSAGE_FILE).read_text()
    thread = (args.dir / THREAD_FILE).read_text()
    ts = post_message(token, args.slack_channel, message)
    post_message(token, args.slack_channel, thread, thread_ts=ts)
    print(f"Posted to #{args.slack_channel}.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)

    score_parser = commands.add_parser("score", help="score the docs with afdocs")
    score_parser.add_argument(
        "base_url", help="the docs root, such as https://materialize.com/docs"
    )
    score_parser.add_argument(
        "--out", type=Path, required=True, help="directory to write the results to"
    )
    score_parser.add_argument(
        "--run-url", default="", help="link to this run in the message"
    )
    score_parser.set_defaults(func=score)

    post_parser = commands.add_parser("post", help="post a scored report to Slack")
    post_parser.add_argument("dir", type=Path, help="the directory `score` wrote")
    post_parser.add_argument(
        "--slack-channel", required=True, help="post to this Slack channel"
    )
    post_parser.set_defaults(func=post)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
