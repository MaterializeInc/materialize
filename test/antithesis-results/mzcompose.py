# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Waits for the Antithesis run that ci/test/build-antithesis.sh launched and
prints a block per distinct failure, which bin/ci-annotate-errors annotates.
"""

import json
import os
import re
import sys
import time
from collections import defaultdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from materialize import spawn
from materialize.mzcompose.composition import Composition

SERVICES = []

# Attaches the panic message, the log line after "panicked at", to the panic.
PANICS = 'contains({output_text: "panicked at"}).with_next({msg: matches({stream: "error"})}, 1)'


def describe(event: dict) -> str:
    if "antithesis_assert" in event:
        return json.dumps(event["antithesis_assert"].get("details"))
    crashed = event.get("source", {}).get("container")
    msg = event.get("last_panic", {})
    header = msg.get("last_event", {})
    # The panic and its message are the nearest matches in the whole timeline,
    # so either can come from another container or an earlier process. In past
    # runs, a crash followed its panic within 7 ms.
    if (
        crashed
        and all(
            e.get("source", {}).get("container", "").startswith(crashed)
            for e in (msg, header)
        )
        and float(event["moment"]["vtime"]) - float(header["moment"]["vtime"]) < 1
    ):
        location = header["output_text"].split("panicked at ", 1)[-1]
        return f"panicked at {location} {msg['output_text']}"
    return event.get("output_text") or json.dumps(
        {k: v for k, v in event.items() if k not in ("moment", "source", "last_panic")}
    )


def workflow_default(c: Composition) -> None:
    run = spawn.capture(
        ["buildkite-agent", "meta-data", "get", "antithesis-run-id"]
    ).strip()
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {os.environ['ANTITHESIS_API_KEY']}"
    # The event search is a POST that only reads, so all methods retry.
    retry = Retry(
        5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=None,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    def api(method: str, path: str = "", **kwargs) -> requests.Response:
        url = f"https://materialize.antithesis.com/api/v0/runs/{run}{path}"
        response = session.request(method, url, timeout=60, **kwargs)
        response.raise_for_status()
        return response

    def search(prop: dict) -> list[dict]:
        if not (prop.get("event_set") and prop.get("passing_predicate")):
            return []
        query = f"{prop['event_set']}.filter(ev => !({prop['passing_predicate']})(ev))"
        try:
            # 999 is the server's maximum limit.
            body = {"query": f"{query}.with_last({{panic: {PANICS}}})", "limit": 999}
            lines = api("POST", "/events/search", json=body).text.splitlines()
        except requests.RequestException as e:
            print(f"Searching for {prop['name']} failed: {e}")
            return []
        rows = [json.loads(line) for line in lines if line]
        # A search that fails midway ends its stream with an error row.
        if errors := [row for row in rows if "moment" not in row]:
            print(f"Searching for {prop['name']} failed: {errors}")
        return [row for row in rows if "moment" in row]

    print(f"--- Waiting for run {run}", flush=True)

    def pending() -> int:
        params = {"status": "Pending", "limit": 1}
        return api("GET", "/properties", params=params).json()["total_count"]

    # A finished run can still be processing its properties' results.
    while api("GET").json()["status"] in ("starting", "in_progress") or pending():
        time.sleep(60)
    info = api("GET").json()

    print(f"--- Failures in run {run}")
    report = (info.get("links") or {}).get("triage_report")
    failures = 0

    # Prints each failure at once, so a later API error loses none.
    def fail(failure: str) -> None:
        nonlocal failures
        failures += 1
        print(
            f"---------- ANTITHESIS FAILURE START ----------\n{failure}\nreport: {report}\n"
            "---------- ANTITHESIS FAILURE END ----------",
            flush=True,
        )

    completed = info["status"] == "completed"
    if not completed:
        fail(f"run {run} ended {info['status']}")
    properties = api("GET", "/properties", params={"status": "Failing", "limit": 1000})
    for prop in properties.json()["data"]:
        name, sample = prop["name"], prop.get("counterexamples") or []
        # A measured property's sample can hold plain values.
        sample = [c if isinstance(c, dict) else {"value": c} for c in sample]
        # The API allows a missing count next to a sample.
        count = prop.get("counterexample_count") or len(sample)
        # An interrupted run also fails every property it never evaluated.
        if not count and not completed:
            continue
        # A `sometimes` fails by never holding, its false evaluations show
        # nothing. The search misses some properties, like container exits,
        # which are journald events, but each carries a sample.
        events = []
        if count and prop.get("group") != "Sometimes assertions":
            events = search(prop) or sample
        # One block per error, so a known issue matches only its own. Numbers
        # vary between occurrences of one error, source locations tell errors
        # apart.
        groups = defaultdict(list)
        for event in events:
            key = re.sub(
                r"(\S+\.rs(?::\d+)+)|\d+", lambda m: m[1] or "N", describe(event)
            )
            groups[key[:150]].append(event)
        for key, group in sorted(groups.items(), key=lambda g: -len(g[1])):
            # Occurrences that differ show only the key they share, so a known
            # issue cannot match the numbers or tail of one but not another.
            texts = {describe(event) for event in group}
            text = texts.pop() if len(texts) == 1 else key
            moment = group[0].get("moment")
            at = f"\nmoment: {moment['input_hash']} {moment['vtime']}" if moment else ""
            fail(f"{name}\ncounterexamples: {len(group)} of {count}\n{text}{at}")
        if not events:
            fail(f"{name}\ncounterexamples: {count}")
        # Its own block, so a known issue in the shown ones cannot cover these.
        elif len(events) < count:
            fail(
                f"{name}\ncounterexamples: {count - len(events)} of {count} not examined"
            )

    if failures:
        print(f"{failures} failures in Antithesis run {run}")
        # NOTE: An exception would also become a JUnit error, which no known
        # issue matches, so ci-ignore-failure could never apply. SystemExit
        # passes Composition.test_case, which records only an Exception.
        sys.exit(1)
