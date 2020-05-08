#!/usr/bin/env python3
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import argparse
import base64
import json
import os
import pathlib
import shlex
import signal
import subprocess
import time
from urllib.request import urlopen, Request
from urllib.error import URLError
from typing import Optional, Any


def main() -> None:
    args = parse_args()
    mz_url = args.materialized_url
    for fname in args.materialize_files:
        with open(fname) as fh:
            contents = fh.read()
        contents = contents.replace("MATERIALIZED_URL", args.materialized_url)
        with open(fname, "w") as fh:
            fh.write(contents)
            if os.getenv("MZ_LOG", "info") in ["debug"]:
                say(f"New file {fname}:\n{contents}")

    pipes = args.create_pipes.split(",")
    for p_fname in pipes:
        _, fname = p_fname.split("=", 1)
        f = pathlib.Path(fname)
        if f.exists():
            if not f.is_fifo():
                say(
                    f"WARNING: {f} already exists but is not a FIFO, trying to delete it"
                )
                f.unlink()
        if not f.exists():
            os.mkfifo(f)
            say(f"created fifo {f}")

    # Note that unfortunately we can't create use log-prefixer as a supervisord-managed
    # process because supervisord tries to open files for writing I suppose a bit earlier
    # than it tries to start the highest-priority program, and pipes block their host
    # when you try to write to them if there is no reader.
    #
    # So we need to spawn off a thread before supervisord has a chance to get itself
    # messed up. Possibly this is related to
    # https://github.com/Supervisor/supervisor/issues/122
    subprocess.Popen(["/bin/log-prefixer.py"] + pipes)

    cmd = shlex.split(args.exec)
    proc = subprocess.Popen(cmd)
    try:
        init_home_dashboard(args.home_dashboard)
        proc.wait()
    except KeyboardInterrupt:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            proc.wait()


def init_home_dashboard(dash_uid: str) -> None:
    while True:
        time.sleep(5)
        try:
            with request(f"/api/dashboards/uid/{dash_uid}") as res:
                dash_data = json.load(res)
            dash_id = dash_data["dashboard"]["id"]

            data = ("""{"homeDashboardId": %s}""" % dash_id).encode("utf-8")
            res = request("/api/org/preferences", data=data, method="PUT")

            with request("/api/org/preferences") as res:
                res_j = json.load(res)
                if res_j["homeDashboardId"] != dash_id:
                    say(f"home dashboard id not updated to {dash_id}: {res_j}")
                else:
                    say(f"successfully updated home dashboard to {dash_uid}")
                    break
        except URLError as e:
            say(f"unable to set home dashboard to {dash_uid} {e}")


def request(
    path: str, data: Optional[str] = None, method: str = "GET", headers=None, anon=False
) -> Any:
    req = Request(f"http://localhost:3000{path}", data=data, method=method)
    if not anon:
        auth = base64.b64encode(b"admin:admin").replace(b"\n", b"").decode("utf-8")
        req.add_header("Authorization", f"Basic {auth}")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Grafana-Org-Id", "1")
    if headers is not None:
        for header, val in headers.items():
            req.add_header(header, val)
    return urlopen(req)


def say(msg: str, *args: str) -> None:
    if os.path.exists("/dev/stdout"):
        with open("/dev/stdout", "w") as fh:
            print("startup    >", msg.format(args), file=fh)
    else:
        print("startup    >", msg.format(args))


def parse_args() -> argparse.Namespace:
    args = argparse.ArgumentParser()
    args.add_argument(
        "-u",
        "--materialized-url",
        default=os.getenv("MATERIALIZED_URL", "materialized:6875"),
    )
    args.add_argument(
        "--exec",
        help="What to execute afterwards",
        default="/bin/supervisord -c /supervisord/supervisord.conf",
    )
    args.add_argument(
        "--create-pipes", help="comma-separated list of files to create", default=""
    )
    args.add_argument(
        "--home-dashboard",
        help="Set the grafana home dashboard",
        default="materialize-overview",
    )
    args.add_argument(
        "materialize_files",
        metavar="MATERIALIZE-FILES",
        nargs="+",
        help="Which files to replace MATERIALIZED_URL in",
    )

    return args.parse_args()


if __name__ == "__main__":
    main()
