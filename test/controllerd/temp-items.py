#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

"""Check temporary-table isolation between two running, managed adapters.

Run with bin/pyactivate. Both controllers must belong to the same environment.
For same-deployment coverage, select different adapter IDs on one controller.
"""

import argparse
import json
import urllib.request
import uuid

import psycopg
from psycopg import sql


def connect(controller: str, adapter_id: str) -> psycopg.Connection:
    with urllib.request.urlopen(f"{controller}/api/adapters", timeout=10) as response:
        adapter = next(a for a in json.load(response) if a["id"] == adapter_id)
    assert adapter["readiness"]["ready"], adapter["readiness"]
    host, port = adapter["endpoints"]["sql-external"][0].rsplit(":", 1)
    return psycopg.connect(
        host=host,
        port=int(port),
        user="materialize",
        dbname="materialize",
        connect_timeout=10,
        autocommit=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-controller", required=True)
    parser.add_argument("--peer-controller", required=True)
    parser.add_argument("--owner-adapter", default="a1")
    parser.add_argument("--peer-adapter", default="a1")
    args = parser.parse_args()
    inventories = []
    for controller in (args.owner_controller, args.peer_controller):
        with urllib.request.urlopen(
            f"{controller}/api/inventory", timeout=10
        ) as response:
            inventories.append(json.load(response))
    assert inventories[0]["environment_id"] == inventories[1]["environment_id"]
    assert (inventories[0]["generation"], args.owner_adapter) != (
        inventories[1]["generation"],
        args.peer_adapter,
    ), "select two distinct managed adapters"
    with (
        connect(args.owner_controller, args.owner_adapter) as owner,
        connect(args.peer_controller, args.peer_adapter) as peer,
    ):
        for client in (owner, peer):
            client.execute("SET statement_timeout = '30s'")
        versions = [
            client.execute("SELECT mz_version()").fetchone()[0]
            for client in (owner, peer)
        ]
        # Reuse the same peer connection throughout. A restarted process cannot
        # satisfy this check by accepting a replacement SQL session.
        for creator, observer in ((owner, peer), (peer, owner)):
            table = sql.Identifier(f"temp_peer_{uuid.uuid4().hex}")
            creator.execute(sql.SQL("CREATE TEMP TABLE {} (n INT)").format(table))
            creator.execute(sql.SQL("INSERT INTO {} VALUES (7)").format(table))
            assert observer.execute("SELECT 1").fetchone() == (1,)
            assert creator.execute(
                sql.SQL("SELECT * FROM {}").format(table)
            ).fetchall() == [(7,)]
            creator.execute(sql.SQL("DROP TABLE {}").format(table))
            assert observer.execute("SELECT 1").fetchone() == (1,)
        print(json.dumps({"versions": versions, "sessions_survived": True}))


if __name__ == "__main__":
    main()
