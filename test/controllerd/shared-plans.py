#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

"""Check bidirectional post-admission plan reuse between prototype versions.

Run with bin/pyactivate against two controllers in one fresh native environment.
Both versions must understand the same complete committed-plan encoding. This is not release compatibility
coverage. No processes are started or restarted by this test.
"""

import argparse
import json
import time
import urllib.request
import uuid

import psycopg
from psycopg import sql


def get(controller: str, path: str):
    with urllib.request.urlopen(controller + path, timeout=15) as response:
        return json.load(response)


def connect(controller: str, adapter_id: str) -> psycopg.Connection:
    adapter = next(a for a in get(controller, "/api/adapters") if a["id"] == adapter_id)
    assert adapter["readiness"]["ready"], adapter
    host, port = adapter["endpoints"]["sql-external"][0].rsplit(":", 1)
    return psycopg.connect(
        host=host,
        port=int(port),
        user="materialize",
        dbname="materialize",
        autocommit=True,
        connect_timeout=10,
        options="-c statement_timeout=60000",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-controller", required=True)
    parser.add_argument("--new-controller", required=True)
    parser.add_argument("--old-adapter", default="a1")
    parser.add_argument("--new-adapter", default="a1")
    args = parser.parse_args()
    controllers = (args.old_controller, args.new_controller)
    inventories = [get(c, "/api/inventory") for c in controllers]
    assert inventories[0]["environment_id"] == inventories[1]["environment_id"]
    assert inventories[0]["generation"] != inventories[1]["generation"]

    with (
        connect(args.old_controller, args.old_adapter) as old,
        connect(args.new_controller, args.new_adapter) as new,
    ):
        clients = (old, new)
        versions = [c.execute("SELECT mz_version()").fetchone()[0] for c in clients]
        assert versions[0] != versions[1], versions
        for client in clients:
            client.execute("SET cluster = quickstart")
        observations = []
        # Both SQL sessions precede all test DDL and survive both directions.
        for origin, creator in enumerate(clients):
            name = f"shared_plan_{uuid.uuid4().hex}"
            table = sql.Identifier(name)
            index_name = name + "_idx"
            index = sql.Identifier(index_name)
            creator.execute(sql.SQL("CREATE TABLE {} (n INT)").format(table))
            try:
                creator.execute(sql.SQL("INSERT INTO {} VALUES (1), (2)").format(table))
                creator.execute(
                    sql.SQL("CREATE INDEX {} ON {} (n)").format(index, table)
                )
                for observer, client in enumerate(clients):
                    started = time.monotonic()
                    plan = client.execute(
                        sql.SQL("EXPLAIN PHYSICAL PLAN FOR SELECT * FROM {}").format(
                            table
                        )
                    ).fetchone()[0]
                    assert index_name in plan, plan
                    assert sorted(
                        client.execute(
                            sql.SQL("SELECT * FROM {}").format(table)
                        ).fetchall()
                    ) == [(1,), (2,)]
                    observations.append(
                        {
                            "origin": origin,
                            "observer": observer,
                            "plan": plan,
                            "seconds": time.monotonic() - started,
                        }
                    )
                peer = clients[1 - origin]
                peer.execute(sql.SQL("INSERT INTO {} VALUES (3)").format(table))
                for client in clients:
                    assert sorted(
                        client.execute(
                            sql.SQL("SELECT * FROM {}").format(table)
                        ).fetchall()
                    ) == [(1,), (2,), (3,)]
            finally:
                creator.execute(sql.SQL("DROP TABLE {} CASCADE").format(table))
            for client in clients:
                assert client.execute("SELECT 1").fetchone() == (1,)
        print(
            json.dumps(
                {
                    "versions": versions,
                    "sessions_survived": True,
                    "observations": observations,
                }
            )
        )


if __name__ == "__main__":
    main()
