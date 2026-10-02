# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# balancerd_lb_conformance.py: check that the load balancer in front of
# balancerd keeps established connections when a pod leaves rotation.

"""Load balancer conformance check for balancerd connection watermarks.

Runs against a deployed Materialize instance whose balancerd pods sit behind a
load balancer and use a dynamic config ConfigMap (`spec.balancerdConfigmapName`):

    bin/pyactivate -m materialize.cli.balancerd_lb_conformance \\
        --host <load-balancer-hostname> \\
        --namespace materialize-environment --configmap mz-balancerd-config

The check holds pgwire and HTTPS connections on one balancerd pod, sets the
connection watermarks in the ConfigMap low enough that only that pod leaves
rotation, and after the load balancer has had time to react asserts that the
held connections still work, that new connections land on another pod, and that
the pod receives new connections again once the watermarks are cleared. The
ConfigMap is restored on exit.

Nothing in a proxied session names the balancerd pod that serves it, so the
check tells pods apart by reading each pod's `mz_balancer_connection_active`
gauges through the apiserver pod proxy and watching which one moves. That, and
the fact that every pod reads the same ConfigMap, requires the instance to be
otherwise idle.
"""

import argparse
import base64
import http.client
import json
import ssl
import subprocess
import sys
import time
from collections.abc import Callable
from typing import Any

import psycopg

HIGH_WATERMARK = "balancerd_connection_high_watermark"
LOW_WATERMARK = "balancerd_connection_low_watermark"
# Held pgwire connections on the target pod. The high watermark is set to the
# held count plus the HTTPS connection, so a stray connection to another pod
# during the check leaves that pod well below the watermark.
HELD_PGWIRE_CONNECTIONS = 3
LANDING_ATTEMPTS = 40


class CheckFailed(Exception):
    pass


class Kubectl:
    def __init__(self, namespace: str, context: str | None):
        self.namespace = namespace
        self.base = ["kubectl"] + (["--context", context] if context else [])

    def run(self, *args: str) -> str:
        cmd = [*self.base, *args]
        try:
            return subprocess.run(
                cmd, check=True, capture_output=True, text=True
            ).stdout
        except subprocess.CalledProcessError as e:
            raise CheckFailed(f"{' '.join(cmd)} failed: {e.stderr.strip()}")

    def get(self, *args: str) -> Any:
        return json.loads(self.run("get", "-n", self.namespace, "-o", "json", *args))

    def balancerd_pods(self) -> dict[str, int]:
        """Map each balancerd pod name to its internal HTTP port."""
        pods = {}
        for pod in self.get("pods", "-l", "app=balancerd")["items"]:
            [port] = [
                p["containerPort"]
                for p in pod["spec"]["containers"][0]["ports"]
                if p["name"] == "internal-http"
            ]
            pods[pod["metadata"]["name"]] = port
        return pods

    def pod_ready(self, name: str) -> bool:
        pod = self.get("pod", name)
        return any(
            c["type"] == "Ready" and c["status"] == "True"
            for c in pod["status"].get("conditions", [])
        )

    def metrics(self, pod: str, port: int) -> dict[str, int]:
        """The pod's integer Prometheus metrics, keyed by name with labels."""
        path = f"/api/v1/namespaces/{self.namespace}/pods/{pod}:{port}/proxy/metrics"
        metrics = {}
        for line in self.run("get", "--raw", path).splitlines():
            if line.startswith("mz_balancer_"):
                name, value = line.rsplit(" ", 1)
                metrics[name] = int(value)
        return metrics

    def read_config(self, configmap: str) -> dict[str, Any]:
        return json.loads(self.get("configmap", configmap)["data"]["config.json"])

    def write_config(self, configmap: str, config: dict[str, Any]) -> None:
        patch = {"data": {"config.json": json.dumps(config)}}
        self.run(
            "patch",
            "-n",
            self.namespace,
            "configmap",
            configmap,
            "--type",
            "merge",
            "-p",
            json.dumps(patch),
        )


class Client:
    def __init__(self, args: argparse.Namespace):
        self.args = args

    def pgwire(self) -> psycopg.Connection[Any]:
        return psycopg.connect(
            host=self.args.host,
            port=self.args.pgwire_port,
            user=self.args.user,
            password=self.args.password,
            dbname="materialize",
            sslmode="require" if self.args.tls else "disable",
            connect_timeout=10,
            autocommit=True,
            application_name="balancerd_lb_conformance",
        )

    def https(self) -> http.client.HTTPConnection:
        if self.args.tls:
            conn = http.client.HTTPSConnection(
                self.args.host,
                self.args.https_port,
                context=ssl._create_unverified_context(),
                timeout=30,
            )
        else:
            conn = http.client.HTTPConnection(
                self.args.host, self.args.https_port, timeout=30
            )
        conn.connect()
        # A request on a closed connection must fail instead of silently
        # opening a new socket, or a dropped held connection goes unnoticed.
        conn.auto_open = 0
        return conn

    def https_query(self, conn: http.client.HTTPConnection) -> None:
        creds = f"{self.args.user}:{self.args.password or ''}".encode()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {base64.b64encode(creds).decode()}",
        }
        conn.request("POST", "/api/sql", json.dumps({"query": "SELECT 1"}), headers)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise CheckFailed(f"/api/sql returned {resp.status}: {body.decode()}")


class Balancers:
    """Attributes connections to balancerd pods through their connection gauges."""

    def __init__(self, kubectl: Kubectl, pods: dict[str, int]):
        self.kubectl = kubectl
        self.pods = pods

    def connections(self) -> dict[str, int]:
        return {
            pod: sum(
                value
                for name, value in self.kubectl.metrics(pod, port).items()
                if name.startswith("mz_balancer_connection_active{")
            )
            for pod, port in self.pods.items()
        }

    def limit(self, pod: str) -> int:
        return self.kubectl.metrics(pod, self.pods[pod])["mz_balancer_connection_limit"]

    def wait_for_total(self, total: int) -> dict[str, int]:
        deadline = time.monotonic() + 30
        while True:
            counts = self.connections()
            if sum(counts.values()) == total:
                return counts
            if time.monotonic() > deadline:
                raise CheckFailed(
                    f"expected {total} balancerd connections in total, found {counts}; "
                    "the instance must be idle"
                )
            time.sleep(1)

    def open(self, open_conn: Callable[[], Any]) -> tuple[Any, str]:
        """Open a connection and return it with the pod that serves it."""
        before = self.connections()
        conn = open_conn()
        after = self.wait_for_total(sum(before.values()) + 1)
        [pod] = [pod for pod in after if after[pod] > before[pod]]
        return conn, pod

    def close(self, conn: Any) -> None:
        total = sum(self.connections().values()) - 1
        conn.close()
        self.wait_for_total(total)

    def hold_on(
        self, target: str, open_conn: Callable[[], Any], count: int
    ) -> list[Any]:
        """Open connections until `count` of them are served by `target`."""
        held = []
        for _ in range(LANDING_ATTEMPTS):
            conn, pod = self.open(open_conn)
            if pod == target:
                held.append(conn)
                if len(held) == count:
                    return held
            else:
                self.close(conn)
        raise CheckFailed(
            f"could not land {count} connections on {target} in {LANDING_ATTEMPTS} attempts"
        )


def wait_for(what: str, predicate: Callable[[], bool], timeout: float) -> None:
    print(f"waiting for {what}")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(2)
    raise CheckFailed(f"timed out after {timeout}s waiting for {what}")


def check(args: argparse.Namespace) -> None:
    kubectl = Kubectl(args.namespace, args.context)
    client = Client(args)
    pods = kubectl.balancerd_pods()
    if len(pods) < 2:
        raise CheckFailed(f"need at least 2 balancerd pods, found {sorted(pods)}")
    balancers = Balancers(kubectl, pods)
    original = kubectl.read_config(args.configmap)
    # Removing a key from config.json does not reset its running value, so
    # restoring the original config must set the watermarks explicitly.
    restored = {
        **original,
        HIGH_WATERMARK: original.get(HIGH_WATERMARK),
        LOW_WATERMARK: original.get(LOW_WATERMARK),
    }
    high = HELD_PGWIRE_CONNECTIONS + 1
    for pod in pods:
        if not 0 < high < balancers.limit(pod):
            raise CheckFailed(
                f"{pod} has balancerd_max_connections={balancers.limit(pod)}, "
                f"which must be above {high} for the watermarks to apply"
            )

    first, target = balancers.open(client.pgwire)
    print(f"target pod {target}")
    held_pgwire = [first] + balancers.hold_on(
        target, client.pgwire, HELD_PGWIRE_CONNECTIONS - 1
    )
    [held_https] = balancers.hold_on(target, client.https, 1)
    others = [pod for pod in pods if pod != target]

    try:
        print(f"setting {HIGH_WATERMARK}={high} {LOW_WATERMARK}=1")
        kubectl.write_config(
            args.configmap, {**original, HIGH_WATERMARK: high, LOW_WATERMARK: 1}
        )
        wait_for(
            f"{target} to become not ready",
            lambda: not kubectl.pod_ready(target),
            args.timeout_seconds,
        )
        if not any(kubectl.pod_ready(pod) for pod in others):
            raise CheckFailed(f"no other balancerd pod is ready: {others}")

        print(f"waiting {args.settle_seconds}s for the load balancer to act")
        time.sleep(args.settle_seconds)

        try:
            for conn in held_pgwire:
                conn.execute("SELECT 1")
            client.https_query(held_https)
        except (psycopg.Error, http.client.HTTPException, OSError) as e:
            raise CheckFailed(f"held connection to {target} broke: {e}")
        print(f"held connections to {target} still work")

        for _ in range(args.new_connections):
            for open_conn in (client.pgwire, client.https):
                conn, pod = balancers.open(open_conn)
                if pod == target:
                    raise CheckFailed(f"new connection reached not-ready pod {target}")
                if isinstance(conn, http.client.HTTPConnection):
                    client.https_query(conn)
                balancers.close(conn)
        print(
            f"{args.new_connections} new pgwire and HTTPS connections avoided {target}"
        )

        print("clearing the watermarks")
        kubectl.write_config(args.configmap, restored)
        wait_for(
            f"{target} to become ready",
            lambda: kubectl.pod_ready(target),
            args.timeout_seconds,
        )

        def new_connection_reaches_target() -> bool:
            conn, pod = balancers.open(client.pgwire)
            balancers.close(conn)
            return pod == target

        wait_for(
            f"a new connection to reach {target}",
            new_connection_reaches_target,
            args.timeout_seconds,
        )
    finally:
        kubectl.write_config(args.configmap, restored)
        for conn in held_pgwire:
            conn.close()
        held_https.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="balancerd_lb_conformance",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", required=True, help="load balancer hostname")
    parser.add_argument("--pgwire-port", type=int, default=6875)
    parser.add_argument("--https-port", type=int, default=6876)
    parser.add_argument("--user", default="materialize")
    parser.add_argument("--password")
    parser.add_argument(
        "--tls",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="connect with TLS, without verifying the certificate",
    )
    parser.add_argument(
        "--namespace", required=True, help="Materialize instance namespace"
    )
    parser.add_argument(
        "--configmap",
        required=True,
        help="ConfigMap named by spec.balancerdConfigmapName",
    )
    parser.add_argument("--context", help="kubectl context")
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=90,
        help="time for the load balancer to stop routing to a not-ready pod: its "
        "unhealthy threshold times its health check interval, plus propagation",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=180,
        help="deadline for a ConfigMap change to flip a pod's readiness, and for "
        "the load balancer to route to a pod again once it is ready",
    )
    parser.add_argument("--new-connections", type=int, default=10)
    args = parser.parse_args()
    try:
        check(args)
    except CheckFailed as e:
        print(f"FAILED: {e}", file=sys.stderr)
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
