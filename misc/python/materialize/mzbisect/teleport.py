# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Teleport integration: elevated login and database tunnels via tsh."""

import contextlib
import socket
import subprocess
import time
from collections.abc import Generator

TELEPORT_PROXY = "materialize.teleport.sh"


def login(request_id: str) -> None:
    """Run `tsh login` with an approved access request.

    Inherits stdio so tsh can drive whatever interaction it needs (browser
    SSO, approval wait). Raises on a non-zero exit.
    """
    cmd = [
        "tsh",
        "login",
        f"--proxy={TELEPORT_PROXY}",
        f"--request-id={request_id}",
    ]
    print("$ " + " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        raise RuntimeError(
            "tsh not found, install Teleport before using --teleport-request-id"
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"tsh login failed with exit code {e.returncode}")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
        assert isinstance(port, int)
        return port


@contextlib.contextmanager
def tunnel(
    env: str,
    db_user: str,
    db_name: str,
    timeout_secs: int = 60,
) -> Generator[tuple[str, int], None, None]:
    """Yield (host, port) of an authenticated local tunnel to env's database.

    Spawns `tsh proxy db --tunnel`, which terminates TLS and client
    authentication in the proxy, so a plain pgwire client can connect to the
    local port. The db user is pinned by the tunnel's certificate, so
    connections through it must use db_user. The proxy is torn down when the
    context exits.
    """
    port = _free_port()
    cmd = [
        "tsh",
        "proxy",
        "db",
        "--tunnel",
        f"--db-user={db_user}",
        f"--db-name={db_name}",
        f"--port={port}",
        env,
    ]
    print("$ " + " ".join(cmd))
    try:
        # tsh writes a few lines at startup and then stays quiet, so a pipe
        # we only drain on failure cannot fill up and stall it.
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "tsh not found, install Teleport before using --teleport-env"
        )
    try:
        deadline = time.monotonic() + timeout_secs
        while True:
            if proc.poll() is not None:
                stderr = proc.stderr.read().strip() if proc.stderr else ""
                raise RuntimeError(
                    f"tsh proxy db exited with code {proc.returncode}"
                    + (f": {stderr}" if stderr else "")
                )
            try:
                with socket.create_connection(("localhost", port), timeout=1):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"timed out after {timeout_secs}s waiting for the"
                        f" teleport tunnel on port {port}"
                    )
                time.sleep(0.5)
        print(f"teleport tunnel to {env} ready on localhost:{port}")
        yield ("localhost", port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
