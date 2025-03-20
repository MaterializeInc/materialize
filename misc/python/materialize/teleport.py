# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import subprocess
import threading
import time
from textwrap import dedent

import psutil

from materialize import build_config, ui


class TeleportProxy:
    @classmethod
    def spawn(cls, app_name: str, port: str):
        """Spawn a Teleport proxy for the provided app_name."""

        teleport_state = build_config.TeleportLocalState.read()

        # If there is already a Teleport proxy running, no need to restart one.
        running_pid = TeleportProxy.check(app_name)
        if running_pid:
            ui.say(f"Teleport proxy already running, PID: {running_pid}")
            return
        else:
            # If the existing PID doesn't exist, clear it from state.
            teleport_state.set_pid(app_name, None)
            teleport_state.set_address(app_name, None)
            teleport_state.write()

        # Otherwise spawn a Teleport proxy.
        cmd_args = ["tsh", "proxy", "app", f"{app_name}", "--port", port]
        child = subprocess.Popen(
            cmd_args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setpgrp,
        )
        ui.say(f"starting Teleport proxy for '{app_name}'...")

        def wait(child, teleport_state, address):
            wait_start = time.time()

            while time.time() - wait_start < 2:
                child_terminated = child.poll()
                if child_terminated:
                    other_tshs = [
                        p.pid
                        for p in psutil.process_iter(["pid", "name"])
                        if p.name() == "tsh"
                    ]
                    ui.warn(
                        dedent(
                            f"""
                    Teleport proxy failed to start, 'tsh' process already running!
                        existing 'tsh' processes: {other_tshs}
                        exit code: {child_terminated}
                    """
                        )
                    )
                    break

            # Timed out! Check if the process is running.
            child_pid_status = psutil.pid_exists(child.pid)
            if child_pid_status:
                # Record the PID, if the process started successfully.
                teleport_state.set_pid(app_name, child.pid)
                teleport_state.set_address(app_name, address)
                teleport_state.write()

        # Spawn a thread that will wait for the Teleport proxy to start, and
        # record it's PID, or warn that it failed to start.
        address = f"http://localhost:{port}"
        thread = threading.Thread(target=wait, args=[child, teleport_state, address])
        thread.start()

    @classmethod
    def check(cls, app_name: str) -> str | None:
        """Check if a Teleport proxy is already running for the specified app_name."""

        teleport_state = build_config.TeleportLocalState.read()
        existing_pid = teleport_state.get_pid(app_name)

        if existing_pid and psutil.pid_exists(int(existing_pid)):
            return teleport_state.get_pid(app_name)
        else:
            return None
