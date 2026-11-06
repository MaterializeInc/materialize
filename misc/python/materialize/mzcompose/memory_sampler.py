# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Background sampling of a container's cgroup memory usage.

The cgroup's own high-water mark, `memory.peak`, cannot be reset from inside an
unprivileged container: `/sys/fs/cgroup` is mounted read-only there, and even a
privileged container gets `EPERM` writing to its own cgroup. Sampling
`memory.current` is the portable way to get a per-window peak.
"""

import subprocess
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition

# Prints the container's cgroup memory usage in bytes every `$1` seconds. `read`
# is a shell builtin, so each sample forks only `sleep`.
_SAMPLE_SCRIPT = """
f=/sys/fs/cgroup/memory.current
[ -r "$f" ] || f=/sys/fs/cgroup/memory/memory.usage_in_bytes
while :; do
    read -r v < "$f" || exit 1
    echo "$v" || exit 0
    sleep "$1"
done
"""


class MemorySampler:
    """Samples a service container's cgroup memory usage in the background and
    reports the maximum within a window.

    Spikes shorter than `interval` seconds can fall between samples. The
    sampler follows container restarts: `start_window` re-attaches when the
    service's container changed or the sampling process exited.
    """

    def __init__(
        self, composition: "Composition", service: str, interval: float = 0.05
    ) -> None:
        self._composition = composition
        self._service = service
        self._interval = interval
        self._lock = threading.Lock()
        self._max: int | None = None
        self._container_id: str | None = None
        self._process: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None

    def start_window(self) -> None:
        """Begin a new window, discarding the maximum of the previous one."""
        container_id = self._composition.container_id(self._service)
        assert container_id is not None, f"service {self._service} is not running"
        if (
            container_id != self._container_id
            or self._process is None
            or self._process.poll() is not None
        ):
            self.stop()
            self._attach(container_id)
        with self._lock:
            self._max = None

    def window_peak(self, current: int) -> int:
        """Return the maximum memory usage in bytes since `start_window`,
        including `current`, a reading the caller took at the window's end."""
        with self._lock:
            return current if self._max is None else max(self._max, current)

    def stop(self) -> None:
        """Stop sampling. Safe to call repeatedly."""
        if self._process is not None:
            self._process.kill()
            self._process.wait()
        if self._reader is not None:
            self._reader.join()
        self._process = None
        self._reader = None
        self._container_id = None

    def _attach(self, container_id: str) -> None:
        self._process = subprocess.Popen(
            [
                "docker",
                "exec",
                container_id,
                "sh",
                "-c",
                _SAMPLE_SCRIPT,
                "sh",
                str(self._interval),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        self._container_id = container_id
        self._reader = threading.Thread(
            target=self._read, args=(self._process,), daemon=True
        )
        self._reader.start()

    def _read(self, process: "subprocess.Popen[str]") -> None:
        assert process.stdout is not None
        for line in process.stdout:
            try:
                value = int(line)
            except ValueError:
                continue
            with self._lock:
                if self._max is None or value > self._max:
                    self._max = value
