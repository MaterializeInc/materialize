# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# pyright: reportMissingImports=false
import socket
import subprocess
import urllib.parse
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

from materialize.cloudtest.util.common import eprint, retry
from materialize.cloudtest.util.docker import docker_env


@dataclass
class Endpoint:
    scheme: str
    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def host_port(self) -> Tuple[str, int]:
        return (self.host, self.port)

    @classmethod
    def parse(cls, s: str) -> "Endpoint":
        u = parse_url(s)
        assert u.hostname is not None and u.port is not None
        return cls(scheme=u.scheme or "http", host=u.hostname, port=u.port)


def wait_for_connectable(address: Union[Tuple[Any, int], str], max_attempts: int = 30):
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(address)

    retry(
        f,
        max_attempts=max_attempts,
        exception_types=[ConnectionRefusedError, socket.gaierror],
        message=f"Error connecting to {address}. Tried {max_attempts} times.",
    )


def parse_url(s: str) -> urllib.parse.ParseResult:
    """
    >>> parse_url('127.0.0.1:8002').port
    8002
    >>> parse_url('127.0.0.1:8002').hostname
    '127.0.0.1'
    >>> parse_url('the men who stare at goats')
    Traceback (most recent call last):
      File "/nix/store/7awm88zrzq5c0qks8ypf8s8jblm4r3i2-python3-3.9.16/lib/python3.9/doctest.py", line 1334, in __run
        exec(compile(example.source, filename, "single",
      File "<doctest __main__.parse_url[2]>", line 1, in <module>
        parse_url('the men who stare at goats')
      File "/Users/rami/Code/cloud/k8s_tests/util.py", line 343, in parse_url
        raise ValueError(s)
    ValueError: //the men who stare at goats
    """
    try:
        parsed = urllib.parse.urlparse(s)
        assert parsed.netloc is not None and parsed.port is not None
    except AssertionError:
        try:
            s = "//" + s
            parsed = urllib.parse.urlparse(s)
            assert parsed.netloc is not None and parsed.port is not None
        except AssertionError as e:
            raise ValueError(s) from e
    return parsed


def launch_controllers(controller_names: List[str]):
    try:
        subprocess.run(
            [
                "bin/compose",
                "up",
                "--wait",
                *controller_names,
            ],
            capture_output=True,
            check=True,
            env=docker_env(),
        )
    except subprocess.CalledProcessError as e:
        eprint(e.returncode, e.stdout, e.stderr)
        raise


def wait_for_controllers(*endpoints: Optional[Endpoint]):
    for endpoint in endpoints:
        if endpoint:
            eprint(f"Waiting for {endpoint.host_port} to be connectable...")
            wait_for_connectable(endpoint.host_port)


def cleanup_controllers():
    try:
        subprocess.run(
            ["bin/compose", "down", "-v"],
            capture_output=True,
            check=True,
            env=docker_env(),
        )
    except subprocess.CalledProcessError as e:
        eprint(e.returncode, e.stdout, e.stderr)
        raise
