# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzcompose system for Docker compositions.

For an overview of what mzcompose is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""
import argparse
import copy
import importlib
import importlib.abc
import importlib.util
import inspect
import json
import os
import re
import subprocess
import sys
import time
import traceback
from collections import OrderedDict
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from inspect import Traceback, getframeinfo, getmembers, isfunction, stack
from tempfile import TemporaryFile
from typing import (
    Any,
    cast,
)

import pg8000
import sqlparse
import yaml
from pg8000 import Connection, Cursor

from materialize import mzbuild, spawn, ui
from materialize.mzcompose import loader
from materialize.mzcompose.service import Service
from materialize.ui import UIError


class UnknownCompositionError(UIError):
    """The specified composition was unknown."""

    def __init__(self, name: str):
        super().__init__(f"unknown composition {name!r}")


class WorkflowArgumentParser(argparse.ArgumentParser):
    """An argument parser provided to a workflow in a `Composition`.

    You can call `add_argument` and other methods on this argument parser like
    usual. When you are ready to parse arguments, call `parse_args` or
    `parse_known_args` like usual; the argument parser will automatically use
    the arguments that the user provided to the workflow.
    """

    def __init__(self, name: str, description: str | None, args: list[str]):
        self.args = args
        super().__init__(prog=f"mzcompose run {name}", description=description)

    def parse_known_args(
        self,
        args: Sequence[str] | None = None,
        namespace: argparse.Namespace | None = None,
    ) -> tuple[argparse.Namespace, list[str]]:
        if args is None:
            args = self.args
        return super().parse_known_args(args, namespace)


class Composition:
    """A loaded mzcompose.py file."""

    @dataclass
    class TestResult:
        duration: float
        error: str | None

    def __init__(
        self,
        repo: mzbuild.Repository,
        name: str,
        preserve_ports: bool = False,
        silent: bool = False,
        munge_services: bool = True,
        project_name: str | None = None,
    ):
        self.name = name
        self.description = None
        self.repo = repo
        self.preserve_ports = preserve_ports
        self.project_name = project_name
        self.silent = silent
        self.workflows: dict[str, Callable[..., None]] = {}
        self.test_results: OrderedDict[str, Composition.TestResult] = OrderedDict()

        if name in self.repo.compositions:
            self.path = self.repo.compositions[name]
        else:
            raise UnknownCompositionError(name)

        self.compose: dict[str, Any] = {
            "version": "3.7",
            "services": {},
        }

        # Load the mzcompose.py file, if one exists
        mzcompose_py = self.path / "mzcompose.py"
        if mzcompose_py.exists():
            spec = importlib.util.spec_from_file_location("mzcompose", mzcompose_py)
            assert spec
            module = importlib.util.module_from_spec(spec)
            assert isinstance(spec.loader, importlib.abc.Loader)
            loader.composition_path = self.path
            spec.loader.exec_module(module)
            loader.composition_path = None
            self.description = inspect.getdoc(module)
            for name, fn in getmembers(module, isfunction):
                if name.startswith("workflow_"):
                    # The name of the workflow is the name of the function
                    # with the "workflow_" prefix stripped and any underscores
                    # replaced with dashes.
                    name = name[len("workflow_") :].replace("_", "-")
                    self.workflows[name] = fn

            for python_service in getattr(module, "SERVICES", []):
                name = python_service.name
                if name in self.compose["services"]:
                    raise UIError(f"service {name!r} specified more than once")
                self.compose["services"][name] = python_service.config

        # Add default volumes
        self.compose.setdefault("volumes", {}).update(
            {
                "mzdata": None,
                "pgdata": None,
                # Used for certain pg-cdc scenarios. The memory will not be
                # allocated for compositions that do not require this volume.
                "pgdata_512Mb": {
                    "driver_opts": {
                        "device": "tmpfs",
                        "type": "tmpfs",
                        "o": "size=512m",
                    }
                },
                "mydata": None,
                "tmp": None,
                "secrets": None,
                "scratch": None,
            }
        )

        # The CLI driver will handle acquiring these dependencies.
        if munge_services:
            self.dependencies = self._munge_services(self.compose["services"].items())

        # Emit the munged configuration to a temporary file so that we can later
        # pass it to Docker Compose.
        self.file = TemporaryFile(mode="w")
        os.set_inheritable(self.file.fileno(), True)
        self._write_compose()

    def _munge_services(
        self, services: list[tuple[str, dict]]
    ) -> mzbuild.DependencySet:
        images = []

        for name, config in services:
            # Remember any mzbuild references.
            if "mzbuild" in config:
                image_name = config["mzbuild"]
                if image_name not in self.repo.images:
                    raise UIError(f"mzcompose: unknown image {image_name}")
                image = self.repo.images[image_name]
                images.append(image)

            if "propagate_uid_gid" in config:
                if config["propagate_uid_gid"]:
                    config["user"] = f"{os.getuid()}:{os.getgid()}"
                del config["propagate_uid_gid"]

            ports = config.setdefault("ports", [])
            for i, port in enumerate(ports):
                if self.preserve_ports and not ":" in str(port):
                    # If preserving ports, bind the container port to the same
                    # host port, assuming the host port is available.
                    ports[i] = f"{port}:{port}"
                elif ":" in str(port) and not config.get("allow_host_ports", False):
                    # Raise an error for host-bound ports, unless
                    # `allow_host_ports` is `True`
                    raise UIError(
                        "programming error: disallowed host port in service {name!r}",
                        hint='Add `"allow_host_ports": True` to the service config to disable this check.',
                    )

            if "allow_host_ports" in config:
                config.pop("allow_host_ports")

            if self.repo.rd.coverage:
                coverage_volume = "./coverage:/coverage"
                if coverage_volume not in config.get("volumes", []):
                    # Emit coverage information to a file in a directory that is
                    # bind-mounted to the "coverage" directory on the host. We
                    # inject the configuration to all services for simplicity, but
                    # this only have an effect if the service runs instrumented Rust
                    # binaries.
                    config.setdefault("volumes", []).append(coverage_volume)

                llvm_profile_file = (
                    f"LLVM_PROFILE_FILE=/coverage/{name}-%p-%9m%c.profraw"
                )
                for i, env in enumerate(config.get("environment", [])):
                    # Make sure we don't have duplicate environment entries.
                    if env.startswith("LLVM_PROFILE_FILE="):
                        config["environment"][i] = llvm_profile_file
                        break
                else:
                    config.setdefault("environment", []).append(llvm_profile_file)

        # Determine mzbuild specs and inject them into services accordingly.
        deps = self.repo.resolve_dependencies(images)
        for _name, config in services:
            if "mzbuild" in config:
                config["image"] = deps[config["mzbuild"]].spec()
                del config["mzbuild"]

        return deps

    def _write_compose(self) -> None:
        self.file.seek(0)
        self.file.truncate()
        yaml.dump(self.compose, self.file)
        self.file.flush()

    def invoke(
        self,
        *args: str,
        capture: bool = False,
        capture_stderr: bool = False,
        stdin: str | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """Invoke `docker compose` on the rendered composition.

        Args:
            args: The arguments to pass to `docker compose`.
            capture: Whether to capture the child's stdout stream.
            capture_stderr: Whether to capture the child's stderr stream.
            input: A string to provide as stdin for the command.
        """

        if not self.silent:
            print(f"$ docker compose {' '.join(args)}", file=sys.stderr)

        self.file.seek(0)

        stdout = None
        if capture:
            stdout = subprocess.PIPE
        stderr = None
        if capture_stderr:
            stderr = subprocess.PIPE
        project_name_args = (
            ("--project-name", self.project_name) if self.project_name else ()
        )

        try:
            return subprocess.run(
                [
                    "docker",
                    "compose",
                    f"-f/dev/fd/{self.file.fileno()}",
                    "--project-directory",
                    self.path,
                    *project_name_args,
                    *args,
                ],
                close_fds=False,
                check=check,
                stdout=stdout,
                stderr=stderr,
                input=stdin,
                text=True,
                bufsize=1,
            )
        except subprocess.CalledProcessError as e:
            if e.stdout:
                print(e.stdout)
            raise UIError(f"running docker compose failed (exit status {e.returncode})")

    def port(self, service: str, private_port: int | str) -> int:
        """Get the public port for a service's private port.

        Delegates to `docker compose port`. See that command's help for details.

        Args:
            service: The name of a service in the composition.
            private_port: A private port exposed by the service.
        """
        proc = self.invoke("port", service, str(private_port), capture=True)
        if not proc.stdout.strip():
            raise UIError(
                f"service f{service!r} is not exposing port {private_port!r}",
                hint="is the service running?",
            )
        return int(proc.stdout.split(":")[1])

    def default_port(self, service: str) -> int:
        """Get the default public port for a service.

        Args:
            service: The name of a service in the composition.
        """
        ports = self.compose["services"][service]["ports"]
        if not ports:
            raise UIError(f"service f{service!r} does not expose any ports")
        private_port = str(ports[0]).split(":")[-1]
        return self.port(service, private_port)

    def workflow(self, name: str, *args: str) -> None:
        """Run a workflow in the composition.

        Raises a `KeyError` if the workflow does not exist.

        Args:
            name: The name of the workflow to run.
            args: The arguments to pass to the workflow function.
        """
        ui.header(f"Running workflow {name}")
        func = self.workflows[name]
        parser = WorkflowArgumentParser(name, inspect.getdoc(func), list(args))
        try:
            loader.composition_path = self.path
            if len(inspect.signature(func).parameters) > 1:
                func(self, parser)
            else:
                # If the workflow doesn't have an `args` parameter, parse them here
                # with an empty parser to reject bogus arguments and to handle the
                # trivial help message.
                parser.parse_args()
                func(self)
            self.sanity_restart_mz()
        finally:
            loader.composition_path = None

    @contextmanager
    def override(self, *services: "Service") -> Iterator[None]:
        """Temporarily update the composition with the specified services.

        The services must already exist in the composition. They restored to
        their old definitions when the `with` block ends. Note that the service
        definition is written in its entirety; i.e., the configuration is not
        deep merged but replaced wholesale.

        Lest you are tempted to change this function to allow dynamically
        injecting new services: do not do this! These services will not be
        visible to other commands, like `mzcompose run`, `mzcompose logs`, or
        `mzcompose down`, which makes debugging or inspecting the composition
        challenging.
        """
        # Remember the old composition.
        old_compose = copy.deepcopy(self.compose)

        # Update the composition with the new service definitions.
        deps = self._munge_services([(s.name, cast(dict, s.config)) for s in services])
        for service in services:
            self.compose["services"][service.name] = service.config

        # Re-acquire dependencies, as the override may have swapped an `image`
        # config for an `mzbuild` config.
        deps.acquire()

        self._write_compose()

        # Ensure image freshness
        self.pull_if_variable([service.name for service in services])

        try:
            # Run the next composition.
            yield
        finally:
            # Restore the old composition.
            self.compose = old_compose
            self._write_compose()

    @contextmanager
    def test_case(self, name: str) -> Iterator[None]:
        """Execute a test case.

        This context manager provides a very lightweight testing framework. If
        the body of the context manager raises an exception, the test case is
        considered to have failed; otherwise it is considered to have succeeded.
        In either case the execution time and status of the test are recorded in
        `test_results`.

        Example:
            A simple workflow that executes a table-driven test:

            ```
            @dataclass
            class TestCase:
                name: str
                files: list[str]

            test_cases = [
                TestCase(name="short", files=["quicktests.td"]),
                TestCase(name="long", files=["longtest1.td", "longtest2.td"]),
            ]

            def workflow_default(c: Composition):
                for tc in test_cases:
                    with c.test_case(tc.name):
                        c.run("testdrive", *tc.files)
            ```

        Args:
            name: The name of the test case. Must be unique across the lifetime
                of a composition.
        """
        if name in self.test_results:
            raise UIError(f"test case {name} executed twice")
        ui.header(f"Running test case {name}")
        error = None
        start_time = time.time()
        try:
            yield
            ui.header(f"mzcompose: test case {name} succeeded")
        except Exception as e:
            error = f"{str(type(e))}: {e}"
            print(f"mzcompose: test case {name} failed: {error}", file=sys.stderr)

            if not isinstance(e, UIError):
                traceback.print_exc()
        elapsed = time.time() - start_time
        self.test_results[name] = Composition.TestResult(elapsed, error)

    def sql_connection(
        self,
        service: str = "materialized",
        user: str = "materialize",
        port: int | None = None,
        password: str | None = None,
    ) -> Connection:
        """Get a connection (with autocommit enabled) to the materialized service."""
        port = self.port(service, port) if port else self.default_port(service)
        conn = pg8000.connect(host="localhost", user=user, password=password, port=port)
        conn.autocommit = True
        return conn

    def sql_cursor(
        self,
        service: str = "materialized",
        user: str = "materialize",
        port: int | None = None,
        password: str | None = None,
    ) -> Cursor:
        """Get a cursor to run SQL queries against the materialized service."""
        conn = self.sql_connection(service, user, port, password)
        return conn.cursor()

    def sql(
        self,
        sql: str,
        service: str = "materialized",
        user: str = "materialize",
        port: int | None = None,
        password: str | None = None,
        print_statement: bool = True,
    ) -> None:
        """Run a batch of SQL statements against the materialized service."""
        with self.sql_cursor(
            service=service, user=user, port=port, password=password
        ) as cursor:
            for statement in sqlparse.split(sql):
                if print_statement:
                    print(f"> {statement}")
                cursor.execute(statement)

    def sql_query(
        self,
        sql: str,
        service: str = "materialized",
        user: str = "materialize",
        port: int | None = None,
        password: str | None = None,
    ) -> Any:
        """Execute and return results of a SQL query."""
        with self.sql_cursor(
            service=service, user=user, port=port, password=password
        ) as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def run(
        self,
        service: str,
        *args: str,
        detach: bool = False,
        rm: bool = False,
        env_extra: dict[str, str] = {},
        capture: bool = False,
        capture_stderr: bool = False,
        stdin: str | None = None,
        entrypoint: str | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """Run a one-off command in a service.

        Delegates to `docker compose run`. See that command's help for details.
        Note that unlike `docker compose run`, any services whose definitions
        have changed are rebuilt (like `docker compose up` would do) before the
        command is executed.

        Args:
            service: The name of a service in the composition.
            args: Arguments to pass to the service's entrypoint.
            detach: Run the container in the background.
            stdin: read STDIN from a string.
            env_extra: Additional environment variables to set in the container.
            rm: Remove container after run.
            capture: Capture the stdout of the `docker compose` invocation.
            capture_stderr: Capture the stderr of the `docker compose` invocation.
        """
        # Restart any dependencies whose definitions have changed. The trick,
        # taken from Buildkite's Docker Compose plugin, is to run an `up`
        # command that requests zero instances of the requested service.
        self.invoke("up", "--detach", "--scale", f"{service}=0", service)
        return self.invoke(
            "run",
            *(["--entrypoint", entrypoint] if entrypoint else []),
            *(f"-e{k}={v}" for k, v in env_extra.items()),
            *(["--detach"] if detach else []),
            *(["--rm"] if rm else []),
            service,
            *args,
            capture=capture,
            capture_stderr=capture_stderr,
            stdin=stdin,
            check=check,
        )

    def exec(
        self,
        service: str,
        *args: str,
        detach: bool = False,
        capture: bool = False,
        capture_stderr: bool = False,
        stdin: str | None = None,
        check: bool = True,
        workdir: str | None = None,
    ) -> subprocess.CompletedProcess:
        """Execute a one-off command in a service's running container

        Delegates to `docker compose exec`.

        Args:
            service: The service whose container will be used.
            command: The command to run.
            args: Arguments to pass to the command.
            detach: Run the container in the background.
            stdin: read STDIN from a string.
        """

        return self.invoke(
            "exec",
            *(["--detach"] if detach else []),
            *(["--workdir", workdir] if workdir else []),
            "-T",
            service,
            *(
                self.compose["services"][service]["entrypoint"]
                if "entrypoint" in self.compose["services"][service]
                else []
            ),
            *args,
            capture=capture,
            capture_stderr=capture_stderr,
            stdin=stdin,
            check=check,
        )

    def pull_if_variable(self, services: list[str]) -> None:
        """Pull fresh service images in case the tag indicates thee underlying image may change over time.

        Args:
            services: List of service names
        """

        for service in services:
            if "image" in self.compose["services"][service] and any(
                self.compose["services"][service]["image"].endswith(tag)
                for tag in [":latest", ":unstable", ":rolling"]
            ):
                self.invoke("pull", service)

    def up(
        self,
        *services: str,
        detach: bool = True,
        wait: bool = True,
        persistent: bool = False,
        max_retries: int = 2,
    ) -> None:
        """Build, (re)create, and start the named services.

        Delegates to `docker compose up`. See that command's help for details.

        Args:
            services: The names of services in the composition.
            detach: Run containers in the background.
            wait: Wait for health checks to complete before returning.
                Implies `detach` mode.
            persistent: Replace the container's entrypoint and command with
                `sleep infinity` so that additional commands can be scheduled
                on the container with `Composition.exec`.
        """
        if persistent:
            old_compose = copy.deepcopy(self.compose)
            for service in self.compose["services"].values():
                service["entrypoint"] = ["sleep", "infinity"]
                service["command"] = []
            self._write_compose()

        for retry in range(1, max_retries + 1):
            try:
                self.invoke(
                    "up",
                    *(["--detach"] if detach else []),
                    *(["--wait"] if wait else []),
                    *services,
                )
            except UIError as e:
                if retry < max_retries:
                    print("Retrying ...")
                    time.sleep(1)
                    continue
                else:
                    raise e

            break

        if persistent:
            self.compose = old_compose  # type: ignore
            self._write_compose()

    def validate_sources_sinks_clusters(self) -> str | None:
        """Validate that all sources, sinks & clusters are in a good state"""

        results = self.sql_query(
            """
            SELECT name, status, error, details
            FROM mz_internal.mz_source_statuses
            WHERE NOT(
                status = 'running' OR
                (type = 'progress' AND status = 'created') OR
                (type = 'subsource' AND status = 'starting')
            )
            """
        )
        for (name, status, error, details) in results:
            return f"Source {name} is expected to be running/created, but is {status}, error: {error}, details: {details}"

        results = self.sql_query(
            """
            SELECT name, status, error, details
            FROM mz_internal.mz_sink_statuses
            WHERE status NOT IN ('running', 'dropped')
            """
        )
        for (name, status, error, details) in results:
            return f"Sink {name} is expected to be running/dropped, but is {status}, error: {error}, details: {details}"

        results = self.sql_query(
            """
            SELECT mz_clusters.name, mz_cluster_replicas.name, status, reason
            FROM mz_internal.mz_cluster_replica_statuses
            JOIN mz_cluster_replicas
            ON mz_internal.mz_cluster_replica_statuses.replica_id = mz_cluster_replicas.id
            JOIN mz_clusters ON mz_cluster_replicas.cluster_id = mz_clusters.id
            WHERE status NOT IN ('ready', 'not-ready')
            """
        )
        for (cluster_name, replica_name, status, reason) in results:
            return f"Cluster replica {cluster_name}.{replica_name} is expected to be ready/not-ready, but is {status}, reason: {reason}"

        return None

    def sanity_restart_mz(self) -> None:
        """Restart Materialized if it is part of the composition to find
        problems with persisted objects, functions as a sanity check."""
        if "materialized" in self.compose["services"]:
            self.kill("materialized")
            # TODO(def-): Better way to detect when kill has finished
            time.sleep(3)
            self.up("materialized")
            self.sql("SELECT 1")

            NUM_RETRIES = 60
            for i in range(NUM_RETRIES):
                error = self.validate_sources_sinks_clusters()
                if not error:
                    break
                if i == NUM_RETRIES - 1:
                    raise ValueError(error)
                # Sources and cluster replicas need a few seconds to start up
                print("Retry...")
                time.sleep(1)

    def down(
        self,
        destroy_volumes: bool = True,
        remove_orphans: bool = True,
        sanity_restart_mz: bool = True,
    ) -> None:
        """Stop and remove resources.

        Delegates to `docker compose down`. See that command's help for details.

        Args:
            destroy_volumes: Remove named volumes and anonymous volumes attached
                to containers.
            sanity_restart_mz: Try restarting materialize first if it is part
                of the composition, as a sanity check.
        """
        if sanity_restart_mz:
            self.sanity_restart_mz()

        self.invoke(
            "down",
            *(["--volumes"] if destroy_volumes else []),
            *(["--remove-orphans"] if remove_orphans else []),
        )

    def stop(self, *services: str) -> None:
        """Stop the docker containers for the named services.

        Delegates to `docker compose stop`. See that command's help for details.

        Args:
            services: The names of services in the composition.
        """
        self.invoke("stop", *services)

    def kill(self, *services: str, signal: str = "SIGKILL") -> None:
        """Force stop service containers.

        Delegates to `docker compose kill`. See that command's help for details.

        Args:
            services: The names of services in the composition.
            signal: The signal to deliver.
        """
        self.invoke("kill", f"-s{signal}", *services)

    def pause(self, *services: str) -> None:
        """Pause service containers.

        Delegates to `docker compose pause`. See that command's help for details.

        Args:
            services: The names of services in the composition.
        """
        self.invoke("pause", *services)

    def unpause(self, *services: str) -> None:
        """Unpause service containers

        Delegates to `docker compose unpause`. See that command's help for details.

        Args:
            services: The names of services in the composition.
        """
        self.invoke("unpause", *services)

    def rm(
        self, *services: str, stop: bool = True, destroy_volumes: bool = True
    ) -> None:
        """Remove stopped service containers.

        Delegates to `docker compose rm`. See that command's help for details.

        Args:
            services: The names of services in the composition.
            stop: Stop the containers if necessary.
            destroy_volumes: Destroy any anonymous volumes associated with the
                service. Note that this does not destroy any named volumes
                attached to the service.
        """
        self.invoke(
            "rm",
            "--force",
            *(["--stop"] if stop else []),
            *(["-v"] if destroy_volumes else []),
            *services,
        )

    def rm_volumes(self, *volumes: str, force: bool = False) -> None:
        """Remove the named volumes.

        Args:
            volumes: The names of volumes in the composition.
            force: Whether to force the removal (i.e., don't error if the
                volume does not exist).
        """
        volumes = tuple(f"{self.name}_{v}" for v in volumes)
        spawn.runv(
            ["docker", "volume", "rm", *(["--force"] if force else []), *volumes]
        )

    def sleep(self, duration: float) -> None:
        """Sleep for the specified duration in seconds."""
        print(f"Sleeping for {duration} seconds...")
        time.sleep(duration)

    def container_id(self, service: str) -> str:
        """Return the container_id for the specified service

        Delegates to `docker compose ps`
        """
        output_str = self.invoke("ps", "--quiet", service, capture=True).stdout
        assert output_str is not None

        output_list = output_str.strip("\n").split("\n")
        assert len(output_list) == 1
        assert output_list[0] is not None

        return str(output_list[0])

    def stats(
        self,
        service: str,
    ) -> str:
        """Delegates to `docker stats`

        Args:
            service: The service whose container's stats will be probed.
        """

        return subprocess.run(
            [
                "docker",
                "stats",
                self.container_id(service),
                "--format",
                "{{json .}}",
                "--no-stream",
                "--no-trunc",
            ],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1,
        ).stdout

    def mem(self, service: str) -> int:
        stats_str = self.stats(service)
        stats = json.loads(stats_str)
        assert service in stats["Name"]
        mem_str, _ = stats["MemUsage"].split("/")  # "MemUsage":"1.542GiB / 62.8GiB"
        mem_float = float(re.findall(r"[\d.]+", mem_str)[0])
        if "MiB" in mem_str:
            mem_float = mem_float * 10**6
        elif "GiB" in mem_str:
            mem_float = mem_float * 10**9
        else:
            assert False, f"Unable to parse {mem_str}"
        return round(mem_float)

    def testdrive(
        self,
        input: str,
        service: str = "testdrive",
        persistent: bool = True,
        args: list[str] = [],
        caller: Traceback | None = None,
    ) -> None:
        """Run a string as a testdrive script.

        Args:
            args: Additional arguments to pass to testdrive
            service: Optional name of the testdrive service to use.
            input: The string to execute.
            persistent: Whether a persistent testdrive container will be used.
        """

        caller = caller or getframeinfo(stack()[1][0])

        args_with_source = args + [f"--source={caller.filename}:{caller.lineno}"]

        if persistent:
            self.exec(service, *args_with_source, stdin=input)
        else:
            self.run(service, *args_with_source, stdin=input)
