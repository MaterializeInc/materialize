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
import selectors
import ssl
import subprocess
import sys
import threading
import time
import traceback
import urllib.parse
from collections import OrderedDict
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from inspect import Traceback, getframeinfo, getmembers, isfunction, stack
from tempfile import TemporaryFile
from typing import Any, TextIO, cast

import pg8000
import sqlparse
import yaml
from pg8000 import Connection, Cursor

from materialize import MZ_ROOT, mzbuild, spawn, ui
from materialize.mzcompose import loader
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import minio_blob_uri
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
    TestResult,
    try_determine_errors_from_cmd_execution,
)
from materialize.parallel_workload.worker_exception import WorkerFailedException
from materialize.ui import (
    CommandFailureCausedUIError,
    UIError,
)
from materialize.xcompile import Arch


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
        self.test_results: OrderedDict[str, TestResult] = OrderedDict()
        self.files = {}
        self.sources_and_sinks_ignored_from_validation = set()

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
                "mysqldata": None,
                # Used for certain pg-cdc scenarios. The memory will not be
                # allocated for compositions that do not require this volume.
                "sourcedata_512Mb": {
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

        self.files = {}

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
                if "platform" in config:
                    # Intentionally not copying the image.rd so that the
                    # repository details of dependencies are also switched to
                    # the wanted architecture. An alternative would be to
                    # iterate over all the dependencies and change their arch
                    # too, but this is messy. Architecting a cleaner approach
                    # is probably not worth it since the platform-checks
                    # scenarios for the x86-64-aarch64 migration are supposed
                    # to be temporary.
                    # image.rd = copy.deepcopy(image.rd)
                    if config["platform"] == "linux/amd64":
                        image.rd.arch = Arch.X86_64
                    elif config["platform"] == "linux/arm64/v8":
                        image.rd.arch = Arch.AARCH64
                    else:
                        raise ValueError(f"Unknown platform {config['platform']}")
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

    def invoke(
        self,
        *args: str,
        capture: bool | TextIO = False,
        capture_stderr: bool | TextIO = False,
        capture_and_print: bool = False,
        stdin: str | None = None,
        check: bool = True,
        max_tries: int = 1,
        silent: bool = False,
    ) -> subprocess.CompletedProcess:
        """Invoke `docker compose` on the rendered composition.

        Args:
            args: The arguments to pass to `docker compose`.
            capture: Whether to capture the child's stdout stream, can be an
                opened file to capture stdout into the file directly.
            capture_stderr: Whether to capture the child's stderr stream, can
                be an opened file to capture stderr into the file directly.
            capture_and_print: Print during execution and capture the stdout and
                stderr of the `docker compose` invocation.
            input: A string to provide as stdin for the command.
        """

        if not self.silent and not silent:
            print(f"$ docker compose {' '.join(args)}", file=sys.stderr)

        stdout = None
        if capture:
            stdout = subprocess.PIPE if capture == True else capture
        elif capture_stderr:
            # this is necessary for the stderr to work
            stdout = subprocess.PIPE

        stderr = None
        if capture_stderr:
            stderr = subprocess.PIPE if capture_stderr == True else capture_stderr
        project_name_args = (
            ("--project-name", self.project_name) if self.project_name else ()
        )

        # One file per thread to make sure we don't try to read a file which is
        # not seeked to 0, leading to "empty compose file" errors
        thread_id = threading.get_ident()
        file = self.files.get(thread_id)
        if not file:
            file = TemporaryFile(mode="w")
            os.set_inheritable(file.fileno(), True)
            yaml.dump(self.compose, file)
            self.files[thread_id] = file

        cmd = [
            "docker",
            "compose",
            f"-f/dev/fd/{file.fileno()}",
            "--project-directory",
            self.path,
            *project_name_args,
            *args,
        ]

        for retry in range(1, max_tries + 1):
            stdout_result = ""
            stderr_result = ""
            file.seek(0)
            try:
                if capture_and_print:
                    p = subprocess.Popen(
                        cmd,
                        close_fds=False,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )
                    if stdin is not None:
                        p.stdin.write(stdin)  # type: ignore
                    if p.stdin is not None:
                        p.stdin.close()
                    sel = selectors.DefaultSelector()
                    sel.register(p.stdout, selectors.EVENT_READ)  # type: ignore
                    sel.register(p.stderr, selectors.EVENT_READ)  # type: ignore
                    running = True
                    while running:
                        running = False
                        for key, val in sel.select():
                            line = key.fileobj.readline()  # type: ignore
                            if not line:
                                continue
                            # Keep running as long as stdout or stderr have any content
                            running = True
                            if key.fileobj is p.stdout:
                                print(line, end="")
                                stdout_result += line
                            else:
                                print(line, end="", file=sys.stderr)
                                stderr_result += line
                    p.wait()
                    retcode = p.poll()
                    assert retcode is not None
                    if check and retcode:
                        raise subprocess.CalledProcessError(
                            retcode, p.args, output=stdout_result, stderr=stderr_result
                        )
                    return subprocess.CompletedProcess(
                        p.args, retcode, stdout_result, stderr_result
                    )
                else:
                    return subprocess.run(
                        cmd,
                        close_fds=False,
                        check=check,
                        stdout=stdout,
                        stderr=stderr,
                        input=stdin,
                        text=True,
                        bufsize=1,
                    )
            except subprocess.CalledProcessError as e:
                if e.stdout and not capture_and_print:
                    print(e.stdout)
                if e.stderr and not capture_and_print:
                    print(e.stderr, file=sys.stderr)

                if retry < max_tries:
                    print("Retrying ...")
                    time.sleep(3)
                    continue
                else:
                    raise CommandFailureCausedUIError(
                        f"running docker compose failed (exit status {e.returncode})",
                        cmd=e.cmd,
                        stdout=e.stdout,
                        stderr=e.stderr,
                    )
        assert False, "unreachable"

    def port(self, service: str, private_port: int | str) -> int:
        """Get the public port for a service's private port.

        Delegates to `docker compose port`. See that command's help for details.

        Args:
            service: The name of a service in the composition.
            private_port: A private port exposed by the service.
        """
        proc = self.invoke(
            "port", service, str(private_port), capture=True, silent=True
        )
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
            if os.getenv("CI_FINAL_PREFLIGHT_CHECK_VERSION") is not None:
                self.final_preflight_check()
            else:
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

        self.files = {}

        # Ensure image freshness
        self.pull_if_variable([service.name for service in services])

        try:
            # Run the next composition.
            yield
        finally:
            # If sanity_restart existed in the overriden service, but
            # override() disabled it by removing the label,
            # keep the sanity check disabled
            if (
                "materialized" in old_compose["services"]
                and "labels" in old_compose["services"]["materialized"]
                and "sanity_restart"
                in old_compose["services"]["materialized"]["labels"]
            ):
                if (
                    "labels" not in self.compose["services"]["materialized"]
                    or "sanity_restart"
                    not in self.compose["services"]["materialized"]["labels"]
                ):
                    print("sanity_restart disabled by override(), keeping it disabled")
                    del old_compose["services"]["materialized"]["labels"][
                        "sanity_restart"
                    ]

            # Restore the old composition.
            self.compose = old_compose
            self.files = {}

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
                        c.run_testdrive_files(*tc.files)
            ```

        Args:
            name: The name of the test case. Must be unique across the lifetime
                of a composition.
        """
        if name in self.test_results:
            raise UIError(f"test case {name} executed twice")
        ui.header(f"Running test case {name}")
        errors = []
        start_time = time.time()
        try:
            yield
            end_time = time.time()
            ui.header(f"mzcompose: test case {name} succeeded")
        except Exception as e:
            end_time = time.time()
            error_message = f"{e.__class__.__module__}.{e.__class__.__name__}: {e}"
            ui.header(f"mzcompose: test case {name} failed: {error_message}")
            errors = self.extract_test_errors(e, error_message)

            if not isinstance(e, UIError):
                traceback.print_exc()

        duration = end_time - start_time
        self.test_results[name] = TestResult(duration, errors)

    def extract_test_errors(
        self, e: Exception, error_message: str
    ) -> list[TestFailureDetails]:
        errors = [
            TestFailureDetails(
                error_message, details=None, location=None, line_number=None
            )
        ]

        if isinstance(e, CommandFailureCausedUIError):
            try:
                extracted_errors = try_determine_errors_from_cmd_execution(e)
            except:
                extracted_errors = []
            errors = extracted_errors if len(extracted_errors) > 0 else errors
        elif isinstance(e, FailedTestExecutionError):
            errors = e.errors
            assert len(errors) > 0, "Failed test execution does not contain any errors"
        elif isinstance(e, WorkerFailedException):
            errors = [
                TestFailureDetails(
                    error_message, details=str(e.cause), location=None, line_number=None
                )
            ]

        return errors

    def sql_connection(
        self,
        service: str | None = None,
        user: str = "materialize",
        port: int | None = None,
        password: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> Connection:
        if service is None:
            service = "materialized"

        """Get a connection (with autocommit enabled) to the materialized service."""
        port = self.port(service, port) if port else self.default_port(service)
        conn = pg8000.connect(
            host="localhost",
            user=user,
            password=password,
            port=port,
            ssl_context=ssl_context,
        )
        conn.autocommit = True
        return conn

    def sql_cursor(
        self,
        service: str | None = None,
        user: str = "materialize",
        port: int | None = None,
        password: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> Cursor:
        """Get a cursor to run SQL queries against the materialized service."""
        conn = self.sql_connection(service, user, port, password, ssl_context)
        return conn.cursor()

    def sql(
        self,
        sql: str,
        service: str | None = None,
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
        service: str | None = None,
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

    def query_mz_version(self, service: str | None = None) -> str:
        return self.sql_query("SELECT mz_version()", service=service)[0][0]

    def run(
        self,
        service: str,
        *args: str,
        detach: bool = False,
        rm: bool = False,
        env_extra: dict[str, str] = {},
        capture: bool = False,
        capture_stderr: bool = False,
        capture_and_print: bool = False,
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
            capture_and_print: Print during execution and capture the
                stdout+stderr of the `docker compose` invocation.
        """
        # Restart any dependencies whose definitions have changed. The trick,
        # taken from Buildkite's Docker Compose plugin, is to run an `up`
        # command that requests zero instances of the requested service.
        self.up("--scale", f"{service}=0", service, wait=False)
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
            capture_and_print=capture_and_print,
            stdin=stdin,
            check=check,
        )

    def run_testdrive_files(
        self,
        *args: str,
        rm: bool = False,
    ) -> subprocess.CompletedProcess:
        return self.run(
            "testdrive",
            *args,
            rm=rm,
            # needed for sufficient error information in the junit.xml while still printing to stdout during execution
            capture_and_print=True,
        )

    def exec(
        self,
        service: str,
        *args: str,
        detach: bool = False,
        capture: bool = False,
        capture_stderr: bool = False,
        capture_and_print: bool = False,
        stdin: str | None = None,
        check: bool = True,
        workdir: str | None = None,
        env_extra: dict[str, str] = {},
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
            *(f"-e{k}={v}" for k, v in env_extra.items()),
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
            capture_and_print=capture_and_print,
            stdin=stdin,
            check=check,
        )

    def pull_if_variable(self, services: list[str], max_tries: int = 2) -> None:
        """Pull fresh service images in case the tag indicates the underlying image may change over time.

        Args:
            services: List of service names
        """

        for service in services:
            if "image" in self.compose["services"][service] and any(
                tag in self.compose["services"][service]["image"]
                for tag in [":latest", ":unstable", ":rolling"]
            ):
                self.pull_single_image_by_service_name(service, max_tries=max_tries)

    def pull_single_image_by_service_name(
        self, service_name: str, max_tries: int
    ) -> None:
        self.invoke("pull", service_name, max_tries=max_tries)

    def try_pull_service_image(self, service: Service, max_tries: int = 2) -> bool:
        """Tries to pull the specified image and returns if this was successful."""
        try:
            with self.override(service):
                self.pull_single_image_by_service_name(
                    service.name, max_tries=max_tries
                )
                return True
        except UIError:
            return False

    def up(
        self,
        *services: str,
        detach: bool = True,
        wait: bool = True,
        persistent: bool = False,
        max_tries: int = 3,  # increased since quay.io returns 502 sometimes
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
            max_tries: Number of tries on failure.
        """
        if persistent:
            old_compose = copy.deepcopy(self.compose)
            for service in self.compose["services"].values():
                service["entrypoint"] = ["sleep", "infinity"]
                service["command"] = []
            self.files = {}

        self.invoke(
            "up",
            *(["--detach"] if detach else []),
            *(["--wait"] if wait else []),
            *services,
            max_tries=max_tries,
        )

        if persistent:
            self.compose = old_compose  # type: ignore
            self.files = {}

    def validate_sources_sinks_clusters(self) -> str | None:
        """Validate that all sources, sinks & clusters are in a good state"""

        exclusion_clause = "true"
        if len(self.sources_and_sinks_ignored_from_validation) > 0:
            excluded_items = ", ".join(
                f"'{name}'" for name in self.sources_and_sinks_ignored_from_validation
            )
            exclusion_clause = f"name NOT IN ({excluded_items})"

        # starting sources are currently expected if no new data is produced, see #21980
        results = self.sql_query(
            f"""
            SELECT name, status, error, details
            FROM mz_internal.mz_source_statuses
            WHERE NOT(
                status IN ('running', 'starting') OR
                (type = 'progress' AND status = 'created')
            )
            AND {exclusion_clause}
            """
        )
        for (name, status, error, details) in results:
            return f"Source {name} is expected to be running/created, but is {status}, error: {error}, details: {details}"

        results = self.sql_query(
            f"""
            SELECT name, status, error, details
            FROM mz_internal.mz_sink_statuses
            WHERE status NOT IN ('running', 'dropped')
            AND {exclusion_clause}
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

    def final_preflight_check(self) -> None:
        """Check if Mz can do the preflight-check and upgrade/rollback with specified version."""
        version = os.getenv("CI_FINAL_PREFLIGHT_CHECK_VERSION")
        if version is None:
            return
        rollback = ui.env_is_truthy("CI_FINAL_PREFLIGHT_CHECK_ROLLBACK")
        if "materialized" in self.compose["services"]:
            ui.header("Final Preflight Check")
            ps = self.invoke("ps", "materialized", "--quiet", capture=True)
            if len(ps.stdout) == 0:
                print("Service materialized not running, will not upgrade it.")
                return

            self.kill("materialized")
            with self.override(
                Materialized(
                    image=f"materialize/materialized:{version}",
                    environment_extra=["MZ_DEPLOY_GENERATION=1"],
                    healthcheck=[
                        "CMD",
                        "curl",
                        "-f",
                        "localhost:6878/api/leader/status",
                    ],
                )
            ):
                self.up("materialized")
                while True:
                    result = json.loads(
                        self.exec(
                            "materialized",
                            "curl",
                            "localhost:6878/api/leader/status",
                            capture=True,
                        ).stdout
                    )
                    if result["status"] == "ReadyToPromote":
                        return
                    assert (
                        result["status"] == "Initializing"
                    ), f"Unexpected status {result}"
                    print("Not ready yet, waiting 1 s")
                    time.sleep(1)
                if rollback:
                    with self.override(Materialized()):
                        self.up("materialized")
                else:
                    result = json.loads(
                        self.exec(
                            "materialized",
                            "curl",
                            "-X",
                            "POST",
                            "localhost:6878/api/leader/promote",
                            capture=True,
                        ).stdout
                    )
                    assert result["result"] == "Success", f"Unexpected result {result}"
            self.sql("SELECT 1")

            NUM_RETRIES = 60
            for i in range(NUM_RETRIES + 1):
                error = self.validate_sources_sinks_clusters()
                if not error:
                    break
                if i == NUM_RETRIES:
                    raise ValueError(error)
                # Sources and cluster replicas need a few seconds to start up
                print(f"Retrying ({i+1}/{NUM_RETRIES})...")
                time.sleep(1)

            # In case the test has to continue, reset state
            self.kill("materialized")
            self.up("materialized")

        else:
            ui.header(
                "Persist Catalog Forward Compatibility Check skipped because Mz not in services"
            )

    def sanity_restart_mz(self) -> None:
        """Restart Materialized if it is part of the composition to find
        problems with persisted objects, functions as a sanity check."""
        if (
            "materialized" in self.compose["services"]
            and "labels" in self.compose["services"]["materialized"]
            and "sanity_restart" in self.compose["services"]["materialized"]["labels"]
        ):
            ui.header(
                "Sanity Restart: Restart Mz and verify source/sink/replica health"
            )
            ps = self.invoke("ps", "materialized", "--quiet", capture=True)
            if len(ps.stdout) == 0:
                print("Service materialized not running, will not restart it.")
                return

            self.kill("materialized")
            self.up("materialized")
            self.sql("SELECT 1")

            NUM_RETRIES = 60
            for i in range(NUM_RETRIES + 1):
                error = self.validate_sources_sinks_clusters()
                if not error:
                    break
                if i == NUM_RETRIES:
                    raise ValueError(error)
                # Sources and cluster replicas need a few seconds to start up
                print(f"Retrying ({i+1}/{NUM_RETRIES})...")
                time.sleep(1)
        else:
            ui.header(
                "Sanity Restart skipped because Mz not in services or `sanity_restart` label not set"
            )

    def capture_logs(self, *services: str) -> None:
        # Capture logs into services.log since they will be lost otherwise
        # after dowing a composition.
        with open(MZ_ROOT / "services.log", "a") as f:
            self.invoke("logs", "--no-color", *services, capture=f)

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
        if os.getenv("CI_FINAL_PREFLIGHT_CHECK_VERSION") is not None:
            self.final_preflight_check()
        elif sanity_restart_mz:
            self.sanity_restart_mz()
        self.capture_logs()
        self.invoke(
            "down",
            *(["--volumes"] if destroy_volumes else []),
            *(["--remove-orphans"] if remove_orphans else []),
        )

    def stop(self, *services: str, wait: bool = True) -> None:
        """Stop the docker containers for the named services.

        Delegates to `docker compose stop`. See that command's help for details.

        Args:
            services: The names of services in the composition.
        """
        self.invoke("stop", *services)

        if wait:
            self.wait(*services)

    def kill(self, *services: str, signal: str = "SIGKILL", wait: bool = True) -> None:
        """Force stop service containers.

        Delegates to `docker compose kill`. See that command's help for details.

        Args:
            services: The names of services in the composition.
            signal: The signal to deliver.
            wait: Wait for the container die
        """
        self.invoke("kill", f"-s{signal}", *services)

        if wait:
            # Containers exit with exit code 137 ( = 128 + 9 ) when killed via SIGKILL
            self.wait(*services, expect_exit_code=137 if signal == "SIGKILL" else None)

    def wait(self, *services: str, expect_exit_code: int | None = None) -> None:
        """Wait for a container to exit

        Delegates to `docker wait`. See that command's help for details.

        Args:
            services: The names of services in the composition.
            expect_exit_code: Optionally expect a specific exit code
        """

        for service in services:
            container_id = self.container_id(service)

            if container_id is None:
                # Container has already exited, can not `docker wait` on it
                continue

            try:
                exit_code = int(
                    subprocess.run(
                        ["docker", "wait", container_id],
                        check=True,
                        stdout=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    ).stdout
                )
                if expect_exit_code is not None:
                    assert (
                        expect_exit_code == exit_code
                    ), f"service {service} exited with exit code {exit_code}, expected {expect_exit_code}"
            except subprocess.CalledProcessError as e:
                assert "No such container" in str(
                    e
                ), f"docker wait unexpectedly returned {e}"

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
        self.capture_logs()
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

    def container_id(self, service: str) -> str | None:
        """Return the container_id for the specified service

        Delegates to `docker compose ps`
        """
        output_str = self.invoke(
            "ps", "--quiet", service, capture=True, silent=True
        ).stdout
        assert output_str is not None

        if output_str == "":
            return None

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

        container_id = self.container_id(service)
        assert container_id is not None

        return subprocess.run(
            [
                "docker",
                "stats",
                container_id,
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
        mz_service: str | None = None,
    ) -> None:
        """Run a string as a testdrive script.

        Args:
            args: Additional arguments to pass to testdrive
            service: Optional name of the testdrive service to use.
            input: The string to execute.
            persistent: Whether a persistent testdrive container will be used.
            caller: The python source line that invoked testdrive()
            mz_service: The Materialize service name to target
        """

        caller = caller or getframeinfo(stack()[1][0])
        args = args + [f"--source={caller.filename}:{caller.lineno}"]

        if mz_service is not None:
            args += [
                f"--materialize-url=postgres://materialize@{mz_service}:6875",
                f"--materialize-internal-url=postgres://mz_system@{mz_service}:6877",
                f"--persist-consensus-url=postgres://root@{mz_service}:26257?options=--search_path=consensus",
            ]

        if persistent:
            self.exec(service, *args, stdin=input, capture_and_print=True)
        else:
            assert (
                mz_service is None
            ), "testdrive(mz_service = ...) can only be used with persistent Testdrive containers."
            self.run(service, *args, stdin=input, capture_and_print=True)

    def enable_minio_versioning(self) -> None:
        self.up("minio")
        self.up("mc", persistent=True)
        self.exec(
            "mc",
            "mc",
            "alias",
            "set",
            "persist",
            "http://minio:9000/",
            "minioadmin",
            "minioadmin",
        )

        self.exec("mc", "mc", "version", "enable", "persist/persist")

    def backup_crdb(self) -> None:
        self.up("mc", persistent=True)
        self.exec("mc", "mc", "mb", "--ignore-existing", "persist/crdb-backup")
        self.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
                CREATE EXTERNAL CONNECTION backup_bucket
                AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
                BACKUP INTO 'external://backup_bucket';
                DROP EXTERNAL CONNECTION backup_bucket;
            """,
        )

    def restore_mz(self) -> None:
        self.kill("materialized")
        self.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
                DROP DATABASE defaultdb;
                CREATE EXTERNAL CONNECTION backup_bucket
                AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
                RESTORE DATABASE defaultdb
                FROM LATEST IN 'external://backup_bucket';
                DROP EXTERNAL CONNECTION backup_bucket;
            """,
        )
        self.run(
            "persistcli",
            "admin",
            "--commit",
            "restore-blob",
            f"--blob-uri={minio_blob_uri()}",
            "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
        )
        self.up("materialized")

    def cloud_hostname(self) -> str:
        """Uses the mz command line tool to get the hostname of the cloud instance"""
        print("Obtaining hostname of cloud instance ...")
        region_status = self.run("mz", "region", "show", capture=True)
        sql_line = region_status.stdout.split("\n")[2]
        cloud_url = sql_line.split("\t")[1].strip()
        # It is necessary to append the 'https://' protocol; otherwise, urllib can't parse it correctly.
        cloud_hostname = urllib.parse.urlparse("https://" + cloud_url).hostname
        return str(cloud_hostname)
