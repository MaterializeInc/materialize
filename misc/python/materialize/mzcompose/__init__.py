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
import ipaddress
import json
import os
import re
import shlex
import subprocess
import sys
from contextlib import contextmanager
from inspect import getmembers, isfunction
from pathlib import Path
from tempfile import TemporaryFile
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    TypedDict,
    TypeVar,
    Union,
)

import pg8000
import sqlparse
import yaml

from materialize import mzbuild, spawn, ui
from materialize.ui import UIError

T = TypeVar("T")
say = ui.speaker("C> ")


class UnknownCompositionError(UIError):
    """The specified composition was unknown."""

    def __init__(self, name: str):
        super().__init__(f"unknown composition {name!r}")


class LintError:
    def __init__(self, file: Path, message: str):
        self.file = file
        self.message = message

    def __str__(self) -> str:
        return f"{os.path.relpath(self.file)}: {self.message}"

    def __lt__(self, other: "LintError") -> bool:
        return (self.file, self.message) < (other.file, other.message)


def lint_composition(path: Path, composition: Any, errors: List[LintError]) -> None:
    if "services" not in composition:
        return

    for (name, service) in composition["services"].items():
        if service.get("mzbuild") == "materialized":
            lint_materialized_service(path, name, service, errors)
        elif "mzbuild" not in service and "image" in service:
            lint_image_name(path, service["image"], errors)

        if isinstance(service.get("environment"), dict):
            errors.append(
                LintError(
                    path, f"environment for service {name} uses dict instead of list"
                )
            )


def lint_image_name(path: Path, spec: str, errors: List[LintError]) -> None:
    from materialize.mzcompose.services import (
        DEFAULT_CONFLUENT_PLATFORM_VERSION,
        LINT_DEBEZIUM_VERSIONS,
    )

    match = re.search(r"((?P<repo>[^/]+)/)?(?P<image>[^:]+)(:(?P<tag>.*))?", spec)
    if not match:
        errors.append(LintError(path, f"malformatted image specification: {spec}"))
        return
    (repo, image, tag) = (match.group("repo"), match.group("image"), match.group("tag"))

    if not tag:
        errors.append(LintError(path, f"image {spec} missing tag"))
    elif tag == "latest":
        errors.append(LintError(path, f'image {spec} depends on floating "latest" tag'))

    if repo == "confluentinc" and image.startswith("cp-"):
        # An '$XXX' environment variable may have been used to specify the version
        if "$" not in tag and tag != DEFAULT_CONFLUENT_PLATFORM_VERSION:
            errors.append(
                LintError(
                    path,
                    f"image {spec} depends on wrong version of Confluent Platform "
                    f"(want {DEFAULT_CONFLUENT_PLATFORM_VERSION})",
                )
            )

    if repo == "debezium":
        if "$" not in tag and tag not in LINT_DEBEZIUM_VERSIONS:
            errors.append(
                LintError(
                    path,
                    f"image {spec} depends on wrong version of Debezium "
                    f"(want {LINT_DEBEZIUM_VERSIONS})",
                )
            )

    if not repo and image == "zookeeper":
        errors.append(
            LintError(
                path, f"replace {spec} with official confluentinc/cp-zookeeper image"
            )
        )

    if repo == "wurstmeister" and image == "kafka":
        errors.append(
            LintError(path, f"replace {spec} with official confluentinc/cp-kafka image")
        )


def lint_materialized_service(
    path: Path, name: str, service: Any, errors: List[LintError]
) -> None:
    # command may be a string that is passed to the shell, or a list of
    # arguments.
    command = service.get("command", "")
    if isinstance(command, str):
        command = command.split()  # split on whitespace to extract individual arguments
    if "--disable-telemetry" not in command:
        errors.append(
            LintError(
                path,
                "materialized service command does not include --disable-telemetry",
            )
        )
    env = service.get("environment", [])
    if "MZ_DEV=1" not in env:
        errors.append(
            LintError(
                path,
                f"materialized service '{name}' does not specify MZ_DEV=1 in its environment: {env}",
            )
        )


class Composition:
    """A parsed mzcompose.yml with a loaded mzworkflows.py file."""

    def __init__(
        self, repo: mzbuild.Repository, name: str, preserve_ports: bool = False
    ):
        self.name = name
        self.repo = repo
        self.images: List[mzbuild.Image] = []
        self.workflows: Dict[str, Callable[[Composition], None]] = {}

        default_tag = os.getenv(f"MZBUILD_TAG", None)

        if name in self.repo.compositions:
            self.path = self.repo.compositions[name]
        else:
            raise UnknownCompositionError(name)

        # load the mzcompose.yml file, if one exists
        mzcompose_yml = self.path / "mzcompose.yml"
        if mzcompose_yml.exists():
            with open(mzcompose_yml) as f:
                compose = yaml.safe_load(f) or {}
        else:
            compose = {}

        if "version" not in compose:
            compose["version"] = "3.7"

        if "services" not in compose:
            compose["services"] = {}

        # Load the mzworkflows.py file, if one exists
        mzworkflows_py = self.path / "mzworkflows.py"
        if mzworkflows_py.exists():
            spec = importlib.util.spec_from_file_location("mzworkflows", mzworkflows_py)
            assert spec
            module = importlib.util.module_from_spec(spec)
            assert isinstance(spec.loader, importlib.abc.Loader)
            spec.loader.exec_module(module)
            for name, fn in getmembers(module, isfunction):
                if name.startswith("workflow_"):
                    # The name of the workflow is the name of the function
                    # with the "workflow_" prefix stripped and any underscores
                    # replaced with dashes.
                    name = name[len("workflow_") :].replace("_", "-")
                    self.workflows[name] = fn

            for python_service in getattr(module, "services", []):
                compose["services"][python_service.name] = python_service.config

        # Resolve all services that reference an `mzbuild` image to a specific
        # `image` reference.
        for name, config in compose["services"].items():
            if "mzbuild" in config:
                image_name = config["mzbuild"]

                if image_name not in self.repo.images:
                    raise UIError(f"mzcompose: unknown image {image_name}")

                image = self.repo.images[image_name]
                override_tag = os.getenv(
                    f"MZBUILD_{image.env_var_name()}_TAG", default_tag
                )
                if override_tag is not None:
                    config["image"] = image.docker_name(override_tag)
                    print(
                        f"mzcompose: warning: overriding {image_name} image to tag {override_tag}",
                        file=sys.stderr,
                    )
                    del config["mzbuild"]
                else:
                    self.images.append(image)

                if "propagate_uid_gid" in config:
                    config["user"] = f"{os.getuid()}:{os.getgid()}"
                    del config["propagate_uid_gid"]

            ports = config.setdefault("ports", [])
            for i, port in enumerate(ports):
                if ":" in str(port):
                    raise UIError(
                        "programming error: disallowed host port in service {name!r}"
                    )
                if preserve_ports:
                    # If preserving ports, bind the container port to the same
                    # host port.
                    ports[i] = f"{port}:{port}"

            if self.repo.rd.coverage:
                # Emit coverage information to a file in a directory that is
                # bind-mounted to the "coverage" directory on the host. We
                # inject the configuration to all services for simplicity, but
                # this only have an effect if the service runs instrumented Rust
                # binaries.
                config.setdefault("environment", []).append(
                    f"LLVM_PROFILE_FILE=/coverage/{name}-%m.profraw"
                )
                config.setdefault("volumes", []).append("./coverage:/coverage")

        # Add default volumes
        compose.setdefault("volumes", {}).update(
            {
                "mzdata": None,
                "tmp": None,
                "secrets": None,
            }
        )

        deps = self.repo.resolve_dependencies(self.images)
        for config in compose["services"].values():
            if "mzbuild" in config:
                config["image"] = deps[config["mzbuild"]].spec()
                del config["mzbuild"]

        self.compose = compose

        # Emit the munged configuration to a temporary file so that we can later
        # pass it to Docker Compose.
        self.file = TemporaryFile()
        os.set_inheritable(self.file.fileno(), True)
        self._write_compose()

    def _write_compose(self) -> None:
        self.file.seek(0)
        self.file.truncate()
        yaml.dump(self.compose, self.file, encoding="utf-8")  # type: ignore
        self.file.flush()

    def get_workflow(self, workflow_name: str) -> "Workflow":
        """Return sub-workflow."""
        if workflow_name not in self.workflows:
            raise KeyError(f"No workflow called {workflow_name} in {self.name}")

        return Workflow(
            name=workflow_name,
            func=self.workflows[workflow_name],
            composition=self,
        )

    @classmethod
    def lint(cls, repo: mzbuild.Repository, name: str) -> List[LintError]:
        """Checks a composition for common errors."""
        if not name in repo.compositions:
            raise UnknownCompositionError(name)

        errs: List[LintError] = []

        path = repo.compositions[name] / "mzcompose.yml"

        if path.exists():
            with open(path) as f:
                composition = yaml.safe_load(f) or {}

            lint_composition(path, composition, errs)
        return errs

    def run(
        self,
        args: List[str],
        env: Optional[Dict[str, str]] = None,
        capture: bool = False,
        capture_combined: bool = False,
        check: bool = True,
    ) -> "subprocess.CompletedProcess[str]":
        """Invokes docker-compose on the composition.

        Arguments to specify the files in the composition and the project
        directory are added automatically.

        Args:
            args: Additional arguments to pass to docker-compose.
            env: Additional environment variables to set for the child process.
                These are merged with the current environment.
            capture: Whether to capture the child's stdout and stderr, or
                whether to emit directly to the current stdout/stderr streams.
            capture_combined: capture stdout and stderr, and direct all output
                to the stdout property on the returned object
            check: Whether to raise an error if the child process exits with
                a failing exit code.
        """
        print(f"$ docker-compose {' '.join(args)}")

        self.file.seek(0)
        if env is not None:
            env = dict(os.environ, **env)

        stdout = 1
        stderr = 2
        if capture:
            stdout = stderr = subprocess.PIPE
        if capture_combined:
            stdout = subprocess.PIPE
            stderr = subprocess.STDOUT
        try:
            return subprocess.run(
                [
                    "docker-compose",
                    f"-f/dev/fd/{self.file.fileno()}",
                    "--project-directory",
                    self.path,
                    *args,
                ],
                env=env,
                close_fds=False,
                check=check,
                stdout=stdout,
                stderr=stderr,
                encoding="utf-8",
            )
        except subprocess.CalledProcessError as e:
            raise UIError(f"running docker-compose failed (exit status {e.returncode})")

    def find_host_ports(self, service: str) -> List[str]:
        """Find all ports open on the host for a given service"""
        # Parsing the output of `docker-compose ps` directly is fraught, as the
        # output depends on terminal width (!). Using the `-q` flag is safe,
        # however, and we can pipe the container IDs into `docker inspect`,
        # which supports machine-readable output.
        if service not in self.compose["services"]:
            raise UIError(f"unknown service {service!r}")
        ports = []
        for info in self.inspect_service_containers(service):
            for (name, port_entry) in info["NetworkSettings"]["Ports"].items():
                for p in port_entry or []:
                    # When IPv6 is enabled, Docker will bind each port twice. Consider
                    # only IPv4 address to avoid spurious warnings about duplicate
                    # ports.
                    if p["HostPort"] not in ports and isinstance(
                        ipaddress.ip_address(p["HostIp"]), ipaddress.IPv4Address
                    ):
                        ports.append(p["HostPort"])
        return ports

    def inspect_service_containers(
        self, service: str, include_stopped: bool = False
    ) -> Iterable[Dict[str, Any]]:
        """
        Return the JSON from `docker inspect` for each container in the given compose service

        There is no explicit documentation of the structure of the returned
        fields, but you can see them in the docker core repo:
        https://github.com/moby/moby/blob/91dc595e9648318/api/types/types.go#L345-L379
        """
        cmd = ["ps", "-q"]
        if include_stopped:
            cmd.append("-a")
        containers = self.run(cmd, capture=True).stdout.splitlines()
        if not containers:
            return
        metadata = spawn.capture(["docker", "inspect", "-f", "{{json .}}", *containers])
        for line in metadata.splitlines():
            info = json.loads(line)
            labels = info["Config"].get("Labels")
            if (
                labels is not None
                and labels.get("com.docker.compose.service") == service
                and labels.get("com.docker.compose.project") == self.name
            ):
                yield info

    def service_logs(self, service_name: str, tail: int = 20) -> str:
        proc = self.run(
            [
                "logs",
                "--tail",
                str(tail),
                service_name,
            ],
            check=True,
            capture_combined=True,
        )
        return proc.stdout

    def get_container_id(self, service: str, running: bool = False) -> str:
        """Given a service name, tries to find a unique matching container id
        If running is True, only return running containers.
        """
        try:
            if running:
                cmd = f"docker ps".split()
            else:
                cmd = f"docker ps -a".split()
            list_containers = spawn.capture(cmd, unicode=True)

            pattern = re.compile(f"^(?P<c_id>[^ ]+).*{service}")
            matches = []
            for line in list_containers.splitlines():
                m = pattern.search(line)
                if m:
                    matches.append(m.group("c_id"))
            if len(matches) != 1:
                raise UIError(
                    f"failed to get a unique container id for service {service}, found: {matches}"
                )

            return matches[0]
        except subprocess.CalledProcessError as e:
            raise UIError(f"failed to get container id for {service}: {e}")

    def docker_inspect(self, format: str, container_id: str) -> str:
        try:
            cmd = f"docker inspect -f '{format}' {container_id}".split()
            output = spawn.capture(cmd, unicode=True, stderr_too=True).splitlines()[0]
        except subprocess.CalledProcessError as e:
            ui.log_in_automation(
                "docker inspect ({}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                    container_id, ui.shell_quote(cmd), e, e.stdout, e.stderr
                )
            )
            raise UIError(f"failed to inspect Docker container: {e}")
        else:
            return output

    def docker_container_is_running(self, container_id: str) -> bool:
        return self.docker_inspect("{{.State.Running}}", container_id) == "'true'"


class Workflow:
    """
    A workflow is a collection of WorkflowSteps and some context

    It is possible to specify additional compose files for specific workflows, and all
    their child workflows will have access to services defined in those files.
    """

    def __init__(
        self,
        name: str,
        func: Callable,
        composition: Composition,
    ) -> None:
        self.name = name
        self.func = func
        self.composition = composition
        self.takes_args = len(inspect.signature(func).parameters) > 1

    def run(self, args: List[str]) -> None:
        print("Running Python function {}".format(self.name))
        if self.takes_args:
            self.func(self, args)
        else:
            # If the workflow doesn't have an `args` parameter, construct
            # an empty parser to reject bogus arguments and to handle the
            # trivial help message.
            parser = WorkflowArgumentParser(self)
            parser.parse_args(args)
            self.func(self)

    @contextmanager
    def with_services(self, services: List["Service"]) -> Iterator[None]:
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
        old_compose = copy.deepcopy(self.composition.compose)

        # Update the composition with the new service definitions.
        for service in services:
            if service.name not in self.composition.compose["services"]:
                raise RuntimeError(
                    "programming error in call to Workflow.with_services: "
                    f"{service.name!r} does not exist"
                )
            self.composition.compose["services"][service.name] = service.config
        self.composition._write_compose()

        try:
            # Run the next composition.
            yield
        finally:
            # Restore the old composition.
            self.composition.compose = old_compose
            self.composition._write_compose()

    def run_compose(
        self, args: List[str], capture: bool = False
    ) -> subprocess.CompletedProcess:
        return self.composition.run(args, capture=capture)

    def run_sql(self, sql: str) -> None:
        """Run a batch of SQL statements against the materialized service."""
        ports = self.composition.find_host_ports("materialized")
        conn = pg8000.connect(host="localhost", user="materialize", port=int(ports[0]))
        conn.autocommit = True
        cursor = conn.cursor()
        for statement in sqlparse.split(sql):
            cursor.execute(statement)

    def start_and_wait_for_tcp(self, services: List[str]) -> None:
        """Sequentially start the named services, waiting for eaach to become
        available via TCP before moving on to the next."""
        # TODO(benesch): once the workflow API is a proper Python API,
        # remove the `type: ignore` comments below.
        for service in services:
            self.start_services(services=[service])
            for port in self.composition.compose["services"][service].get("ports", []):
                self.wait_for_tcp(host=service, port=port)

    def run_service(
        self,
        service: str,
        command: Optional[Union[str, list]] = None,
        *,
        env: Dict[str, str] = {},
        capture: bool = False,
        daemon: bool = False,
        entrypoint: Optional[str] = None,
    ) -> Any:
        """Run a service using `mzcompose run`.

        Running a service behaves slightly differently than making it come up, importantly it
        is not an _error_ if it ends at all.

        Args:
            service: (required) the name of the service, from the mzcompose file
            entrypoint: Overwrite the entrypoint with this
            command: the command to run. These are the arguments to the entrypoint
            capture: Capture and return output (default: False)
            daemon: run as a daemon (default: False)
        """

        cmd = []
        if daemon:
            cmd.append("-d")
        if entrypoint:
            cmd.append(f"--entrypoint={entrypoint}")
        cmd.append(service)
        if isinstance(command, str):
            cmd.extend(shlex.split(command))
        elif isinstance(command, list):
            cmd.extend(command)
        return self.run_compose(
            args=[
                "run",
                *(f"-e{k}={v}" for k, v in env.items()),
                *cmd,
            ],
            capture=capture,
        ).stdout

    def start_services(self, services: List[str]) -> None:
        """Start a service.

        This method delegates to `docker-compose start`. See that command's help
        for details.

        Args:
            services: The names of services in the workflow.
        """
        self.run_compose(["up", "-d", *services])

    def kill_services(self, services: List[str], signal: Optional[str] = None) -> None:
        """Kill a service.

        This method delegates to `docker-compose kill`. See that command's help
        for details.

        Args:
            services: The names of services in the workflow.
            signal: The signal to send. The default is SIGKILL.
        """
        self.run_compose(
            [
                "kill",
                *(["-s", signal] if signal else []),
                *services,
            ]
        )

    def remove_services(
        self, services: List[str], destroy_volumes: bool = False
    ) -> None:
        """Remove a stopped service.

        This method delegates to `docker-compose rm`. See that command's help
        for details.

        Args:
            services: The names of services in the workflow.
            destroy_volumes: Whether to destroy any anonymous volumes associated
                with the service. Note that named volumes are not removed even
                when this option is enabled.
        """
        self.run_compose(
            [
                "rm",
                "-f",
                "-s",
                *(["-v"] if destroy_volumes else []),
                *services,
            ],
        )

    def remove_volumes(self, volumes: List[str]) -> None:
        """Remove the named volumes.

        Args:
            volumes: The volumes to remove.
        """
        volumes = (f"{self.composition.name}_{v}" for v in volumes)
        spawn.runv(["docker", "volume", "rm", *volumes])

    def wait_for_tcp(
        self,
        *,
        host: str = "localhost",
        port: int,
        timeout_secs: int = 240,
    ) -> None:
        ui.progress(f"waiting for {host}:{port}", "C")
        for remaining in ui.timeout_loop(timeout_secs):
            cmd = f"docker run --rm -t --network {self.composition.name}_default ubuntu:focal-20210723".split()

            try:
                _check_tcp(cmd[:], host, port, timeout_secs)
            except subprocess.CalledProcessError:
                ui.progress(" {}".format(int(remaining)))
            else:
                ui.progress(" success!", finish=True)
                return

        ui.progress(" error!", finish=True)
        try:
            logs = self.composition.service_logs(host)
        except Exception as e:
            logs = f"unable to determine logs: {e}"

        raise UIError(f"Unable to connect to {host}:{port}\nService logs:\n{logs}")

    def wait_for_postgres(
        self,
        *,
        dbname: str = "postgres",
        port: Optional[int] = None,
        host: str = "localhost",
        timeout_secs: int = 120,
        query: str = "SELECT 1",
        user: str = "postgres",
        password: str = "postgres",
        expected: Union[Iterable[Any], Literal["any"]] = [[1]],
        print_result: bool = False,
        service: str = "postgres",
    ) -> None:
        """Wait for a PostgreSQL service to start.

        Args:
            dbname: the name of the database to wait for
            host: the host postgres is listening on
            port: the port postgres is listening on
            timeout_secs: How long to wait for postgres to be up before failing (Default: 30)
            query: The query to execute to ensure that it is running (Default: "Select 1")
            user: The chosen user (this is only relevant for postgres)
            service: The service that postgres is running as (Default: postgres)
        """
        if port is None:
            ports = self.composition.find_host_ports(service)
            if len(ports) != 1:
                logs = self.composition.service_logs(service)
                if ports:
                    msg = (
                        f"Unable to unambiguously determine port for {service},"
                        f"found ports: {','.join(ports)}\nService logs:\n{logs}"
                    )
                else:
                    msg = f"No ports found for {service}\nService logs:\n{logs}"
                raise UIError(msg)
            port = int(ports[0])
        else:
            port = port
        _wait_for_pg(
            dbname=dbname,
            host=host,
            port=port,
            timeout_secs=timeout_secs,
            query=query,
            user=user,
            password=password,
            expected=expected,
            print_result=print_result,
        )

    def wait_for_mz(
        self,
        *,
        user: str = "materialize",
        dbname: str = "materialize",
        host: str = "localhost",
        port: Optional[int] = None,
        timeout_secs: int = 60,
        query: str = "SELECT 1",
        expected: Union[Iterable[Any], Literal["any"]] = [[1]],
        print_result: bool = False,
        service: str = "materialized",
    ) -> None:
        """Like `Workflow.wait_for_mz`, but with Materialize defaults."""
        self.wait_for_postgres(
            user=user,
            dbname=dbname,
            host=host,
            port=port,
            timeout_secs=timeout_secs,
            query=query,
            expected=expected,
            print_result=print_result,
            service=service,
        )


class ServiceConfig(TypedDict, total=False):
    mzbuild: str
    image: str
    hostname: str
    command: str
    ports: Sequence[Union[int, str]]
    environment: List[str]
    depends_on: List[str]
    entrypoint: List[str]
    volumes: List[str]
    networks: Dict[str, Dict[str, List[str]]]
    deploy: Dict[str, Dict[str, Dict[str, str]]]
    propagate_uid_gid: bool
    init: bool


class Service:
    """
    A Service is a service that has been specified in the 'services' variable of mzworkflows.py
    """

    def __init__(self, name: str, config: ServiceConfig) -> None:
        self.name = name
        self.config = config


class WorkflowArgumentParser(argparse.ArgumentParser):
    """An argument parser that takes its name and description from a `Workflow`."""

    def __init__(self, w: Workflow):
        super().__init__(
            prog=f"mzcompose run {w.name}", description=inspect.getdoc(w.func)
        )


def _check_tcp(
    cmd: List[str], host: str, port: int, timeout_secs: int, kind: str = ""
) -> List[str]:
    cmd.extend(
        [
            "timeout",
            str(timeout_secs),
            "bash",
            "-c",
            f"cat < /dev/null > /dev/tcp/{host}/{port}",
        ]
    )
    try:
        spawn.capture(cmd, unicode=True, stderr_too=True)
    except subprocess.CalledProcessError as e:
        ui.log_in_automation(
            "wait-for-tcp ({}{}:{}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                kind, host, port, ui.shell_quote(cmd), e, e.stdout, e.stderr
            )
        )
        raise
    return cmd


def _wait_for_pg(
    timeout_secs: int,
    query: str,
    dbname: str,
    port: int,
    host: str,
    user: str,
    password: str,
    print_result: bool,
    expected: Union[Iterable[Any], Literal["any"]],
) -> None:
    """Wait for a pg-compatible database (includes materialized)"""
    args = f"dbname={dbname} host={host} port={port} user={user} password={password}"
    ui.progress(f"waiting for {args} to handle {query!r}", "C")
    error = None
    for remaining in ui.timeout_loop(timeout_secs):
        try:
            conn = pg8000.connect(
                database=dbname,
                host=host,
                port=port,
                user=user,
                password=password,
                timeout=1,
            )
            # The default (autocommit = false) wraps everything in a transaction.
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(query)
            if expected == "any" and cur.rowcount == -1:
                ui.progress("success!", finish=True)
                return
            result = list(cur.fetchall())
            if expected == "any" or result == expected:
                if print_result:
                    say(f"query result: {result}")
                else:
                    ui.progress("success!", finish=True)
                return
            else:
                say(
                    f"host={host} port={port} did not return rows matching {expected} got: {result}"
                )
        except Exception as e:
            ui.progress(" " + str(int(remaining)))
            error = e
    ui.progress(finish=True)
    raise UIError(f"never got correct result for {args}: {error}")
