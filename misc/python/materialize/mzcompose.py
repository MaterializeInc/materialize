# Copyright Materialize, Inc. All rights reserved.
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

from pathlib import Path
from tempfile import TemporaryFile
from typing import (
    Any,
    Dict,
    IO,
    List,
    Optional,
    Collection,
    Type,
    Callable,
    Match,
    TypeVar,
    Union,
    Iterable,
    cast,
)
from typing_extensions import Literal, TypedDict
import functools
import os
import pathlib
import shlex
import json
import subprocess
import sys
import random
import re
import time
import yaml

import pg8000  # type: ignore
import pymysql
import yaml

from materialize import errors
from materialize import mzbuild
from materialize import spawn
from materialize import ui

T = TypeVar("T")
say = ui.speaker("C> ")

_BASHLIKE_ALT_VAR_PATTERN = re.compile(
    r"""\$\{
        (?P<var>[^:}]+):\+
        (?P<alt_var>[^}]+)
        \}""",
    re.VERBOSE,
)

_BASHLIKE_ENV_VAR_PATTERN = re.compile(
    r"""\$\{
        (?P<var>[^:}]+)
        (?P<default>:-[^}]+)?
        \}""",
    re.VERBOSE,
)


LINT_CONFLUENT_PLATFORM_VERSION = "5.5.3"
LINT_DEBEZIUM_VERSIONS = ["1.4", "1.5"]


class LintError:
    def __init__(self, file: Path, message: str):
        self.file = file
        self.message = message

    def __str__(self) -> str:
        return f"{os.path.relpath(self.file)}: {self.message}"

    def __lt__(self, other: "LintError") -> bool:
        return (self.file, self.message) < (other.file, other.message)


def lint_composition(path: Path, composition: Any, errors: List[LintError]) -> None:
    for (name, service) in composition["services"].items():
        if service.get("mzbuild") == "materialized":
            lint_materialized_service(path, name, service, errors)
        elif "mzbuild" not in service and "image" in service:
            lint_image_name(path, service["image"], errors)


def lint_image_name(path: Path, spec: str, errors: List[LintError]) -> None:
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
        if "$" not in tag and tag != LINT_CONFLUENT_PLATFORM_VERSION:
            errors.append(
                LintError(
                    path,
                    f"image {spec} depends on wrong version of Confluent Platform "
                    f"(want {LINT_CONFLUENT_PLATFORM_VERSION})",
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
    """A parsed mzcompose.yml file."""

    def __init__(self, repo: mzbuild.Repository, name: str):
        self.name = name
        self.repo = repo
        self.images: List[mzbuild.Image] = []

        default_tag = os.getenv(f"MZBUILD_TAG", None)

        if name in self.repo.compositions:
            self.path = self.repo.compositions[name]
        else:
            raise errors.UnknownComposition

        with open(self.path) as f:
            compose = yaml.safe_load(f)

        # Stash away sub workflows so that we can load them with the correct environment variables
        self.workflows = compose.pop("mzworkflows", None)

        # Resolve all services that reference an `mzbuild` image to a specific
        # `image` reference.
        for config in compose["services"].values():
            if "mzbuild" in config:
                image_name = config["mzbuild"]

                if image_name not in self.repo.images:
                    raise errors.BadSpec(f"mzcompose: unknown image {image_name}")

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

                if "propagate-uid-gid" in config:
                    config["user"] = f"{os.getuid()}:{os.getgid()}"
                    del config["propagate-uid-gid"]

        deps = self.repo.resolve_dependencies(self.images)
        for config in compose["services"].values():
            if "mzbuild" in config:
                config["image"] = deps[config["mzbuild"]].spec()
                del config["mzbuild"]

        # Emit the munged configuration to a temporary file so that we can later
        # pass it to Docker Compose.
        tempfile = TemporaryFile()
        os.set_inheritable(tempfile.fileno(), True)
        yaml.dump(compose, tempfile, encoding="utf-8")  # type: ignore
        tempfile.flush()
        self.file = tempfile

    def get_env(self, workflow_name: str, parent_env: Dict[str, str]) -> Dict[str, str]:
        """Return the desired environment for a workflow."""

        raw_env = self.workflows[workflow_name].get("env")

        if not isinstance(raw_env, dict) and raw_env is not None:
            raise errors.BadSpec(
                f"Workflow {workflow_name} has wrong type for env: "
                f"expected mapping, got {type(raw_env).__name__}: {raw_env}",
            )
        # ensure that integers (e.g. ports) are treated as env vars
        if isinstance(raw_env, dict):
            raw_env = {k: str(v) for k, v in raw_env.items()}

        # Substitute environment variables from the parent environment, allowing for the child
        # environment to inherit variables from the parent
        child_env = _substitute_env_vars(raw_env, parent_env)

        # Merge the child and parent environments, with the child environment having the tie
        # breaker. This allows for the child to decide if it wants to inherit (from the step
        # above) or override (from this step).
        env = dict(**parent_env)
        if child_env:
            env.update(**child_env)
        return env

    def get_workflow(
        self, parent_env: Dict[str, str], workflow_name: str
    ) -> "Workflow":
        """Return sub-workflow, with env vars substituted using the supplied environment."""
        if not self.workflows:
            raise KeyError(f"No workflows defined for composition {self.name}")
        if workflow_name not in self.workflows:
            raise KeyError(f"No workflow called {workflow_name} in {self.name}")

        # Build this workflow, performing environment substitution as necessary
        workflow_env = self.get_env(workflow_name, parent_env)
        workflow = _substitute_env_vars(self.workflows[workflow_name], workflow_env)

        built_steps = []
        for raw_step in workflow["steps"]:
            # A step could be reused over several workflows, so operate on a copy
            raw_step = raw_step.copy()

            step_name = raw_step.pop("step")
            step_ty = Steps.named(step_name)
            munged = {k.replace("-", "_"): v for k, v in raw_step.items()}
            try:
                step = step_ty(**munged)
            except TypeError as e:
                a = " ".join([f"{k}={v}" for k, v in munged.items()])
                raise errors.BadSpec(
                    f"Unable to construct {step_name} with args {a}: {e}"
                )
            built_steps.append(step)

        return Workflow(workflow_name, built_steps, env=workflow_env, composition=self)

    @classmethod
    def lint(cls, repo: mzbuild.Repository, name: str) -> List[LintError]:
        """Checks a composition for common errors."""
        if not name in repo.compositions:
            raise errors.UnknownComposition

        path = repo.compositions[name]
        with open(path) as f:
            composition = yaml.safe_load(f)

        errors: List[LintError] = []
        lint_composition(path, composition, errors)
        return errors

    def run(
        self,
        args: List[str],
        env: Optional[Dict[str, str]] = None,
        capture: bool = False,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """Invokes docker-compose on the composition.

        Arguments to specify the files in the composition and the project
        directory are added automatically.

        Args:
            args: Additional arguments to pass to docker-compose.
            env: Additional environment variables to set for the child process.
                These are merged with the current environment.
            capture: Whether to capture the child's stdout and stderr, or
                whether to emit directly to the current stdout/stderr streams.
            check: Whether to raise an error if the child process exits with
                a failing exit code.
        """
        self.file.seek(0)
        if env is not None:
            env = dict(os.environ, **env)
        return subprocess.run(
            [
                "docker-compose",
                f"-f/dev/fd/{self.file.fileno()}",
                "--project-directory",
                self.path.parent,
                *args,
            ],
            env=env,
            close_fds=False,
            check=check,
            stdout=subprocess.PIPE if capture else 1,
            stderr=subprocess.PIPE if capture else 2,
        )

    def find_host_ports(self, service: str) -> List[str]:
        """Find all ports open on the host for a given service"""
        # Parsing the output of `docker-compose ps` directly is fraught, as the
        # output depends on terminal width (!). Using the `-q` flag is safe,
        # however, and we can pipe the container IDs into `docker inspect`,
        # which supports machine-readable output.
        containers = self.run(["ps", "-q"], capture=True).stdout.splitlines()
        metadata = spawn.capture(["docker", "inspect", "-f", "{{json .}}", *containers])
        metadata = [json.loads(line) for line in metadata.splitlines()]
        ports = []
        for md in metadata:
            if md["Config"]["Labels"]["com.docker.compose.service"] == service:
                for (name, port_entry) in md["NetworkSettings"]["Ports"].items():
                    for p in port_entry or []:
                        ports.append(p["HostPort"])
        return ports

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
                raise errors.Failed(
                    f"failed to get a unique container id for service {service}, found: {matches}"
                )

            return matches[0]
        except subprocess.CalledProcessError as e:
            raise errors.Failed(f"failed to get container id for {service}: {e}")

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
            raise errors.Failed(f"failed to inspect Docker container: {e}")
        else:
            return output

    def docker_container_is_running(self, container_id: str) -> bool:
        return self.docker_inspect("{{.State.Running}}", container_id) == "'true'"


def _substitute_env_vars(val: T, env: Dict[str, str]) -> T:
    """Substitute docker-compose style env vars in a dict

    This is necessary for mzconduct, since its parameters are not handled by docker-compose
    """
    if isinstance(val, str):
        val = cast(
            T, _BASHLIKE_ENV_VAR_PATTERN.sub(functools.partial(_subst, env), val)
        )
        val = cast(
            T,
            _BASHLIKE_ALT_VAR_PATTERN.sub(
                functools.partial(_alt_subst, env), cast(str, val)
            ),
        )
    elif isinstance(val, dict):
        for k, v in val.items():
            val[k] = _substitute_env_vars(v, env)
    elif isinstance(val, list):
        val = cast(T, [_substitute_env_vars(v, env) for v in val])
    return val


def _subst(env: Dict[str, str], match: Match) -> str:
    var = match.group("var")
    if var is None:
        raise errors.BadSpec(f"Unable to parse environment variable {match.group(0)}")
    # https://github.com/python/typeshed/issues/3902
    default = cast(Optional[str], match.group("default"))

    env_val = env.get(var)
    if env_val is None and default is None:
        say(f"WARNING: unknown env var {var!r}")
        return cast(str, match.group(0))
    elif env_val is None and default is not None:
        # strip the leading ":-"
        env_val = default[2:]
    assert env_val is not None, "should be replaced correctly"
    return env_val


def _alt_subst(env: Dict[str, str], match: Match) -> str:
    var = match.group("var")
    if var is None:
        raise errors.BadSpec(f"Unable to parse environment variable {match.group(0)}")
    # https://github.com/python/typeshed/issues/3902
    altvar = cast(Optional[str], match.group("alt_var"))
    assert altvar is not None, "alt var not captured by regex"

    env_val = env.get(var)
    if env_val is None:
        return ""
    return altvar


class Workflows:
    """All Known Workflows inside a Composition"""

    def __init__(self, workflows: Dict[str, "Workflow"]) -> None:
        self._inner = workflows

    def __getitem__(self, workflow: str) -> "Workflow":
        return self._inner[workflow]

    def all_workflows(self) -> Collection["Workflow"]:
        return self._inner.values()

    def names(self) -> Collection[str]:
        return self._inner.keys()


class Workflow:
    """
    A workflow is a collection of WorkflowSteps and some context

    It is possible to specify additional compose files for specific workflows, and all
    their child workflows will have access to services defined in those files.
    """

    def __init__(
        self,
        name: str,
        steps: List["WorkflowStep"],
        env: Dict[str, str],
        composition: Composition,
    ) -> None:
        self.name = name
        self.env = env
        self.composition = composition
        self._steps = steps

    def overview(self) -> str:
        return "{} [{}]".format(self.name, " ".join([s.name for s in self._steps]))

    def __repr__(self) -> str:
        return "Workflow<{}>".format(self.overview())

    def run(self) -> None:
        for step in self._steps:
            step.run(self)

    def run_compose(
        self, args: List[str], capture: bool = False
    ) -> subprocess.CompletedProcess:
        return self.composition.run(args, self.env, capture=capture)


class Steps:
    """A registry of named `WorkflowStep`_"""

    _steps: Dict[str, Type["WorkflowStep"]] = {}

    @classmethod
    def named(cls, name: str) -> Type["WorkflowStep"]:
        try:
            return cls._steps[name]
        except KeyError:
            raise errors.UnknownItem("step", name, list(cls._steps))

    @classmethod
    def register(cls, name: str) -> Callable[[Type[T]], Type[T]]:
        if name in cls._steps:
            raise ValueError(f"Double registration of step name: {name}")

        def reg(to_register: Type[T]) -> Type[T]:
            if not issubclass(to_register, WorkflowStep):
                raise ValueError(
                    f"Registered step must be a WorkflowStep: {to_register}"
                )
            cls._steps[name] = to_register
            to_register.name = name
            return to_register  # type: ignore

        return reg

    @classmethod
    def print_known_steps(cls) -> None:
        """Print all steps registered with `register`_"""
        for name in sorted(cls._steps):
            print(name)


class WorkflowStep:
    """Peform a single action in a workflow"""

    # populated by Steps.register
    name: str
    """The name used to refer to this step in a workflow file"""

    def __init__(self, **kwargs: Any) -> None:
        pass

    def run(self, workflow: Workflow) -> None:
        """Perform the action specified by this step"""


@Steps.register("print-env")
class PrintEnvStep(WorkflowStep):
    """Prints the `env` `Dict` for this workflow."""

    def run(self, workflow: Workflow) -> None:
        print("Workflow has environment of", workflow.env)


@Steps.register("sleep")
class Sleep(WorkflowStep):
    """Waits for the defined duration of time."""

    def __init__(self, duration: Union[int, str]) -> None:
        self._duration = int(duration)

    def run(self, workflow: Workflow) -> None:
        print(f"Sleeping {self._duration} seconds")
        time.sleep(self._duration)


@Steps.register("start-services")
class StartServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
    """

    def __init__(self, *, services: Optional[List[str]] = None) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise errors.BadSpec(f"services should be a list, got: {self._services}")

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(["up", "-d", *self._services])
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise errors.Failed(f"ERROR: services didn't come up cleanly: {services}")


@Steps.register("kill-services")
class KillServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
      signal: signal to send to the container (e.g. SIGINT)
    """

    def __init__(
        self, *, services: Optional[List[str]] = None, signal: Optional[str] = None
    ) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise errors.BadSpec(f"services should be a list, got: {self._services}")
        self._signal = signal

    def run(self, workflow: Workflow) -> None:
        compose_cmd = ["kill"]
        if self._signal:
            compose_cmd.extend(["-s", self._signal])
        compose_cmd.extend(self._services)

        try:
            workflow.run_compose(compose_cmd)
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise errors.Failed(f"ERROR: services didn't die cleanly: {services}")


@Steps.register("restart-services")
class RestartServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
    """

    def __init__(self, *, services: Optional[List[str]] = None) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise errors.BadSpec(f"services should be a list, got: {self._services}")

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(["restart", *self._services])
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise errors.Failed(f"ERROR: services didn't restart cleanly: {services}")


@Steps.register("remove-services")
class RemoveServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
      destroy_volumes: Boolean to indicate if the volumes should be removed as well
    """

    def __init__(
        self, *, services: Optional[List[str]] = None, destroy_volumes: bool = False
    ) -> None:
        self._services = services if services is not None else []
        self._destroy_volumes = destroy_volumes
        if not isinstance(self._services, list):
            raise errors.BadSpec(f"services should be a list, got: {self._services}")

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(
                [
                    "rm",
                    "-f",
                    "-s",
                    *(["-v"] if self._destroy_volumes else []),
                    *self._services,
                ]
            )
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise errors.Failed(f"ERROR: services didn't restart cleanly: {services}")


@Steps.register("wait-for-postgres")
class WaitForPgStep(WorkflowStep):
    """
    Args:
        dbname: the name of the database to wait for
        host: the host postgres is listening on
        port: the port postgres is listening on
        timeout_secs: How long to wait for postgres to be up before failing (Default: 30)
        query: The query to execute to ensure that it is running (Default: "Select 1")
        user: The chosen user (this is only relevant for postgres)
        service: The service that postgres is running as (Default: postgres)
    """

    def __init__(
        self,
        *,
        dbname: str,
        port: Optional[int] = None,
        host: str = "localhost",
        timeout_secs: int = 60,
        query: str = "SELECT 1",
        user: str = "postgres",
        password: str = "postgres",
        expected: Union[Iterable[Any], Literal["any"]] = [[1]],
        print_result: bool = False,
        service: str = "postgres",
    ) -> None:
        self._dbname = dbname
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._timeout_secs = timeout_secs
        self._query = query
        self._expected = expected
        self._print_result = print_result
        self._service = service

    def run(self, workflow: Workflow) -> None:
        if self._port is None:
            ports = workflow.composition.find_host_ports(self._service)
            if len(ports) != 1:
                raise errors.Failed(
                    f"Unable to unambiguously determine port for {self._service}, "
                    f"found ports: {','.join(ports)}"
                )
            port = int(ports[0])
        else:
            port = self._port
        wait_for_pg(
            dbname=self._dbname,
            host=self._host,
            port=port,
            timeout_secs=self._timeout_secs,
            query=self._query,
            user=self._user,
            password=self._password,
            expected=self._expected,
            print_result=self._print_result,
        )


@Steps.register("wait-for-mz")
class WaitForMzStep(WaitForPgStep):
    """Same thing as wait-for-postgres, but with materialized defaults"""

    def __init__(
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
        super().__init__(
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


@Steps.register("wait-for-mysql")
class WaitForMysqlStep(WorkflowStep):
    """
    Params:
        host: The host mysql is running on
        port: The port mysql is listening on (Default: discover host port)
        user: The user to connect as (Default: mysqluser)
        password: The password to use (Default: mysqlpw)
        service: The name mysql is running as (Default: mysql)
    """

    def __init__(
        self,
        *,
        user: str = "root",
        password: str = "rootpw",
        host: str = "localhost",
        port: Optional[int] = None,
        timeout_secs: int = 60,
        service: str = "mysql",
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs
        self._service = service

    def run(self, workflow: Workflow) -> None:
        if self._port is None:
            ports = workflow.composition.find_host_ports(self._service)
            if len(ports) != 1:
                raise errors.Failed(
                    f"Could not unambiguously determine port for {self._service} "
                    f"found: {','.join(ports)}"
                )
            port = int(ports[0])
        else:
            port = self._port
        wait_for_mysql(
            user=self._user,
            passwd=self._password,
            host=self._host,
            port=port,
            timeout_secs=self._timeout_secs,
        )


@Steps.register("run-mysql")
class RunMysql(WorkflowStep):
    """
    Params:
        host: The host mysql is running on
        port: The port mysql is listening on (Default: discover host port)
        user: The user to connect as (Default: root)
        password: The password to use (Default: rootpw)
        service: The name mysql is running as (Default: mysql)
        query: The query to execute
    """

    def __init__(
        self,
        *,
        user: str = "root",
        password: str = "rootpw",
        host: str = "localhost",
        port: Optional[int] = None,
        service: str = "mysql",
        query: str,
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._service = service
        self._query = query

    def run(self, workflow: Workflow) -> None:
        if self._port is None:
            ports = workflow.composition.find_host_ports(self._service)
            if len(ports) != 1:
                raise errors.Failed(
                    f"Could not unambiguously determine port for {self._service} "
                    f"found: {','.join(ports)}"
                )
            port = int(ports[0])
        else:
            port = self._port
        conn = pymysql.connect(
            user=self._user,
            passwd=self._password,
            host=self._host,
            port=port,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
            autocommit=True,
        )
        with conn.cursor() as cur:
            cur.execute(self._query)


class WaitDependency(TypedDict):
    """For wait-for-tcp, specify additional items to check"""

    host: str
    port: int
    hint: Optional[str]


@Steps.register("wait-for-tcp")
class WaitForTcpStep(WorkflowStep):
    """Wait for a tcp port to be open inside a container

    Params:
        host: The host that is available inside the docker network
        port: the port to connect to
        timeout_secs: How long to wait (default: 30)

        dependencies: A list of {host, port, hint} objects that must
            continue to be up while checking this one. Immediately fail
            the wait if these go down.
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int,
        timeout_secs: int = 60,
        dependencies: Optional[List[WaitDependency]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs
        self._dependencies = dependencies or []

    def run(self, workflow: Workflow) -> None:
        ui.progress(f"waiting for {self._host}:{self._port}", "C")
        for remaining in ui.timeout_loop(self._timeout_secs):
            cmd = f"docker run --rm -t --network {workflow.composition.name}_default ubuntu:bionic-20200403".split()

            try:
                executed = _check_tcp(
                    cmd[:], self._host, self._port, self._timeout_secs
                )
            except subprocess.CalledProcessError as e:
                ui.progress(" {}".format(int(remaining)))
            else:
                ui.progress(" success!", finish=True)
                return

            for dep in self._dependencies:
                host, port = dep["host"], dep["port"]
                try:
                    _check_tcp(
                        cmd[:], host, port, self._timeout_secs, kind="dependency "
                    )
                except subprocess.CalledProcessError as e:
                    message = f"Dependency is down {host}:{port}"
                    if "hint" in dep:
                        message += f"\n    hint: {dep['hint']}"
                    raise errors.Failed(message)

        raise errors.Failed(f"Unable to connect to {self._host}:{self._port}")


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


@Steps.register("drop-kafka-topics")
class DropKafkaTopicsStep(WorkflowStep):
    def __init__(self, *, kafka_container: str, topic_pattern: str) -> None:
        self._container = kafka_container
        self._topic_pattern = topic_pattern

    def run(self, workflow: Workflow) -> None:
        say(f"dropping kafka topics {self._topic_pattern} from {self._container}")
        try:
            spawn.runv(
                [
                    "docker",
                    "exec",
                    "-t",
                    self._container,
                    "kafka-topics",
                    "--delete",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--topic",
                    self._topic_pattern,
                ],
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            # generally this is fine, it just means that the topics already don't exist
            ui.log_in_automation(f"DEBUG: error purging topics: {e}: {e.output}")


@Steps.register("random-chaos")
class RandomChaos(WorkflowStep):
    """
    Add random chaos to running Docker containers.

    :param chaos:          List containing types of chaos to add. If not provided,
                           will default to 'default_chaos'.
    :param services:       List of target Docker services for chaos. If not provided,
                           will default to all running Docker services.
    :param other_service:  Chaos will be randomly added to Docker services as long as
                           'other_service' is running. If not provided, chaos will be
                           added forever.
    """

    default_chaos = [
        "pause",
        "stop",
        "kill",
        "delay",
        "rate",
        "loss",
        "duplicate",
        "corrupt",
    ]

    def __init__(
        self, chaos: List[str] = [], services: List[str] = [], other_service: str = ""
    ):
        self._chaos = chaos
        self._services = services
        self._other_service = other_service

    @staticmethod
    def get_docker_processes(running: bool = False) -> List[Dict[str, Any]]:
        """
        Use 'docker ps' to return all Docker process information.

        :param running: If True, only return running processes.
        :return: str of processes
        """
        try:
            if running:
                cmd = ["docker", "ps", "--format", "{{ json . }}"]
            else:
                cmd = ["docker", "ps", "-a", "--format", "{{ json . }}"]
            # json technically returns any
            out = spawn.capture(cmd, unicode=True)
            procs = [json.loads(line) for line in out.splitlines()]
            return cast(List[Dict[str, Any]], procs)
        except subprocess.CalledProcessError as e:
            raise errors.Failed(f"failed to get Docker container ids: {e}")

    def get_container_names(
        self, services: List[str] = [], running: bool = False
    ) -> List[str]:
        """
        Parse Docker processes for container names.

        :param services: If provided, only return container ids for these services.
        :param running: If True, only return container ids of running processes.
        :return: Docker container id strs
        """
        matches = []
        try:
            docker_processes = self.get_docker_processes(running=running)
            for process in docker_processes:
                if services:
                    if any(s in process["Names"] for s in services):
                        matches.append(process["Names"])
                else:
                    matches.append(process["Names"])

            return matches
        except subprocess.CalledProcessError as e:
            raise errors.Failed(f"failed to get Docker container ids: {e}")

    def run_cmd(self, cmd: str) -> None:
        try:
            spawn.runv(cmd.split())
        except subprocess.CalledProcessError as e:
            say(f"Failed to run command {cmd}: {e}")

    def add_and_remove_chaos(self, add_cmd: str, remove_cmd: str = "") -> None:
        self.run_cmd(add_cmd)
        # todo: Make sleep durations configurable
        say(f"sleeping for 60 seconds...")
        time.sleep(60)
        if remove_cmd:
            self.run_cmd(remove_cmd)

    def add_and_remove_netem_chaos(self, container_id: str, add_cmd: str) -> None:
        remove_cmd = f"docker exec -t {container_id} tc qdisc del dev eth0 root netem"
        self.add_and_remove_chaos(add_cmd, remove_cmd)

    def run(self, workflow: Workflow) -> None:
        if not self._chaos:
            self._chaos = self.default_chaos
        if not self._services:
            self._services = self.get_container_names(running=True)
        say(
            f"will run these chaos types: {self._chaos} on these containers: {self._services}"
        )

        if not self._other_service:
            say(f"no 'other_service' provided, running chaos forever")
            while True:
                self.add_chaos()
        else:
            container_ids = self.get_container_names(services=[self._other_service])
            if len(container_ids) != 1:
                raise errors.Failed(
                    f"wrong number of container ids found for service {self._other_service}. expected 1, found: {len(container_ids)}"
                )

            container_id = container_ids[0]
            say(
                f"running chaos as long as {self._other_service} (container {container_id}) is running"
            )
            while workflow.composition.docker_container_is_running(container_id):
                self.add_chaos()

    def add_chaos(self) -> None:
        random_container = random.choice(self._services)
        random_chaos = random.choice(self._chaos)
        if random_chaos == "pause":
            self.add_and_remove_chaos(
                add_cmd=f"docker pause {random_container}",
                remove_cmd=f"docker unpause {random_container}",
            )
        elif random_chaos == "stop":
            self.add_and_remove_chaos(
                add_cmd=f"docker stop {random_container}",
                remove_cmd=f"docker start {random_container}",
            )
        elif random_chaos == "kill":
            self.add_and_remove_chaos(
                add_cmd=f"docker kill {random_container}",
                remove_cmd=f"docker start {random_container}",
            )
        elif random_chaos == "delay":
            self.add_and_remove_netem_chaos(
                container_id=random_container,
                add_cmd=f"docker exec -t {random_container} tc qdisc add dev eth0 root netem \
                delay 100ms 100ms distribution normal",
            )
        elif random_chaos == "rate":
            self.add_and_remove_netem_chaos(
                container_id=random_container,
                add_cmd=f"docker exec -t {random_container} tc qdisc add dev eth0 root netem \
                rate 5kbit 20 100 5",
            )
        elif random_chaos == "loss":
            self.add_and_remove_netem_chaos(
                container_id=random_container,
                add_cmd=f"docker exec -t {random_container} tc qdisc add dev eth0 root netem loss 10",
            )
        elif random_chaos == "duplicate":
            self.add_and_remove_netem_chaos(
                container_id=random_container,
                add_cmd=f"docker exec -t {random_container} tc qdisc add dev eth0 root netem duplicate 10",
            )
        elif random_chaos == "corrupt":
            self.add_and_remove_netem_chaos(
                container_id=random_container,
                add_cmd=f"docker exec -t {random_container} tc qdisc add dev eth0 root netem corrupt 10",
            )
        else:
            raise errors.Failed(f"unexpected type of chaos: {random_chaos}")


@Steps.register("chaos-confirm")
class ChaosConfirmStep(WorkflowStep):
    """
    Confirms the status of a Docker container. Silently succeeds or raises an error.

    :param service: Name of Docker service to confirm, will be used to grep for container id.
                    NOTE: service name must be unique!
    :param running: If True, confirm container is currently running.
    :param exit_code: If provided, confirm container exit code matches this exit code.
    :param wait: If True, wait for target container to exit before confirming its exit code.
    """

    def __init__(
        self,
        service: str,
        running: bool = False,
        exit_code: int = 0,
        wait: bool = False,
    ) -> None:
        self._service = service
        self._running = running
        self._exit_code = exit_code
        self._wait = wait

    def run(self, workflow: Workflow) -> None:
        container_id = workflow.composition.get_container_id(self._service)
        if self._running:
            if not workflow.composition.docker_container_is_running(container_id):
                raise errors.Failed(
                    f"chaos-confirm: container {container_id} is not running"
                )
        else:
            if self._wait:
                while workflow.composition.docker_container_is_running(container_id):
                    say(f"chaos-confirm: waiting for {self._service} to exit")
                    time.sleep(60)
            else:
                if workflow.composition.docker_container_is_running(container_id):
                    raise errors.Failed(
                        f"chaos-confirm: expected {container_id} to have exited, is running"
                    )

            actual_exit_code = workflow.composition.docker_inspect(
                "{{.State.ExitCode}}", container_id
            )
            if actual_exit_code != f"'{self._exit_code}'":
                raise errors.Failed(
                    f"chaos-confirm: expected exit code '{self._exit_code}' for {container_id}, found {actual_exit_code}"
                )


@Steps.register("workflow")
class WorkflowWorkflowStep(WorkflowStep):
    def __init__(self, workflow: str) -> None:
        self._workflow = workflow

    def run(self, workflow: Workflow) -> None:
        try:
            # Run the specified workflow with the context of the parent workflow
            workflow.composition.get_workflow(workflow.env, self._workflow).run()
        except KeyError:
            raise errors.UnknownItem(
                f"workflow in {workflow.composition.name}",
                self._workflow,
                (w for w in workflow.composition.workflows),
            )


@Steps.register("run")
class RunStep(WorkflowStep):
    """
    Run a service using `mzcompose run`

    Running a service behaves slightly differently than making it come up, importantly it
    is not an _error_ if it ends at all.

    Args:

      - service: (required) the name of the service, from the mzcompose file
      - entrypoint: Overwrite the entrypoint with this
      - command: the command to run. These are the arguments to the entrypoint
      - daemon: run as a daemon (default: False)
      - service_ports: expose and use service ports. (Default: True)
    """

    def __init__(
        self,
        *,
        service: str,
        command: Optional[str] = None,
        daemon: bool = False,
        entrypoint: Optional[str] = None,
        service_ports: bool = True,
    ) -> None:
        cmd = []
        if daemon:
            cmd.append("-d")
        if entrypoint:
            cmd.append(f"--entrypoint={entrypoint}")
        cmd.append(service)
        if command is not None:
            cmd.extend(shlex.split(command))
        self._service_ports = service_ports
        self._command = cmd

    def run(self, workflow: Workflow) -> None:
        try:
            workflow.run_compose(
                [
                    "run",
                    *(["--service-ports"] if self._service_ports else []),
                    *self._command,
                ]
            )
        except subprocess.CalledProcessError:
            raise errors.Failed("giving up: {}".format(ui.shell_quote(self._command)))


@Steps.register("ensure-stays-up")
class EnsureStaysUpStep(WorkflowStep):
    def __init__(self, *, container: str, seconds: int) -> None:
        self._container = container
        self._uptime_secs = seconds

    def run(self, workflow: Workflow) -> None:
        pattern = f"{workflow.composition.name}_{self._container}"
        ui.progress(f"Ensuring {self._container} stays up ", "C")
        for i in range(self._uptime_secs, 0, -1):
            time.sleep(1)
            try:
                stdout = spawn.capture(
                    ["docker", "ps", "--format={{.Names}}"], unicode=True
                )
            except subprocess.CalledProcessError as e:
                raise errors.Failed(f"{e.stdout}")
            found = False
            for line in stdout.splitlines():
                if line.startswith(pattern):
                    found = True
                    break
            if not found:
                print(f"failed! {pattern} logs follow:")
                print_docker_logs(pattern, 10)
                raise errors.Failed(f"container {self._container} stopped running!")
            ui.progress(f" {i}")
        print()


@Steps.register("down")
class DownStep(WorkflowStep):
    def __init__(self, *, destroy_volumes: bool = False) -> None:
        """Bring the cluster down"""
        self._destroy_volumes = destroy_volumes

    def run(self, workflow: Workflow) -> None:
        say("bringing the cluster down")
        workflow.run_compose(["down", *(["-v"] if self._destroy_volumes else [])])


@Steps.register("wait")
class WaitStep(WorkflowStep):
    def __init__(
        self, *, service: str, expected_return_code: int, print_logs: bool = False
    ) -> None:
        """Wait for the container with name service to exit"""
        self._expected_return_code = expected_return_code
        self._service = service
        self._print_logs = print_logs

    def run(self, workflow: Workflow) -> None:
        say(f"Waiting for the service {self._service} to exit")
        ps_proc = workflow.run_compose(["ps", "-q", self._service], capture=True)
        container_ids = [c for c in ps_proc.stdout.decode("utf-8").strip().split("\n")]
        if len(container_ids) > 1:
            raise errors.Failed(
                f"Expected to get a single container for {self._service}; got: {container_ids}"
            )
        elif not container_ids:
            raise errors.Failed(f"No containers returned for service {self._service}")

        container_id = container_ids[0]
        wait_cmd = ["docker", "wait", container_id]
        wait_proc = spawn.runv(wait_cmd, capture_output=True)
        return_codes = [
            int(c) for c in wait_proc.stdout.decode("utf-8").strip().split("\n")
        ]
        if len(return_codes) != 1:
            raise errors.Failed(
                f"Expected single exit code for {container_id}; got: {return_codes}"
            )

        return_code = return_codes[0]
        if return_code != self._expected_return_code:
            raise errors.Failed(
                f"Expected exit code {self._expected_return_code} for {container_id}; got: {return_code}"
            )

        if self._print_logs:
            spawn.runv(["docker", "logs", container_id])


def print_docker_logs(pattern: str, tail: int = 0) -> None:
    out = spawn.capture(
        ["docker", "ps", "-a", "--format={{.Names}}"], unicode=True
    ).splitlines()
    for line in out:
        if line.startswith(pattern):
            spawn.runv(["docker", "logs", "--tail", str(tail), line])


def wait_for_pg(
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
    raise errors.Failed(f"never got correct result for {args}: {error}")


def wait_for_mysql(
    timeout_secs: int, user: str, passwd: str, host: str, port: int
) -> None:
    args = f"mysql user={user} host={host} port={port}"
    ui.progress(f"waiting for {args}", "C")
    error = None
    for _ in ui.timeout_loop(timeout_secs):
        try:
            conn = pymysql.connect(user=user, passwd=passwd, host=host, port=port)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
            if result == (1,):
                print(f"success!")
                return
            else:
                print(f"weird, {args} did not return 1: {result}")
        except Exception as e:
            ui.progress(".")
            error = e
    ui.progress(finish=True)

    raise errors.Failed(f"Never got correct result for {args}: {error}")
