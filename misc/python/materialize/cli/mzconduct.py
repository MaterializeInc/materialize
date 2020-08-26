# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzconduct.py - Conduct the runtime behavior of mzcompose compositions

import contextlib
import itertools
import os
import random
import re
import shlex
import subprocess
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Match,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)
from typing_extensions import Literal

import click
import pg8000  # type: ignore
import pymysql
import yaml

from materialize import spawn
from materialize import ui
from materialize.errors import (
    BadSpec,
    Failed,
    MzRuntimeError,
    UnknownItem,
    error_handler,
)


T = TypeVar("T")
say = ui.speaker("C>")

_BASHLIKE_ENV_VAR_PATTERN = re.compile(
    r"""\$\{
        (?P<var>[^:}]+)
        (?P<default>:-[^}]+)?
        \}""",
    re.VERBOSE,
)


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
def cli() -> None:
    """Conduct composed docker services"""


@cli.command()
@click.option("-w", "--workflow", help="The name of a workflow to run")
@click.argument("composition")
@click.argument("services", nargs=-1)
def up(composition: str, workflow: Optional[str], services: Iterable[str],) -> None:
    """Conduct a docker-composed set of services

    With the --workflow flag, perform all the steps in the workflow together.
    """
    comp = Composition.find(composition)
    if workflow is not None:
        say(f"Executing {comp.name} -> {workflow}")
        if services:
            serv = " ".join(services)
            say(f"WARNING: services list specified with -w, ignoring: {serv}")
        comp.run_workflow(workflow)
    else:
        comp.up(list(services))


@cli.command()
@click.option("-w", "--workflow", help="The name of a workflow to run")
@click.argument("composition")
@click.argument("services", nargs=-1)
def run(composition: str, workflow: Optional[str], services: Iterable[str],) -> None:
    """Conduct a docker-composed set of services

    With the --workflow flag, perform all the steps in the workflow together.
    """
    comp = Composition.find(composition)
    if workflow is not None:
        say(f"Executing {comp.name} -> {workflow}")
        if services:
            serv = " ".join(services)
            say(f"WARNING: services list specified with -w, ignoring: {serv}")
        comp.run_workflow(workflow)
    else:
        comp.run(list(services))


@cli.command()
@click.argument("composition")
def build(composition: str) -> None:
    Composition.find(composition).build()


@cli.command()
@click.option("-v", "--volumes", is_flag=True, help="Also destroy volumes")
@click.argument("composition")
def down(composition: str, volumes: bool) -> None:
    comp = Composition.find(composition)
    comp.down(volumes)


@cli.command()
@click.argument("composition")
def ps(composition: str) -> None:
    comp = Composition.find(composition)
    comp.ps()


# Non-mzcompose commands


@cli.command()
@click.argument("composition")
def help_workflows(composition: str) -> None:
    """Help on available workflows in DEMO"""
    comp = Composition.find(composition)
    print("Workflows available:")
    for workflow in comp.workflows():
        print(f"    {workflow.name}")


@cli.command()
@click.argument("composition")
def nuke(composition: str) -> None:
    """Destroy everything docker, stopping composition before trying"""
    comp = Composition.find(composition)
    comp.down()
    cmds = ["docker system prune -af".split(), "docker volume prune -f".split()]
    for cmd in cmds:
        spawn.runv(cmd, capture_output=True)


@cli.command()
@click.argument("composition")
@click.argument("service")
def web(composition: str, service: str) -> None:
    """
    Attempt to open a service in a web browser

    This parses the output of `mzconduct ps` and tries to find the right way to open a
    web browser for you.
    """
    comp = Composition.find(composition)
    comp.web(service)


@cli.group()
def show() -> None:
    """Show properties of a composition"""


@show.command()
@click.argument("composition")
def dir(composition: str) -> None:
    """Show the directory that this composition is in"""
    print(str(Composition.find(composition).path))


# Composition Discovery


class Composition:
    """Information about an mzcompose instance

    This includes its location, and all the workflows it knows about.
    """

    _demos: Optional[Dict[str, "Composition"]] = None

    def __init__(self, name: str, path: Path, workflows: "Workflows") -> None:
        self.name = name
        self._path = path
        self._workflows = workflows

    def __str__(self) -> str:
        return (
            f"Composition<{self.name}, {self._path}, {len(self.workflows())} workflows>"
        )

    @property
    def path(self) -> Path:
        return self._path

    def workflow(self, workflow: str) -> "Workflow":
        """Get a workflow by name"""
        return self._workflows[workflow]

    def workflows(self) -> Collection["Workflow"]:
        return self._workflows.all_workflows()

    def up(self, services: List[str]) -> None:
        with cd(self._path):
            try:
                mzcompose_up(services)
            except subprocess.CalledProcessError:
                raise Failed("error when bringing up all services")

    def build(self) -> None:
        """run mzcompose build in this directory"""
        with cd(self._path):
            spawn.runv(["./mzcompose", "--mz-quiet", "build"])

    def run(self, services: List[str]) -> None:
        """run mzcompose run in this directory"""
        with cd(self._path):
            try:
                mzcompose_run(services)
            except subprocess.CalledProcessError:
                raise Failed("error when bringing up all services")

    def down(self, volumes: bool = False) -> None:
        """run mzcompose down in this directory"""
        with cd(self._path):
            mzcompose_down(volumes)

    def ps(self) -> None:
        with cd(self._path):
            spawn.runv(["./mzcompose", "--mz-quiet", "ps"])

    def run_workflow(self, workflow: str) -> None:
        with cd(self._path):
            try:
                workflow_ = self._workflows[workflow]
            except KeyError:
                raise UnknownItem("workflow", workflow, self._workflows.names())
            workflow_.run(self, None)

    def web(self, service: str) -> None:
        """Best effort attempt to open the service in a web browser"""
        with cd(self._path):
            ports = self.find_host_ports(service)
        if len(ports) == 1:
            webbrowser.open(f"http://localhost:{ports[0]}")
        elif not ports:
            raise MzRuntimeError(f"No running services matched {service}")
        else:
            raise MzRuntimeError(f"Too many ports matched {service}, found: {ports}")

    @classmethod
    def find(cls, comp: str) -> "Composition":
        """Try to find a configured comp

        Raises:
            `UnknownItem`: if the composition cannot be discovered
        """
        if cls._demos is None:
            cls._demos = cls.load()
        try:
            return cls._demos[comp]
        except KeyError:
            raise UnknownItem("composition", comp, Composition.known_compositions())

    @classmethod
    def known_compositions(cls) -> Collection[str]:
        if cls._demos is None:
            cls._demos = cls.load()
        return cls._demos.keys()

    @staticmethod
    def load() -> Dict[str, "Composition"]:
        """Load all demos in the repo"""
        compositions = {}

        compose_files = itertools.chain(
            Path("demo").glob("*/mzcompose.yml"),
            Path("test").glob("*/mzcompose.yml"),
            Path("test/performance").glob("*/mzcompose.yml"),
        )
        for mzcompose in compose_files:
            with mzcompose.open() as fh:
                mzcomp = yaml.safe_load(fh)
            name = mzcompose.parent.name
            raw_comp = mzcomp.get("mzconduct")
            workflows = {}
            if raw_comp is not None:
                # TODO: move this into the workflow so that it can use env vars that are
                # manually defined.
                raw_comp = _substitute_env_vars(raw_comp)
                name = raw_comp.get("name", name)
                for workflow_name, raw_w in raw_comp["workflows"].items():
                    built_steps = []
                    for raw_step in raw_w["steps"]:
                        step_name = raw_step.pop("step")
                        step_ty = Steps.named(step_name)
                        munged = {k.replace("-", "_"): v for k, v in raw_step.items()}
                        try:
                            step = step_ty(**munged)
                        except TypeError as e:
                            a = " ".join([f"{k}={v}" for k, v in munged.items()])
                            raise BadSpec(
                                f"Unable to construct {step_name} with args {a}: {e}"
                            )
                        built_steps.append(step)
                    env = raw_w.get("env")
                    if not isinstance(env, dict) and env is not None:
                        raise BadSpec(
                            f"Workflow {workflow_name} has wrong type for env: "
                            f"expected mapping, got {type(env).__name__}: {env}",
                        )
                    # ensure that integers (e.g. ports) are treated as env vars
                    if isinstance(env, dict):
                        env = {k: str(v) for k, v in env.items()}
                    workflows[workflow_name] = Workflow(
                        workflow_name,
                        built_steps,
                        env=env,
                        include_compose=raw_w.get("include_compose"),
                    )

            compositions[name] = Composition(
                name, mzcompose.parent, Workflows(workflows)
            )

        return compositions

    def find_host_ports(self, service: str) -> List[str]:
        """Find all ports open on the host for a given service
        """
        ps = spawn.capture(["./mzcompose", "--mz-quiet", "ps"], unicode=True)
        # technically 'docker-compose ps' has a `--filter` flag but...
        # https://github.com/docker/compose/issues/5996
        service_lines = [
            l.strip() for l in ps.splitlines() if service in l and "Up" in l
        ]
        ports = []
        for line in service_lines:
            line_parts = line.split()
            host_tcp_parts = [p for p in line_parts if "/tcp" in p and "->" in p]
            these_ports = [p.split(":")[1].split("-")[0] for p in host_tcp_parts]
            ports.extend(these_ports)
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
                raise Failed(
                    f"failed to get a unique container id for service {service}, found: {matches}"
                )

            return matches[0]
        except subprocess.CalledProcessError as e:
            raise Failed(f"failed to get container id for {service}: {e}")

    def docker_inspect(self, format: str, container_id: str) -> str:
        try:
            cmd = f"docker inspect -f '{format}' {container_id}".split()
            output = spawn.capture(cmd, unicode=True, stderr_too=True).splitlines()[0]
        except subprocess.CalledProcessError as e:
            ui.log_in_automation(
                "docker inspect ({}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                    container_id, ui.shell_quote(cmd), e, e.stdout, e.stderr,
                )
            )
            raise Failed(f"failed to inspect Docker container: {e}")
        else:
            return output

    def docker_container_is_running(self, container_id: str) -> bool:
        return self.docker_inspect("{{.State.Running}}", container_id) == "'true'"


def _substitute_env_vars(val: T) -> T:
    """Substitute docker-compose style env vars in a dict

    This is necessary for mzconduct, since its parameters are not handled by docker-compose
    """
    if isinstance(val, str):
        val = cast(T, _BASHLIKE_ENV_VAR_PATTERN.sub(_subst, val))
    elif isinstance(val, dict):
        for k, v in val.items():
            val[k] = _substitute_env_vars(v)
    elif isinstance(val, list):
        val = cast(T, [_substitute_env_vars(v) for v in val])
    return val


def _subst(match: Match) -> str:
    var = match.group("var")
    if var is None:
        raise BadSpec(f"Unable to parse environment variable {match.group(0)}")
    # https://github.com/python/typeshed/issues/3902
    default = cast(Optional[str], match.group("default"))
    env_val = os.getenv(var)
    if env_val is None and default is None:
        say(f"WARNING: unknown env var {var!r}")
        return cast(str, match.group(0))
    elif env_val is None and default is not None:
        # strip the leading ":-"
        env_val = default[2:]
    assert env_val is not None, "should be replaced correctly"
    return env_val


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
        env: Optional[Dict[str, str]] = None,
        include_compose: Optional[List[str]] = None,
    ) -> None:
        self.name = name
        self.include_compose = include_compose or []
        self.env = env if env is not None else {}
        self._steps = steps
        self._parent: Optional["Workflow"] = None

    def overview(self) -> str:
        steps = " ".join([s.name for s in self._steps])
        additional = ""
        if self.include_compose:
            additional = " [depends on {}]".format(",".join(self.include_compose))
        return "{} [{}]{}".format(self.name, steps, additional)

    def __repr__(self) -> str:
        return "Workflow<{}>".format(self.overview())

    def with_parent(self, parent: Optional["Workflow"]) -> "Workflow":
        """Create a new workflow from this one, but with access to the properties on the parent
        """
        env = parent.env.copy() if parent is not None else {}
        env.update(self.env)
        w = Workflow(self.name, self._steps, env, self.include_compose)
        w._parent = parent
        return w

    def _include_compose(self) -> List[str]:
        add = list(self.include_compose)
        if self._parent is not None:
            add.extend(self._parent._include_compose())
        return add

    # Commands
    def run(self, comp: Composition, parent_workflow: Optional["Workflow"]) -> None:
        for step in self._steps:
            step.run(comp, self.with_parent(parent_workflow))

    def mzcompose_up(self, services: List[str]) -> None:
        mzcompose_up(services, self._docker_extra_args(), extra_env=self.env)

    def mzcompose_run(self, services: List[str], service_ports: bool = True) -> None:
        mzcompose_run(
            services,
            self._docker_extra_args(),
            service_ports=service_ports,
            extra_env=self.env,
        )

    def _docker_extra_args(self) -> List[str]:
        """Get additional docker arguments specified by this workflow context
        """
        args = []
        additional = self._include_compose()
        if additional is not None:
            args.extend(["-f", "./mzcompose.yml"])
            for f in additional:
                args.extend(["-f", f])
        return args


class Steps:
    """A registry of named `WorkflowStep`_"""

    _steps: Dict[str, Type["WorkflowStep"]] = {}

    @classmethod
    def named(cls, name: str) -> Type["WorkflowStep"]:
        try:
            return cls._steps[name]
        except KeyError:
            raise UnknownItem("step", name, list(cls._steps))

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

    def run(self, comp: Composition, workflow: Workflow) -> None:
        """Perform the action specified by this step"""


@Steps.register("print-env")
class PrintEnvStep(WorkflowStep):
    """Prints the `env` `Dict` for this workflow.
    """

    def __init__(self) -> None:
        pass

    def run(self, comp: Composition, workflow: Workflow) -> None:
        print("Workflow has environment of", workflow.env)


@Steps.register("start-services")
class StartServicesStep(WorkflowStep):
    """
    Params:
      services: List of service names
    """

    def __init__(self, *, services: Optional[List[str]] = None) -> None:
        self._services = services if services is not None else []
        if not isinstance(self._services, list):
            raise BadSpec(f"services should be a list, got: {self._services}")

    def run(self, comp: Composition, workflow: Workflow) -> None:
        try:
            workflow.mzcompose_up(self._services)
        except subprocess.CalledProcessError:
            services = ", ".join(self._services)
            raise Failed(f"ERROR: services didn't come up cleanly: {services}")


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
        timeout_secs: int = 30,
        query: str = "SELECT 1",
        user: str = "postgres",
        expected: Union[Iterable[Any], Literal["any"]] = (1,),
        print_result: bool = False,
        service: str = "postgres",
    ) -> None:
        self._dbname = dbname
        self._host = host
        self._port = port
        self._user = user
        self._timeout_secs = timeout_secs
        self._query = query
        self._expected = expected
        self._print_result = print_result
        self._service = service

    def run(self, comp: Composition, workflow: Workflow) -> None:
        if self._port is None:
            ports = comp.find_host_ports(self._service)
            if len(ports) != 1:
                raise Failed(
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
            expected=self._expected,
            print_result=self._print_result,
        )


@Steps.register("wait-for-mz")
class WaitForMzStep(WaitForPgStep):
    """Same thing as wait-for-postgres, but with materialized defaults
    """

    def __init__(
        self,
        *,
        dbname: str = "materialize",
        host: str = "localhost",
        port: Optional[int] = None,
        timeout_secs: int = 10,
        query: str = "SELECT 1",
        expected: Union[Iterable[Any], Literal["any"]] = (1,),
        print_result: bool = False,
        service: str = "materialized",
    ) -> None:
        super().__init__(
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
        timeout_secs: int = 10,
        service: str = "mysql",
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs
        self._service = service

    def run(self, comp: Composition, workflow: Workflow) -> None:
        if self._port is None:
            ports = comp.find_host_ports(self._service)
            if len(ports) != 1:
                raise Failed(
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

    def run(self, comp: Composition, workflow: Workflow) -> None:
        if self._port is None:
            ports = comp.find_host_ports(self._service)
            if len(ports) != 1:
                raise Failed(
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


@Steps.register("wait-for-tcp")
class WaitForTcpStep(WorkflowStep):
    """Wait for a tcp port to be open inside a container

    Params:
        host: The host that is available inside the docker network
        port: the port to connect to
        timeout_secs: How long to wait (default: 30)
    """

    def __init__(
        self, *, host: str = "localhost", port: int, timeout_secs: int = 30
    ) -> None:
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs

    def run(self, comp: Composition, workflow: Workflow) -> None:
        ui.progress(
            f"waiting for {self._host}:{self._port}", "C",
        )
        for remaining in ui.timeout_loop(self._timeout_secs):
            cmd = f"docker run --rm -t --network {comp.name}_default ubuntu:bionic-20200403".split()
            cmd.extend(
                [
                    "timeout",
                    str(self._timeout_secs),
                    "bash",
                    "-c",
                    f"cat < /dev/null > /dev/tcp/{self._host}/{self._port}",
                ]
            )
            try:
                spawn.capture(cmd, unicode=True, stderr_too=True)
            except subprocess.CalledProcessError as e:
                ui.log_in_automation(
                    "wait-for-tcp ({}:{}): error running {}: {}, stdout:\n{}\nstderr:\n{}".format(
                        self._host,
                        self._port,
                        ui.shell_quote(cmd),
                        e,
                        e.stdout,
                        e.stderr,
                    )
                )
                ui.progress(" {}".format(int(remaining)))
            else:
                ui.progress(" success!", finish=True)
                return
        raise Failed(f"Unable to connect to {self._host}:{self._port}")


@Steps.register("drop-kafka-topics")
class DropKafkaTopicsStep(WorkflowStep):
    def __init__(self, *, kafka_container: str, topic_pattern: str) -> None:
        self._container = kafka_container
        self._topic_pattern = topic_pattern

    def run(self, comp: Composition, workflow: Workflow) -> None:
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
                ]
            )
        except subprocess.CalledProcessError as e:
            # generally this is fine, it just means that the topics already don't exist
            say(f"INFO: error purging topics: {e}")


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
        self, chaos: List[str] = [], services: List[str] = [], other_service: str = "",
    ):
        self._chaos = chaos
        self._services = services
        self._other_service = other_service

    @staticmethod
    def get_docker_processes(running: bool = False) -> str:
        """
        Use 'docker ps' to return all Docker process information.

        :param running: If True, only return running processes.
        :return: str of processes
        """
        try:
            if running:
                cmd = f"docker ps".split()
            else:
                cmd = f"docker ps -a".split()
            return spawn.capture(cmd, unicode=True)
        except subprocess.CalledProcessError as e:
            raise Failed(f"failed to get Docker container ids: {e}")

    def get_container_ids(
        self, services: List[str] = [], running: bool = False
    ) -> List[str]:
        """
        Parse Docker processes for container ids.

        :param services: If provided, only return container ids for these services.
        :param running: If True, only return container ids of running processes.
        :return: Docker container id strs
        """
        try:
            docker_processes = self.get_docker_processes(running=running)

            patterns = []
            if services:
                for service in services:
                    patterns.append(f"^(?P<c_id>[^ ]+).*{service}")
            else:
                patterns.append(f"^(?P<c_id>[^ ]+).*")

            matches = []
            for pattern in patterns:
                compiled_pattern = re.compile(pattern)
                for process in docker_processes.splitlines():
                    m = compiled_pattern.search(process)
                    if m and m.group("c_id") != "CONTAINER":
                        matches.append(m.group("c_id"))

            return matches
        except subprocess.CalledProcessError as e:
            raise Failed(f"failed to get Docker container ids: {e}")

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

    def run(self, comp: Composition, workflow: Workflow) -> None:
        if not self._chaos:
            self._chaos = self.default_chaos
        if not self._services:
            self._services = self.get_container_ids(running=True)
        say(
            f"will run these chaos types: {self._chaos} on these containers: {self._services}"
        )

        if not self._other_service:
            say(f"no 'other_service' provided, running chaos forever")
            while True:
                self.add_chaos()
        else:
            container_ids = self.get_container_ids(services=[self._other_service])
            if len(container_ids) != 1:
                raise Failed(
                    f"wrong number of container ids found for service {self._other_service}. expected 1, found: {len(container_ids)}"
                )

            container_id = container_ids[0]
            say(
                f"running chaos as long as {self._other_service} (container {container_id}) is running"
            )
            while comp.docker_container_is_running(container_id):
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
            raise Failed(f"unexpected type of chaos: {random_chaos}")


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

    def run(self, comp: Composition, workflow: Workflow) -> None:
        container_id = comp.get_container_id(self._service)
        if self._running:
            if not comp.docker_container_is_running(container_id):
                raise Failed(f"chaos-confirm: container {container_id} is not running")
        else:
            if self._wait:
                while comp.docker_container_is_running(container_id):
                    say(f"chaos-confirm: waiting for {self._service} to exit")
                    time.sleep(60)
            else:
                if comp.docker_container_is_running(container_id):
                    raise Failed(
                        f"chaos-confirm: expected {container_id} to have exited, is running"
                    )

            actual_exit_code = comp.docker_inspect("{{.State.ExitCode}}", container_id)
            if actual_exit_code != f"'{self._exit_code}'":
                raise Failed(
                    f"chaos-confirm: expected exit code '{self._exit_code}' for {container_id}, found {actual_exit_code}"
                )


@Steps.register("workflow")
class WorkflowWorkflowStep(WorkflowStep):
    def __init__(self, workflow: str) -> None:
        self._workflow = workflow

    def run(self, comp: Composition, workflow: Workflow) -> None:
        try:
            # Run the specified workflow with the context of the parent workflow
            sub_workflow = comp.workflow(self._workflow)
            sub_workflow.run(comp, workflow)
        except KeyError:
            raise UnknownItem(
                f"workflow in {comp.name}",
                self._workflow,
                (w.name for w in comp.workflows()),
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

    def run(self, comp: Composition, workflow: Workflow) -> None:
        try:
            workflow.mzcompose_run(self._command, service_ports=self._service_ports)
        except subprocess.CalledProcessError:
            raise Failed("giving up: {}".format(ui.shell_quote(self._command)))


@Steps.register("ensure-stays-up")
class EnsureStaysUpStep(WorkflowStep):
    def __init__(self, *, container: str, seconds: int) -> None:
        self._container = container
        self._uptime_secs = seconds

    def run(self, comp: Composition, workflow: Workflow) -> None:
        pattern = f"{comp.name}_{self._container}"
        ui.progress(f"Ensuring {self._container} stays up ", "C")
        for i in range(self._uptime_secs, 0, -1):
            time.sleep(1)
            try:
                stdout = spawn.capture(
                    ["docker", "ps", "--format={{.Names}}"], unicode=True
                )
            except subprocess.CalledProcessError as e:
                raise Failed(f"{e.stdout}")
            found = False
            for line in stdout.splitlines():
                if line.startswith(pattern):
                    found = True
                    break
            if not found:
                print(f"failed! {pattern} logs follow:")
                print_docker_logs(pattern, 10)
                raise Failed(f"container {self._container} stopped running!")
            ui.progress(f" {i}")
        print()


@Steps.register("down")
class DownStep(WorkflowStep):
    def __init__(self, *, destroy_volumes: bool = False) -> None:
        """Bring the cluster down"""
        self._destroy_volumes = destroy_volumes

    def run(self, comp: Composition, workflow: Workflow) -> None:
        say("bringing the cluster down")
        mzcompose_down(self._destroy_volumes)


# Generic commands


def mzcompose_up(
    services: List[str],
    args: Optional[List[str]] = None,
    extra_env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    if args is None:
        args = []
    cmd = ["./mzcompose", "--mz-quiet", *args, "up", "-d"]
    return spawn.runv(cmd + services, env=_merge_env(extra_env))


def mzcompose_run(
    command: List[str],
    args: Optional[List[str]] = None,
    service_ports: bool = True,
    extra_env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    if args is None:
        args = []
    sp = ["--service-ports"] if service_ports else []
    cmd = ["./mzcompose", "--mz-quiet", *args, "run", *sp, *command]
    return spawn.runv(cmd, env=_merge_env(extra_env))


def _merge_env(extra_env: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Get a mapping that has values from os.environ overwritten by env, if present
    """
    env = cast(dict, os.environ)
    if extra_env:
        env = os.environ.copy()
        env.update(extra_env)
    return env


def mzcompose_stop(services: List[str]) -> subprocess.CompletedProcess:
    cmd = ["./mzcompose", "--mz-quiet", "stop"]
    return spawn.runv(cmd + services)


def mzcompose_down(destroy_volumes: bool = False) -> subprocess.CompletedProcess:
    cmd = ["./mzcompose", "--mz-quiet", "down"]
    if destroy_volumes:
        cmd.append("--volumes")
    return spawn.runv(cmd)


# Helpers


def print_docker_logs(pattern: str, tail: int = 0) -> None:
    out = spawn.capture(
        ["docker", "ps", "-a", "--format={{.Names}}"], unicode=True
    ).splitlines()
    for line in out:
        if line.startswith(pattern):
            spawn.runv(["docker", "logs", "--tail", str(tail), line])


def now() -> datetime:
    return datetime.now(timezone.utc)


def wait_for_pg(
    timeout_secs: int,
    query: str,
    dbname: str,
    port: int,
    host: str,
    user: str,
    print_result: bool,
    expected: Union[Iterable[Any], Literal["any"]],
) -> None:
    """Wait for a pg-compatible database (includes materialized)
    """
    args = f"dbname={dbname} host={host} port={port} user={user}"
    ui.progress(f"waiting for {args} to handle {query!r}", "C")
    error = None
    for remaining in ui.timeout_loop(timeout_secs):
        try:
            conn = pg8000.connect(
                database=dbname, host=host, port=port, user=user, timeout=1
            )
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
    raise Failed(f"never got correct result for {args}: {error}")


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

    raise Failed(f"Never got correct result for {args}: {error}")


@contextlib.contextmanager
def cd(path: Path) -> Any:
    """Execute block within path, and then return"""
    orig_path = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(orig_path)


if __name__ == "__main__":
    with error_handler(say):
        cli(auto_envvar_prefix="MZ")
