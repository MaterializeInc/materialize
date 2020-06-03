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
import re
import shlex
import socket
import subprocess
import sys
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Match,
    Optional,
    TextIO,
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
            ps = spawn.capture(["./mzcompose", "--mz-quiet", "ps"], unicode=True,)
        # technically 'docker-compose ps' has a `--filter` flag but...
        # https://github.com/docker/compose/issues/5996
        service_lines = [
            l.strip() for l in ps.splitlines() if service in l and "Up" in l
        ]
        if len(service_lines) == 1:
            *_, port_config = service_lines[0].split()
            if "," in port_config or " " in port_config or "->" not in port_config:
                raise MzRuntimeError(
                    "Unable to unambiguously determine listening port for service:"
                    "\n{}".format(service_lines[0])
                )
            port = port_config.split(":")[1].split("-")[0]
            webbrowser.open(f"http://localhost:{port}")
        elif not service_lines:
            raise MzRuntimeError(f"No running services matched {service}")
        else:
            raise MzRuntimeError(
                "Too many services matched {}:\n{}".format(
                    service, "\n".join(service_lines)
                )
            )

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
                    workflows[workflow_name] = Workflow(
                        workflow_name, built_steps, raw_w.get("include_compose")
                    )

            compositions[name] = Composition(
                name, mzcompose.parent, Workflows(workflows)
            )

        return compositions


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
        include_compose: Optional[List[str]] = None,
    ) -> None:
        self.name = name
        self.include_compose = include_compose or []
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
        w = Workflow(self.name, self._steps, self.include_compose)
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
        mzcompose_up(services, self._docker_extra_args())

    def mzcompose_run(self, services: List[str]) -> None:
        mzcompose_run(services, self._docker_extra_args())

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
        query: The query to execute to ensure that it is running
    """

    def __init__(
        self,
        *,
        dbname: str,
        port: int,
        host: str = "localhost",
        timeout_secs: int = 30,
        query: str = "SELECT 1",
        expected: Union[Iterable[Any], Literal["any"]] = (1,),
        print_result: bool = False,
    ) -> None:
        self._dbname = dbname
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs
        self._query = query
        self._expected = expected
        self._print_result = print_result

    def run(self, comp: Composition, workflow: Workflow) -> None:
        wait_for_pg(
            dbname=self._dbname,
            host=self._host,
            port=self._port,
            timeout_secs=self._timeout_secs,
            query=self._query,
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
        port: int = 6875,
        timeout_secs: int = 10,
        query: str = "SELECT 1",
        expected: Union[Iterable[Any], Literal["any"]] = (1,),
        print_result: bool = False,
    ) -> None:
        super().__init__(
            dbname=dbname,
            host=host,
            port=port,
            timeout_secs=timeout_secs,
            query=query,
            expected=expected,
            print_result=print_result,
        )


@Steps.register("wait-for-mysql")
class WaitForMysqlStep(WorkflowStep):
    """
    Params:
        host: The host mysql is running on
        port: The port mysql is listening on
        user: The user to connect as (Default: mysqluser)
        password: The password to use (Default: mysqlpw)
    """

    def __init__(
        self,
        *,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 3306,
        timeout_secs: int = 10,
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._timeout_secs = timeout_secs

    def run(self, comp: Composition, workflow: Workflow) -> None:
        wait_for_mysql(
            user=self._user,
            passwd=self._password,
            host=self._host,
            port=self._port,
            timeout_secs=self._timeout_secs,
        )


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
    def __init__(
        self,
        *,
        service: str,
        command: Optional[str] = None,
        daemon: bool = False,
        entrypoint: Optional[str] = None,
    ) -> None:
        cmd = []
        if daemon:
            cmd.append("-d")
        if entrypoint:
            cmd.append(f"--entrypoint={entrypoint}")
        cmd.append(service)
        if command is not None:
            cmd.extend(shlex.split(command))
        self._command = cmd

    def run(self, comp: Composition, workflow: Workflow) -> None:
        try:
            workflow.mzcompose_run(self._command)
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
    services: List[str], args: Optional[List[str]] = None
) -> subprocess.CompletedProcess:
    if args is None:
        args = []
    cmd = ["./mzcompose", "--mz-quiet", *args, "up", "-d"]
    return spawn.runv(cmd + services)


def mzcompose_run(
    command: List[str], args: Optional[List[str]] = None
) -> subprocess.CompletedProcess:
    if args is None:
        args = []
    cmd = ["./mzcompose", "--mz-quiet", *args, "run"]
    return spawn.runv(cmd + command)


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
    print_result: bool,
    expected: Union[Iterable[Any], Literal["any"]],
) -> None:
    """Wait for a pg-compatible database (includes materialized)
    """
    args = f"dbname={dbname} host={host} port={port} user=ignored"
    ui.progress(f"waiting for {args} to handle {query!r}", "C")
    error = None
    if isinstance(expected, tuple):
        expected = list(expected)
    for remaining in ui.timeout_loop(timeout_secs):
        try:
            conn = pg8000.connect(
                database=dbname, host=host, port=port, user="ignored", timeout=1
            )
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall()
            found_result = False
            for row in result:
                if expected == "any" or list(row) == expected:
                    if not found_result:
                        found_result = True
                        ui.progress(" up and responding!", finish=True)
                        if print_result:
                            say("query result:")
                    if print_result:
                        print(" ".join([str(r) for r in row]))
            if found_result:
                return
            else:
                say(
                    f"host={host} port={port} did not return any row matching {expected} got: {result}"
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
    ui.progress(f"waitng for {args}", "C")
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
