# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# load-test — entry point to normal running of all our load tests

import contextlib
import itertools
import os
import shlex
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    TextIO,
    Type,
    TypeVar,
    Union,
    cast,
)
from typing_extensions import Literal

import click
import psycopg2  # type: ignore
import pymysql
import yaml

T = TypeVar("T")


class MzError(Exception):
    """All errors are MzErrors"""


class UnknownItem(MzError):
    """A user specified something that we don't recognize"""

    def __init__(self, kind: str, item: Any, acceptable: Iterable[Any]) -> None:
        self.kind = kind
        self.item = item
        self.acceptable = acceptable

    def __str__(self) -> str:
        val = f"Unknown {self.kind}: '{self.item}'"
        if self.acceptable:
            val += ". Expected one of: " + ", ".join([str(a) for a in self.acceptable])
        return val


class BadSpec(MzError):
    """User provided a bad specification"""


class Failed(MzError):
    """The workflow failed"""


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
def cli() -> None:
    """Conduct composed docker services"""


@cli.command()
@click.option("-d", "--duration-seconds", "duration", default=60 * 5)
@click.option("--tag", help="Which tag to launch for docker processes")
@click.option("-w", "--workflow", help="The name of a workflow to run")
@click.argument("composition")
def run(duration: int, composition: str, tag: str, workflow: Optional[str]) -> None:
    """Conduct a docker-composed set of services

    With the --workflow flag, perform all the steps in the workflow together.
    """
    comp = Composition.find(composition)
    if comp is not None:
        if workflow is not None:
            sayd(f"Executing {comp.name} -> {workflow}")
            comp.run_workflow(workflow)
        else:
            sayd(f"Starting {comp.name}")
            comp.run()
    else:
        raise MzError(
            f"Unknown test {composition}, expected one of: {' '.join(Composition.known_compositions())}"
        )


@cli.command()
@click.argument("composition")
def help_workflows(composition: str) -> None:
    """Help on available workflows in DEMO"""
    comp = Composition.find(composition)
    if comp is not None:
        print("Workflows available:")
        for workflow in comp.workflows():
            print(f"    {workflow.name}")
    else:
        print(f"Unknown comp {comp}, available demos:")
        for comp_ in Composition.known_compositions():
            print(f"    {comp_}")


@cli.command()
@click.argument("composition")
def down(composition: str) -> None:
    comp = Composition.find(composition)
    if comp is not None:
        comp.down()
    else:
        raise UnknownItem("composition", comp, Composition.known_compositions())


@cli.command()
@click.argument("composition")
def nuke(composition: str) -> None:
    """Destroy everything docker, stopping composition before trying"""
    comp = Composition.find(composition)
    if comp is not None:
        comp.down()
        cmds = ["docker system prune -af".split(), "docker volume prune -f".split()]
        for cmd in cmds:
            runv(cmd)
    else:
        raise UnknownItem("composition", comp, Composition.known_compositions())


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
            f"Composition<{self.name}, {self._path}, {len(self._workflows)} workflows>"
        )

    def workflow(self, workflow: str) -> "Workflow":
        """Get a workflow by name"""
        return self._workflows[workflow]

    def workflows(self) -> Iterable["Workflow"]:
        return self._workflows.all_workflows()

    def run(self) -> None:
        with cd(self._path):
            mzcompose_up([])

    def down(self) -> None:
        with cd(self._path):
            mzcompose_down()

    def run_workflow(self, workflow: str) -> None:
        with cd(self._path):
            try:
                workflow_ = self._workflows[workflow]
            except KeyError:
                raise UnknownItem("workflow", workflow, self._workflows.names())
            workflow_.run(self)

    @classmethod
    def find(cls, comp: str) -> Optional["Composition"]:
        """Try to find a configured comp"""
        if cls._demos is None:
            cls._demos = cls.load()
        return cls._demos.get(comp)

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
            Path("demo").glob("*/mzcompose.yml"), Path("test").glob("*/mzcompose.yml")
        )
        for mzcompose in compose_files:
            with mzcompose.open() as fh:
                mzcomp = yaml.safe_load(fh)
            name = mzcompose.parent.name
            raw_comp = mzcomp.get("mzconduct")
            workflows = {}
            if raw_comp is not None:
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
                    workflows[workflow_name] = Workflow(workflow_name, built_steps)

            compositions[name] = Composition(
                name, mzcompose.parent, Workflows(workflows)
            )

        return compositions


class Workflows:
    """All Known Workflows inside a Composition"""

    def __init__(self, workflows: Dict[str, "Workflow"]) -> None:
        self._inner = workflows

    def __getitem__(self, workflow: str) -> "Workflow":
        return self._inner[workflow]

    def all_workflows(self) -> Iterable["Workflow"]:
        return self._inner.values()

    def names(self) -> Iterable[str]:
        return self._inner.keys()


class Workflow:
    """A workflow is a collection of WorkflowSteps
    """

    def __init__(self, name: str, steps: List["WorkflowStep"]) -> None:
        self.name = name
        self._steps = steps

    def overview(self) -> str:
        return "{} [{}]".format(self.name, " ".join([s.name for s in self._steps]))

    def __repr__(self) -> str:
        return "Workflow<{}>".format(self.overview())

    def run(self, comp: Composition) -> None:
        for step in self._steps:
            step.run(comp)


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

    def run(self, comp: Composition) -> None:
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

    def run(self, comp: Composition) -> None:
        proc = mzcompose_up(self._services)
        if proc.returncode != 0:
            sayd(
                "ERROR: processes didn't come up cleanly: {}".format(
                    ", ".join(self._services)
                )
            )


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

    def run(self, comp: Composition) -> None:
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

    def run(self, comp: Composition) -> None:
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

    def run(self, comp: Composition) -> None:
        progress(
            f"waiting for {self._host}:{self._port}", start=True,
        )
        end_time = time.monotonic() + self._timeout_secs
        last_time = 0
        while time.monotonic() < end_time:
            cmd = f"docker run --rm -it --network {comp.name}_default ubuntu:bionic-20200403".split()
            cmd.extend(
                [
                    "timeout",
                    str(self._timeout_secs),
                    "bash",
                    "-c",
                    f"cat < /dev/null > /dev/tcp/{self._host}/{self._port}",
                ]
            )
            proc = run_proc(cmd, capture_output=True)
            if proc.returncode != 0:
                this_time = int(end_time - time.monotonic())
                if this_time != last_time:
                    last_time = this_time
                    progress(" {}".format(this_time))
            else:
                print(" success!")
                return
        raise Failed(f"Unable to connect to {self._host}:{self._port}")


@Steps.register("drop-kafka-topics")
class DropKafkaTopicsStep(WorkflowStep):
    def __init__(self, *, kafka_container: str, topic_pattern: str) -> None:
        self._container = kafka_container
        self._topic_pattern = topic_pattern

    def run(self, comp: Composition) -> None:
        sayd(f"dropping kafka topics {self._topic_pattern} from {self._container}")
        try:
            runv(
                [
                    "docker",
                    "exec",
                    "-it",
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
            sayd(f"INFO: error purging topics: {e}")


@Steps.register("workflow")
class WorkflowWorkflowStep(WorkflowStep):
    def __init__(self, workflow: str) -> None:
        self._workflow = workflow

    def run(self, comp: Composition) -> None:
        comp.workflow(self._workflow).run(comp)


@Steps.register("run")
class RunStep(WorkflowStep):
    def __init__(
        self,
        *,
        service: str,
        command: str,
        daemon: bool = False,
        entrypoint: Optional[str] = None,
    ) -> None:
        cmd = []
        if daemon:
            cmd.append("-d")
        if entrypoint:
            cmd.append(f"--entrypoint={entrypoint}")
        cmd.append(service)
        cmd.extend(shlex.split(command))
        self._command = cmd

    def run(self, comp: Composition) -> None:
        mzcompose_run(self._command)


@Steps.register("ensure-stays-up")
class EnsureStaysUpStep(WorkflowStep):
    def __init__(self, *, container: str, seconds: int) -> None:
        self._container = container
        self._uptime_secs = seconds

    def run(self, comp: Composition) -> None:
        pattern = f"{comp.name}_{self._container}"
        progress(f"Ensuring {self._container} stays up ", start=True)
        for i in range(self._uptime_secs, 0, -1):
            time.sleep(1)
            proc = run_proc(
                ["docker", "ps", "--format={{.Names}}"], capture_output=True,
            )
            if proc.returncode != 0:
                raise Failed(f"{proc.stderr}\n{proc.stdout}")
            found = False
            for line in proc.stdout.splitlines():
                if line.startswith(pattern):
                    found = True
                    break
            if not found:
                print(f"failed! {pattern} logs follow:")
                print_docker_logs(pattern, 10)
                raise Failed(f"container {self._container} stopped running!")
            progress(f" {i}")
        print()


@Steps.register("down")
class DownStep(WorkflowStep):
    def __init__(self, *, destroy_volumes: bool = False) -> None:
        """Bring the cluster down"""
        self._destroy_volumes = destroy_volumes

    def run(self, comp: Composition) -> None:
        sayd("bringing the cluster down")
        mzcompose_down(self._destroy_volumes)


# Generic commands


def mzcompose_up(services: List[str]) -> subprocess.CompletedProcess:
    cmd = ["./mzcompose", "--mz-quiet", "up", "-d"]
    return runv(cmd + services)


def mzcompose_run(command: List[str]) -> subprocess.CompletedProcess:
    cmd = ["./mzcompose", "--mz-quiet", "run"]
    return runv(cmd + command)


def mzcompose_stop(services: List[str]) -> subprocess.CompletedProcess:
    cmd = ["./mzcompose", "stop"]
    return runv(cmd + services)


def mzcompose_down(destroy_volumes: bool = False) -> subprocess.CompletedProcess:
    cmd = ["./mzcompose", "down"]
    if destroy_volumes:
        cmd.append("-v")
    return runv(cmd)


# Helpers


def runv(
    cmd: List[str], check: bool = True, capture_output: bool = False
) -> subprocess.CompletedProcess:
    print("🚀>$", " ".join([shlex.quote(c) for c in cmd]))
    return run_proc(cmd, check=check, capture_output=capture_output)


def run_proc(
    cmd: List[str], encoding: str = "utf-8", **kwargs: Any
) -> subprocess.CompletedProcess:
    """subprocess.run wrapper """
    return subprocess.run(cmd, encoding=encoding, **kwargs)


def progress(msg: str, *, start: bool = False) -> None:
    if start:
        msg = f"w> {msg}"
    print(msg, end="", flush=True)


def sayd(msg: str) -> None:
    print("w>", msg)


def print_docker_logs(pattern: str, tail: int = 0) -> None:
    out = subprocess.check_output(
        ["docker", "ps", "-a", "--format={{.Names}}"], encoding="utf-8"
    ).splitlines()
    for line in out:
        if line.startswith(pattern):
            runv(["docker", "logs", "--tail", str(tail), line])


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
    end_time = time.monotonic() + timeout_secs
    args = f"{dbname=} {host=} {port=}"
    progress(f"waiting for {args} to handle {query!r}", start=True)
    error = None
    if isinstance(expected, tuple):
        expected = list(expected)
    while time.monotonic() < end_time:
        try:
            conn = psycopg2.connect(
                f"dbname={dbname} host={host} port={port}", connect_timeout=1
            )
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall()
            found_result = False
            for row in result:
                if expected == "any" or list(row) == expected:
                    if not found_result:
                        found_result = True
                        print("up and responding!")
                        if print_result:
                            sayd("query result:")
                    if print_result:
                        print(" ".join([str(r) for r in row]))
            if found_result:
                return
            else:
                print(
                    f"{host=} {port=} did not return any row matching {expected} got: {result}"
                )
        except Exception as e:
            progress(" " + str(int(end_time - time.monotonic())))
            error = e
        time.sleep(1)
    raise Failed(f"never got correct result for {args}: {error}")


def wait_for_mysql(
    timeout_secs: int, user: str, passwd: str, host: str, port: int
) -> None:
    end_time = time.monotonic() + timeout_secs
    args = f"mysql {user=} {host=} {port=}"
    print(f"waitng for {args}", end="", flush=True)
    error = None
    while time.monotonic() < end_time:
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
            print(".", end="", flush=True)
            error = e
        time.sleep(1)
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
    try:
        cli(auto_envvar_prefix="MZ")
    except MzError as e:
        sayd(f"ERROR: {e}")
        sys.exit(1)
