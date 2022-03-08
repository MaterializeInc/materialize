# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzcompose.py — runs Docker Compose with Materialize customizations.

"""Command-line driver for mzcompose.

This module is the main user interface to mzcompose, which looks much like
Docker Compose but adds several Materialize-specific features.

NOTE(benesch): Much of the complexity here is very delicate argument parsing to
ensure that the correct command line flags are forwarded to Docker Compose in
just the right way. This stretches the limit of argparse, but that complexity
has been carefully managed. If you are tempted to refactor the argument parsing
code, please talk to me first!
"""


import argparse
import inspect
import os
import subprocess
import sys
import webbrowser
from pathlib import Path
from typing import IO, Any, List, Optional, Sequence, Text, Tuple, Union

import junit_xml
from humanize import naturalsize

from materialize import ROOT, ci_util, mzbuild, mzcompose, spawn, ui
from materialize.ui import UIError

MIN_COMPOSE_VERSION = (1, 24, 0)
RECOMMENDED_MIN_MEM = 8 * 1024**3  # 8GiB
RECOMMENDED_MIN_CPUS = 2


def main(argv: List[str]) -> None:
    parser = ArgumentParser(
        prog="mzcompose",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
mzcompose orchestrates services defined in mzcompose.yml or mzcompose.py.
It wraps Docker Compose to add some Materialize-specific features.""",
        epilog="""
These are only the most common options. There are additional Docker Compose
options that are also supported. Consult `docker-compose help` for the full
set.

For help on a specific command, run `mzcompose COMMAND --help`.

For additional details on mzcompose, consult doc/developer/mzbuild.md.""",
    )

    # Global arguments.
    parser.add_argument(
        "--mz-quiet",
        action="store_true",
        help="suppress Materialize-specific informational messages",
    )
    parser.add_argument(
        "--find",
        metavar="DIR",
        help="use the mzcompose.yml file from DIR, rather than the current directory",
    )
    parser.add_argument(
        "--preserve-ports",
        action="store_true",
        help="bind container ports to the same host ports rather than choosing random host ports",
    )
    mzbuild.Repository.install_arguments(parser)

    # Docker Compose arguments that we explicitly ban. Since we don't support
    # these, we hide them from the help output.
    parser.add_argument("-f", "--file", nargs="?", help=argparse.SUPPRESS)
    parser.add_argument("--project-directory", nargs="?", help=argparse.SUPPRESS)
    parser.add_argument("-v", "--version", action="store_true", help=argparse.SUPPRESS)

    # Subcommands. We explicitly hardcode a list of known Docker Compose
    # commands for the sake of help text.
    subparsers = parser.add_subparsers(
        metavar="COMMAND", parser_class=ArgumentSubparser
    )
    BuildCommand.register(parser, subparsers)
    ConfigCommand.register(parser, subparsers)
    CreateCommand.register(parser, subparsers)
    DescribeCommand().register(parser, subparsers)
    DownCommand.register(parser, subparsers)
    EventsCommand.register(parser, subparsers)
    ExecCommand.register(parser, subparsers)
    GenShortcutsCommand().register(parser, subparsers)
    help_command = HelpCommand()
    parser.set_defaults(command=help_command)
    help_command.register(parser, subparsers)
    ImagesCommand.register(parser, subparsers)
    KillCommand.register(parser, subparsers)
    LintCommand().register(parser, subparsers)
    ListCompositionsCommand().register(parser, subparsers)
    ListWorkflowsCommand().register(parser, subparsers)
    LogsCommand.register(parser, subparsers)
    PauseCommand.register(parser, subparsers)
    PortCommand.register(parser, subparsers)
    PsCommand.register(parser, subparsers)
    PullCommand.register(parser, subparsers)
    PushCommand.register(parser, subparsers)
    RestartCommand.register(parser, subparsers)
    RmCommand.register(parser, subparsers)
    RunCommand().register(parser, subparsers)
    ScaleCommand.register(parser, subparsers)
    StartCommand.register(parser, subparsers)
    StopCommand.register(parser, subparsers)
    SqlCommand().register(parser, subparsers)
    TopCommand.register(parser, subparsers)
    UnpauseCommand.register(parser, subparsers)
    UpCommand.register(parser, subparsers)
    WebCommand().register(parser, subparsers)

    args = parser.parse_args(argv)
    if args.file:
        parser.error("-f/--file option not supported")
    elif args.project_directory:
        parser.error("--project-directory option not supported")
    elif args.version:
        parser.error("-v/--version option not supported")
    ui.Verbosity.init_from_env(args.mz_quiet)
    args.command.invoke(args)


def load_composition(args: argparse.Namespace) -> mzcompose.Composition:
    """Loads the composition specified by the command-line arguments."""
    repo = mzbuild.Repository.from_arguments(ROOT, args)
    try:
        return mzcompose.Composition(
            repo, name=args.find or Path.cwd().name, preserve_ports=args.preserve_ports
        )
    except mzcompose.UnknownCompositionError as e:
        if args.find:
            hint = "available compositions:\n"
            for name in repo.compositions:
                hint += f"    {name}\n"
            e.set_hint(hint)
            raise e
        else:
            hint = "enter one of the following directories and run ./mzcompose:\n"
            for path in repo.compositions.values():
                hint += f"    {path.relative_to(Path.cwd())}\n"
            raise UIError(
                "directory does not contain an mzcompose.yml or mzcompose.py",
                hint,
            )


class Command:
    """An mzcompose command."""

    name: str
    """The name of the command."""

    aliases: List[str] = []
    """Aliases to register for the command."""

    help: str
    """The help text displayed in top-level usage output."""

    add_help = True
    """Whether to add the `--help` argument to the subparser."""

    allow_unknown_arguments = False
    """Whether to error if unknown arguments are encountered before calling `run`."""

    def register(
        self,
        root_parser: argparse.ArgumentParser,
        subparsers: argparse._SubParsersAction,
    ) -> None:
        """Register this command as a subcommand of an argument parser."""
        self.root_parser = root_parser
        parser = subparsers.add_parser(
            self.name,
            aliases=self.aliases,
            help=self.help,
            description=self.help,
            add_help=self.add_help,
        )
        parser.set_defaults(command=self)
        self.configure(parser)

    def invoke(self, args: argparse.Namespace) -> None:
        """Invoke this command."""
        if not self.allow_unknown_arguments:
            unknown_args = [*args.unknown_args, *args.unknown_subargs]
            if unknown_args:
                self.root_parser.error(f"unknown argument {unknown_args[0]!r}")
        self.run(args)

    def configure(self, parser: argparse.ArgumentParser) -> None:
        """Override this in subclasses to add additional arguments."""
        pass

    def run(self, args: argparse.Namespace) -> None:
        """Override this in subclasses to specify the command's behavior."""
        pass


class HelpCommand(Command):
    name = "help"
    help = "show this command"
    add_help = False
    allow_unknown_arguments = True

    def run(self, args: argparse.Namespace) -> None:
        self.root_parser.print_help()


class GenShortcutsCommand(Command):
    name = "gen-shortcuts"
    help = "generate shortcut `mzcompose` shell scripts in mzcompose directories"

    def run(self, args: argparse.Namespace) -> None:
        repo = mzbuild.Repository.from_arguments(ROOT, args)
        template = """#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzcompose — runs Docker Compose with Materialize customizations.

exec "$(dirname "$0")"/{}/bin/pyactivate -m materialize.cli.mzcompose "$@"
"""
        for path in repo.compositions.values():
            mzcompose_path = path / "mzcompose"
            with open(mzcompose_path, "w") as f:
                f.write(template.format(os.path.relpath(repo.root, path)))
            mzbuild.chmod_x(mzcompose_path)


class LintCommand(Command):
    name = "lint"
    help = "surface common errors in compositions"

    def run(cls, args: argparse.Namespace) -> None:
        repo = mzbuild.Repository.from_arguments(ROOT, args)
        errors = []
        for name in repo.compositions:
            errors += mzcompose.Composition.lint(repo, name)
        for error in sorted(errors):
            print(error)
        if errors:
            raise UIError("lint errors discovered")


class ListCompositionsCommand(Command):
    name = "list-compositions"
    help = "list the directories that contain compositions and their summaries"

    def run(cls, args: argparse.Namespace) -> None:
        repo = mzbuild.Repository.from_arguments(ROOT, args)
        for name, path in sorted(repo.compositions.items(), key=lambda item: item[1]):
            print(os.path.relpath(path, repo.root))
            composition = mzcompose.Composition(repo, name, munge_services=False)
            if composition.description:
                # Emit the first paragraph of the description.
                for line in composition.description.split("\n"):
                    if line.strip() == "":
                        break
                    print(f"  {line}")


class ListWorkflowsCommand(Command):
    name = "list-workflows"
    help = "list workflows in the composition"

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)
        for name in sorted(composition.workflows):
            print(name)


class DescribeCommand(Command):
    name = "describe"
    aliases = ["ls", "list"]
    help = "describe services and workflows in the composition"

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)

        workflows = []
        for name, fn in composition.workflows.items():
            workflows.append((name, inspect.getdoc(fn) or ""))
        workflows.sort()

        name_width = min(max(len(name) for name, _ in workflows), 16)

        print("Services:")
        for name in sorted(composition.compose["services"]):
            print(f"    {name}")

        print()
        print("Workflows:")
        for name, description in workflows:
            if len(name) <= name_width or not description:
                print(f"    {name: <{name_width}}    {description}")
            else:
                print(f"    {name}")
                print(f"    {' ' * name_width}    {description}")

        print()
        print(
            """For help on a specific workflow, run:

    $ ./mzcompose run WORKFLOW --help
"""
        )


class SqlCommand(Command):
    name = "sql"
    help = "connect a SQL shell to a running materialized service"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("service", metavar="SERVICE", help="the service to target")

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)

        service = composition.compose["services"].get(args.service)
        if not service:
            raise UIError(f"unknown service {args.service!r}")

        image = service["image"].split(":")[0]
        if image != "materialize/materialized":
            raise UIError(
                f"cannot connect SQL shell to non-materialized service {args.service!r}"
            )

        # Attempting to load the default port will produce a nice error message
        # if the service isn't running or isn't exposing a port.
        composition.default_port(args.service)

        deps = composition.repo.resolve_dependencies([composition.repo.images["psql"]])
        deps.acquire()
        deps["psql"].run(
            [
                "-h",
                service.get("hostname", args.service),
                "-p",
                "6875",
                "-U",
                "materialize",
                "materialize",
            ],
            docker_args=["--interactive", f"--network={composition.name}_default"],
        )


class WebCommand(Command):
    name = "web"
    help = "open a service's URL in a web browser"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("service", metavar="SERVICE", help="the service to target")

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)
        port = composition.default_port(args.service)
        url = f"http://localhost:{port}"
        print(f"Opening {url} in a web browser...")
        webbrowser.open(url)


class DockerComposeCommand(Command):
    add_help = False
    allow_unknown_arguments = True

    def __init__(
        self,
        name: str,
        help: str,
        help_epilog: Optional[str] = None,
        runs_containers: bool = False,
    ):
        self.name = name
        self.help = help
        self.help_epilog = help_epilog
        self.runs_containers = runs_containers

    def configure(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("-h", "--help", action="store_true")

    def run(self, args: argparse.Namespace) -> None:
        if args.help:
            output = self.capture(
                ["docker-compose", self.name, "--help"], stderr=subprocess.STDOUT
            )
            output = output.replace("docker-compose", "./mzcompose")
            output += "\nThis command is a wrapper around Docker Compose."
            if self.help_epilog:
                output += "\n"
                output += self.help_epilog
            print(output, file=sys.stderr)
            return

        # Make sure Docker Compose is new enough.
        output = (
            self.capture(
                ["docker-compose", "version", "--short"], stderr=subprocess.STDOUT
            )
            .strip()
            .strip("v")
        )
        version = tuple(int(i) for i in output.split("."))
        if version < MIN_COMPOSE_VERSION:
            raise UIError(
                f"unsupported docker-compose version v{output}",
                hint=f"minimum version allowed: v{'.'.join(str(p) for p in MIN_COMPOSE_VERSION)}",
            )

        composition = load_composition(args)
        ui.header("Collecting mzbuild images")
        for d in composition.dependencies:
            ui.say(d.spec())

        if self.runs_containers:
            if args.coverage:
                # If the user has requested coverage information, create the
                # coverage directory as the current user, so Docker doesn't create
                # it as root.
                (composition.path / "coverage").mkdir(exist_ok=True)
            self.check_docker_resource_limits()
            composition.dependencies.acquire()

            if "services" in composition.compose:
                composition.pull_if_variable(composition.compose["services"].keys())

        self.handle_composition(args, composition)

    def handle_composition(
        self, args: argparse.Namespace, composition: mzcompose.Composition
    ) -> None:
        ui.header("Delegating to Docker Compose")
        composition.invoke(*args.unknown_args, self.name, *args.unknown_subargs)

    def check_docker_resource_limits(self) -> None:
        output = self.capture(
            ["docker", "system", "info", "--format", "{{.MemTotal}} {{.NCPU}}"]
        )
        [mem, ncpus] = [int(field) for field in output.split()]
        if mem < RECOMMENDED_MIN_MEM:
            ui.warn(
                f"Docker only has {naturalsize(mem, binary=True)} of memory available. "
                f"We recommend at least {naturalsize(RECOMMENDED_MIN_MEM, binary=True)} of memory. "
                "See https://materialize.com/docs/third-party/docker/."
            )
        if ncpus < RECOMMENDED_MIN_CPUS:
            ui.warn(
                f"Docker only has {ncpus} CPU available. "
                f"We recommend at least {RECOMMENDED_MIN_CPUS} CPUs. "
                "See https://materialize.com/docs/third-party/docker/."
            )

    def capture(
        self, args: List[str], stderr: Union[None, int, IO[bytes]] = None
    ) -> str:
        try:
            return spawn.capture(args, stderr=stderr)
        except subprocess.CalledProcessError as e:
            # Print any captured output, since it probably hints at the problem.
            print(e.output, file=sys.stderr, end="")
            raise UIError(f"running `{args[0]}` failed (exit status {e.returncode})")
        except FileNotFoundError:
            raise UIError(
                f"unable to launch `{args[0]}`", hint=f"is {args[0]} installed?"
            )


class RunCommand(DockerComposeCommand):
    def __init__(self) -> None:
        super().__init__(
            "run",
            "run a one-off command",
            runs_containers=True,
            help_epilog="""As an mzcompose extension, run also supports running a workflow, as in:

    $ ./mzcompose run WORKFLOW [workflow-options...]

In this form, run does not accept any of the arguments listed above.

To see the available workflows, run:

    $ ./mzcompose list
""",
        )

    def configure(self, parser: argparse.ArgumentParser) -> None:
        pass

    def run(self, args: argparse.Namespace) -> Any:
        # This is a bit gross, but to determine the first position argument to
        # `run` we have no choice but to hardcode the list of `run` options that
        # take a value. E.g., in `run --entrypoint bash service`, we need to
        # return `service`, not `bash`. We also distinguish between `run --help
        # workflow` and `run workflow --help`: the former asks for help on the
        # `run` command while the latter asks for help on the named `workflow`.

        KNOWN_OPTIONS_WITH_ARGUMENT = [
            "-d",
            "--detach",
            "--name",
            "--entrypoint",
            "-e",
            "-l",
            "--label",
            "-p",
            "--publish",
            "-v",
            "--volume",
            "-w",
            "--workdir",
        ]

        setattr(args, "workflow", None)
        setattr(args, "help", False)
        arg_iter = iter(args.unknown_subargs)
        for arg in arg_iter:
            if arg in ["-h", "--help"]:
                setattr(args, "help", True)
            elif arg in KNOWN_OPTIONS_WITH_ARGUMENT:
                # This is an option that's known to take a value, so skip the
                # next argument too.
                next(arg_iter, None)
            elif arg.startswith("-"):
                # Flag option. Skip it.
                pass
            else:
                # Found a positional argument. Save it.
                setattr(args, "workflow", arg)
                break

        super().run(args)

    def handle_composition(
        self, args: argparse.Namespace, composition: mzcompose.Composition
    ) -> None:
        if args.workflow not in composition.workflows:
            # Restart any dependencies whose definitions have changed. This is
            # Docker Compose's default behavior for `up`, but not for `run`,
            # which is a constant irritation that we paper over here. The trick,
            # taken from Buildkite's Docker Compose plugin, is to run an `up`
            # command that requests zero instances of the requested service.
            if args.workflow:
                composition.invoke(
                    "up",
                    "-d",
                    "--scale",
                    f"{args.workflow}=0",
                    args.workflow,
                )
            super().handle_composition(args, composition)
        else:
            # The user has specified a workflow rather than a service. Run the
            # workflow instead of Docker Compose.
            if args.unknown_args:
                bad_arg = args.unknown_args[0]
            elif args.unknown_subargs[0].startswith("-"):
                bad_arg = args.unknown_subargs[0]
            else:
                bad_arg = None
            if bad_arg:
                raise UIError(
                    f"unknown option {bad_arg!r}",
                    hint=f"if {bad_arg!r} is a valid Docker Compose option, "
                    f"it can't be used when running {args.workflow!r}, because {args.workflow!r} "
                    "is a custom mzcompose workflow, not a Docker Compose service",
                )

            # Run the workflow inside of a test case so that we get some basic
            # test analytics, even if the workflow doesn't define more granular
            # test cases.
            with composition.test_case(f"workflow-{args.workflow}"):
                composition.workflow(args.workflow, *args.unknown_subargs[1:])

            # Upload test report to Buildkite Test Analytics.
            junit_suite = junit_xml.TestSuite(composition.name)
            for (name, result) in composition.test_results.items():
                test_case = junit_xml.TestCase(name, composition.name, result.duration)
                if result.error:
                    test_case.add_error_info(message=result.error)
                junit_suite.test_cases.append(test_case)
            junit_report = ci_util.junit_report_filename("mzcompose")
            with junit_report.open("w") as f:
                junit_xml.to_xml_report_file(f, [junit_suite])
            ci_util.upload_junit_report("mzcompose", junit_report)

            if any(result.error for result in composition.test_results.values()):
                raise UIError("at least one test case failed")


BuildCommand = DockerComposeCommand("build", "build or rebuild services")
ConfigCommand = DockerComposeCommand("config", "validate and view the Compose file")
CreateCommand = DockerComposeCommand("create", "create services", runs_containers=True)
DownCommand = DockerComposeCommand("down", "stop and remove resources")
EventsCommand = DockerComposeCommand(
    "events", "receive real time events from containers"
)
ExecCommand = DockerComposeCommand("exec", "execute a command in a running container")
ImagesCommand = DockerComposeCommand("images", "list images")
KillCommand = DockerComposeCommand("kill", "kill containers")
LogsCommand = DockerComposeCommand("logs", "view output from containers")
PauseCommand = DockerComposeCommand("pause", "pause services")
PortCommand = DockerComposeCommand("port", "print the public port for a port binding")
PsCommand = DockerComposeCommand("ps", "list containers")
PullCommand = DockerComposeCommand("pull", "pull service images")
PushCommand = DockerComposeCommand("push", "push service images")
RestartCommand = DockerComposeCommand("restart", "restart services")
RmCommand = DockerComposeCommand("rm", "remove stopped containers")
ScaleCommand = DockerComposeCommand("scale", "set number of containers for a service")
StartCommand = DockerComposeCommand("start", "start services", runs_containers=True)
StopCommand = DockerComposeCommand("stop", "stop services")
TopCommand = DockerComposeCommand("top", "display the running processes")
UnpauseCommand = DockerComposeCommand("unpause", "unpause services")
UpCommand = DockerComposeCommand(
    "up", "create and start containers", runs_containers=True
)

# The following commands are intentionally omitted:
#
#   * `help`, because it is hard to integrate help messages for our custom
#     commands. Instead we focus on making `--help` work perfectly.
#
#   * `version`, because mzcompose isn't versioned. If someone wants their
#     Docker Compose version, it's clearer to have them run
#     `docker-compose version` explicitly.


# The following `ArgumentParser` subclasses attach unknown arguments as
# `unknown_args` and `unknown_subargs` to the returned arguments object. The
# difference between unknown arguments that occur *before* the command vs. after
# (consider `./mzcompose --before command --after) is important when forwarding
# arguments to `docker-compose`.
#
# `argparse.REMAINDER` seems like it'd be useful here, but it doesn't maintain
# the above distinction, plus was deprecated in Python 3.9 due to unfixable
# bugs: https://bugs.python.org/issue17050.


class ArgumentParser(argparse.ArgumentParser):
    def parse_known_args(
        self,
        args: Optional[Sequence[Text]] = None,
        namespace: Optional[argparse.Namespace] = None,
    ) -> Tuple[argparse.Namespace, List[str]]:
        namespace, unknown_args = super().parse_known_args(args, namespace)
        setattr(namespace, "unknown_args", unknown_args)
        return namespace, []


class ArgumentSubparser(argparse.ArgumentParser):
    def parse_known_args(
        self,
        args: Optional[Sequence[Text]] = None,
        namespace: Optional[argparse.Namespace] = None,
    ) -> Tuple[argparse.Namespace, List[str]]:
        namespace, unknown_args = super().parse_known_args(args, namespace)
        setattr(namespace, "unknown_subargs", unknown_args)
        return namespace, []


if __name__ == "__main__":
    with ui.error_handler("mzcompose"):
        main(sys.argv[1:])
