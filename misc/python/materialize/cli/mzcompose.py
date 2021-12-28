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
import os
import sys
import webbrowser
from pathlib import Path
from typing import List, Optional, Sequence, Text, Tuple

from materialize import errors, mzbuild, mzcompose, spawn, ui

announce = ui.speaker("==> ")
say = ui.speaker("")

MIN_COMPOSE_VERSION = (1, 24, 0)


def main(argv: List[str]) -> None:
    parser = ArgumentParser(
        prog="mzcompose",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
mzcompose orchestrates services defined in mzcompose.yml or mzworkflows.py.
It wraps Docker Compose to add some Materialize-specific features.""",
        epilog="""
These are only the most common options. There are additional Docker Compose
options that are also supported. Consult `docker-compose help` for the full
set.

For help on a specific command, run `mzcompose COMMAND --help`.

For additional details on mzcompose, consult doc/developer/mzbuild.md.""",
    )

    # Global arguments. For all arguments but `--mz-quiet` the `--mz` prefix
    # is accepted for backwards compatibility.
    parser.add_argument(
        "--mz-quiet",
        action="store_true",
        help="suppress Materialize-specific informational messages",
    )
    parser.add_argument(
        "--find",
        "--mz-find",
        metavar="DIR",
        help="use the mzcompose.yml file from DIR, rather than the current directory",
    )
    parser.add_argument(
        "--build-mode",
        "--mz-build-mode",
        default="release",
        choices=["dev", "release"],
        help="specify the Cargo profile to use when compiling Rust crates",
    )
    parser.add_argument(
        "--coverage",
        "--mz-coverage",
        action="store_true",
        help='emit code coverage reports to the "coverage" directory',
    )

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
    ListPortsCommand().register(parser, subparsers)
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


def load_repo(args: argparse.Namespace) -> mzbuild.Repository:
    """Load the repository in the mode specified by the command-line
    arguments."""
    root = Path(os.environ["MZ_ROOT"])
    return mzbuild.Repository(
        root,
        release_mode=(args.build_mode == "release"),
        coverage=args.coverage,
    )


def load_composition(args: argparse.Namespace) -> mzcompose.Composition:
    """Loads the composition specified by the command-line arguments."""
    repo = load_repo(args)
    try:
        return mzcompose.Composition(repo, args.find or Path.cwd().name)
    except errors.UnknownComposition:
        if args.find:
            print(f"error: unknown composition {args.find!r}", file=sys.stderr)
            print("hint: available compositions:", file=sys.stderr)
            for name in repo.compositions:
                print(f"    {name}", file=sys.stderr)
        else:
            print(
                "error: directory does not contain a mzcompose.yml or mzworkflows.py file",
                file=sys.stderr,
            )
            print(
                "hint: enter one of the following directories and run ./mzcompose:",
                file=sys.stderr,
            )
            for path in repo.compositions.values():
                print(f"    {path.relative_to(Path.cwd())}", file=sys.stderr)
        sys.exit(1)


class Command:
    """An mzcompose command."""

    name: str
    """The name of the command."""

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
            self.name, help=self.help, description=self.help, add_help=self.add_help
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
        repo = load_repo(args)
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

exec "$(dirname "$0")/{}/bin/mzcompose" "$@"
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
        repo = load_repo(args)
        errors = []
        for name in repo.compositions:
            errors += mzcompose.Composition.lint(repo, name)
        for error in sorted(errors):
            print(error)
        if errors:
            sys.exit(1)


class ListCompositionsCommand(Command):
    name = "list-compositions"
    help = "list the directories that contain compositions"

    def run(cls, args: argparse.Namespace) -> None:
        repo = load_repo(args)
        for name in sorted(repo.compositions):
            print(name)


class ListPortsCommand(Command):
    name = "list-ports"
    help = "list ports exposed by a service"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "service", metavar="SERVICE", help="the service to list ports for"
        )

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)
        for port in composition.find_host_ports(args.service):
            print(port)


class ListWorkflowsCommand(Command):
    name = "list-workflows"
    help = "list workflows in the composition"

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)
        for name in sorted(
            list(composition.yaml_workflows) + list(composition.python_funcs)
        ):
            print(name)


class WebCommand(Command):
    name = "web"
    help = "open a service's URL in a web browser"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("service", metavar="SERVICE", help="the service to target")

    def run(self, args: argparse.Namespace) -> None:
        composition = load_composition(args)
        ports = composition.find_host_ports(args.service)
        if len(ports) == 1:
            webbrowser.open(f"http://localhost:{ports[0]}")
        elif not ports:
            raise errors.MzRuntimeError(f"No running services matched {args.service!r}")
        else:
            raise errors.MzRuntimeError(
                f"Too many ports matched {args.service!r}, found: {ports}"
            )


class DockerComposeCommand(Command):
    add_help = False
    allow_unknown_arguments = True

    def __init__(
        self,
        name: str,
        help: str,
        help_epilog: Optional[str] = None,
        acquire_deps: bool = False,
    ):
        self.name = name
        self.help = help
        self.help_epilog = help_epilog
        self.acquire_deps = acquire_deps

    def configure(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("-h", "--help", action="store_true")

    def run(self, args: argparse.Namespace) -> None:
        if args.help:
            output = spawn.capture(
                ["docker-compose", self.name, "--help"], stderr_too=True, unicode=True
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
            spawn.capture(["docker-compose", "version", "--short"], unicode=True)
            .strip()
            .strip("v")
        )
        version = tuple(int(i) for i in output.split("."))
        if version < MIN_COMPOSE_VERSION:
            msg = f"Unsupported docker-compose version: {version}, min required: {MIN_COMPOSE_VERSION}"
            raise errors.MzConfigurationError(msg)

        composition = load_composition(args)
        announce("Collecting mzbuild dependencies")
        deps = composition.repo.resolve_dependencies(composition.images)
        for d in deps:
            say(d.spec())

        if self.acquire_deps:
            if args.coverage:
                # If the user has requested coverage information, create the
                # coverage directory as the current user, so Docker doesn't create
                # it as root.
                (composition.path / "coverage").mkdir(exist_ok=True)
            deps.acquire()

        self.handle_composition(args, composition)

    def handle_composition(
        self, args: argparse.Namespace, composition: mzcompose.Composition
    ) -> None:
        announce("Delegating to Docker Compose")
        proc = composition.run(
            [*args.unknown_args, self.name, *args.unknown_subargs],
            check=False,
        )
        sys.exit(proc.returncode)


class RunCommand(DockerComposeCommand):
    def __init__(self) -> None:
        super().__init__(
            "run",
            "run a one-off command",
            acquire_deps=True,
            help_epilog="""As an mzcompose extension, run also supports running a workflow, as in:

    $ ./mzcompose run WORKFLOW

In this form, run does not accept any of the arguments listed above.

To see the available workflows, run:

    $ ./mzcompose list-workflows
""",
        )

    def configure(self, parser: argparse.ArgumentParser) -> None:
        super().configure(parser)
        parser.add_argument("workflow", nargs="?")

    def handle_composition(
        self, args: argparse.Namespace, composition: mzcompose.Composition
    ) -> None:
        try:
            workflow = composition.get_workflow(args.workflow, dict(os.environ))
        except KeyError:
            # Restart any dependencies whose definitions have changed. This is
            # Docker Compose's default behavior for `up`, but not for `run`,
            # which is a constant irritation that we paper over here. The trick,
            # taken from Buildkite's Docker Compose plugin, is to run an `up`
            # command that requests zero instances of the requested service.
            if args.workflow:
                composition.run(
                    ["up", "-d", "--scale", f"{args.workflow}=0", args.workflow]
                )
            args.unknown_subargs = [args.workflow] + args.unknown_subargs
            super().handle_composition(args, composition)
        else:
            # The user has specified a workflow rather than a service. Run the
            # workflow instead of Docker Compose.
            unknown_args = [*args.unknown_args, *args.unknown_subargs]
            if unknown_args:
                raise errors.MzRuntimeError(
                    f"cannot specify option {unknown_args[0]!r} when running a workflow"
                )
            workflow.run()


BuildCommand = DockerComposeCommand("build", "build or rebuild services")
ConfigCommand = DockerComposeCommand("config", "validate and view the Compose file")
CreateCommand = DockerComposeCommand("create", "create services", acquire_deps=True)
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
StartCommand = DockerComposeCommand("start", "start services", acquire_deps=True)
StopCommand = DockerComposeCommand("stop", "stop services")
TopCommand = DockerComposeCommand("top", "display the running processes")
UnpauseCommand = DockerComposeCommand("unpause", "unpause services")
UpCommand = DockerComposeCommand("up", "create and start containers", acquire_deps=True)

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
    with errors.error_handler(lambda *args: print(*args, file=sys.stderr)):
        main(sys.argv[1:])
