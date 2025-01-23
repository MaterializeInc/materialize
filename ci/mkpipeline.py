# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Generator for the test CI pipeline.

This script takes pipeline.template.yml as input, possibly trims out jobs
whose inputs have not changed relative to the code on main, and uploads the
resulting pipeline to the Buildkite job that triggers this script.

On main and tags, all jobs are always run.

For details about how steps are trimmed, see the comment at the top of
pipeline.template.yml and the docstring on `trim_tests_pipeline` below.
"""

import argparse
import copy
import hashlib
import os
import subprocess
import sys
import traceback
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import yaml

from materialize import mzbuild, spawn, ui
from materialize.buildkite_insights.buildkite_api import generic_api
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition
from materialize.rustc_flags import Sanitizer
from materialize.ui import UIError
from materialize.version_list import get_previous_published_version
from materialize.xcompile import Arch

from .deploy.deploy_util import rust_version

# These paths contain "CI glue code", i.e., the code that powers CI itself,
# including this very script! All of CI implicitly depends on this code, so
# whenever it changes, we ought not trim any jobs from the pipeline in order to
# exercise as much of the glue code as possible.
#
# It's tough to track this code with any sort of fine-grained granularity, so we
# err on the side of including too much rather than too little. (For example,
# bin/resync-submodules is not presently used by CI, but it's just not worth
# trying to capture that.)
CI_GLUE_GLOBS = ["bin", "ci"]

DEFAULT_AGENT = "hetzner-aarch64-4cpu-8gb"


def steps(pipeline: Any) -> Iterator[dict[str, Any]]:
    for step in pipeline["steps"]:
        yield step
        if "group" in step:
            yield from step.get("steps", [])


def get_imported_files(composition: str) -> list[str]:
    return spawn.capture(["bin/ci-python-imports", composition]).splitlines()


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mkpipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
mkpipeline creates a Buildkite pipeline based on a template file and uploads it
so it is executed.""",
    )

    parser.add_argument("--coverage", action="store_true")
    parser.add_argument(
        "--sanitizer",
        default=Sanitizer[os.getenv("CI_SANITIZER", "none")],
        type=Sanitizer,
        choices=Sanitizer,
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=os.getenv("CI_PRIORITY", 0),
    )
    parser.add_argument("pipeline", type=str)
    parser.add_argument(
        "--bazel-remote-cache",
        default=os.getenv("CI_BAZEL_REMOTE_CACHE"),
        action="store",
    )
    args = parser.parse_args()

    print(f"Pipeline is: {args.pipeline}")

    # Make sure we have an up to date view of main.
    spawn.runv(["git", "fetch", "origin", "main"])

    # Print out a summary of all changes.
    os.environ["GIT_PAGER"] = ""
    spawn.runv(["git", "diff", "--stat", "origin/main..."])

    with open(Path(__file__).parent / args.pipeline / "pipeline.template.yml") as f:
        raw = f.read()
    raw = raw.replace("$RUST_VERSION", rust_version())

    # On 'main' or tagged branches, we use a separate remote cache that only CI can write to.
    if os.environ["BUILDKITE_BRANCH"] == "main" or os.environ["BUILDKITE_TAG"]:
        bazel_remote_cache = "https://bazel-remote-pa.dev.materialize.com"
    else:
        bazel_remote_cache = "https://bazel-remote.dev.materialize.com"
    raw = raw.replace("$BAZEL_REMOTE_CACHE", bazel_remote_cache)

    pipeline = yaml.safe_load(raw)

    if args.pipeline == "test":
        if args.coverage or args.sanitizer != Sanitizer.none:
            print("Coverage/Sanitizer build, not trimming pipeline")
        elif os.environ["BUILDKITE_BRANCH"] == "main" or os.environ["BUILDKITE_TAG"]:
            print("On main branch or tag, so not trimming pipeline")
        elif have_paths_changed(CI_GLUE_GLOBS):
            # We still execute pipeline trimming on a copy of the pipeline to
            # protect against bugs in the pipeline trimming itself.
            print("--- [DRY RUN] Trimming unchanged steps from pipeline")
            print(
                "Repository glue code has changed, so the trimmed pipeline below does not apply"
            )
            trim_tests_pipeline(
                copy.deepcopy(pipeline),
                args.coverage,
                args.sanitizer,
                ui.env_is_truthy("CI_BAZEL_BUILD", "1"),
                args.bazel_remote_cache,
            )
        else:
            print("--- Trimming unchanged steps from pipeline")
            trim_tests_pipeline(
                pipeline,
                args.coverage,
                args.sanitizer,
                ui.env_is_truthy("CI_BAZEL_BUILD", "1"),
                args.bazel_remote_cache,
            )

    if args.sanitizer != Sanitizer.none:
        pipeline.setdefault("env", {})["CI_SANITIZER"] = args.sanitizer.value

        def visit(step: dict[str, Any]) -> None:
            # ASan runs are slower ...
            if "timeout_in_minutes" in step:
                step["timeout_in_minutes"] *= 3

            # ... and need more memory:
            if "agents" in step:
                agent = step["agents"].get("queue", None)
                if agent == "linux-aarch64-small":
                    agent = "linux-aarch64"
                elif agent == "linux-aarch64":
                    agent = "linux-aarch64-medium"
                elif agent == "linux-aarch64-medium":
                    agent = "linux-aarch64-large"
                elif agent == "linux-aarch64-large":
                    agent = "builder-linux-aarch64-mem"
                elif agent == "linux-x86_64-small":
                    agent = "linux-x86_64"
                elif agent == "linux-x86_64":
                    agent = "linux-x86_64-medium"
                elif agent == "linux-x86_64-medium":
                    agent = "linux-x86_64-large"
                elif agent == "linux-x86_64-large":
                    agent = "builder-linux-x86_64"
                elif agent == "hetzner-aarch64-2cpu-4gb":
                    agent = "hetzner-aarch64-4cpu-8gb"
                elif agent == "hetzner-aarch64-4cpu-8gb":
                    agent = "hetzner-aarch64-8cpu-16gb"
                elif agent == "hetzner-x86-64-2cpu-4gb":
                    agent = "hetzner-x86-64-4cpu-8gb"
                elif agent == "hetzner-x86-64-4cpu-8gb":
                    agent = "hetzner-x86-64-8cpu-16gb"
                elif agent == "hetzner-x86-64-8cpu-16gb":
                    agent = "hetzner-x86-64-16cpu-32gb"
                elif agent == "hetzner-x86-64-16cpu-32gb":
                    agent = "hetzner-x86-64-16cpu-64gb"
                elif agent == "hetzner-x86-64-16cpu-64gb":
                    agent = "hetzner-x86-64-32cpu-128gb"
                elif agent == "hetzner-x86-64-32cpu-128gb":
                    agent = "hetzner-x86-64-48cpu-192gb"
                step["agents"] = {"queue": agent}

            if step.get("sanitizer") == "skip":
                step["skip"] = True

            # Nightly and Rust build required for sanitizers
            if step.get("id") in ("rust-build-x86_64", "rust-build-aarch64"):
                step["command"] = (
                    "bin/ci-builder run nightly bin/pyactivate -m ci.test.build"
                )

        for step in pipeline["steps"]:
            visit(step)
            # Groups can't be nested, so handle them explicitly here instead of recursing
            if "group" in step:
                for inner_step in step.get("steps", []):
                    visit(inner_step)
    else:

        def visit(step: dict[str, Any]) -> None:
            if step.get("sanitizer") == "only":
                step["skip"] = True

            # Skip the Cargo driven builds if the Sanitizers aren't specified since everything
            # relies on the Bazel builds.
            if step.get("id") in ("rust-build-x86_64", "rust-build-aarch64"):
                step["skip"] = True

        for step in pipeline["steps"]:
            visit(step)
            if "group" in step:
                for inner_step in step.get("steps", []):
                    visit(inner_step)

    if args.coverage:
        pipeline["env"]["CI_BUILDER_SCCACHE"] = 1
        pipeline["env"]["CI_COVERAGE_ENABLED"] = 1

        for step in steps(pipeline):
            # Coverage runs are slower
            if "timeout_in_minutes" in step:
                step["timeout_in_minutes"] *= 3

            if step.get("coverage") == "skip":
                step["skip"] = True
            if step.get("id") == "build-x86_64":
                step["name"] = "Build x86_64 with coverage"
            if step.get("id") == "build-aarch":
                step["name"] = "Build aarch64 with coverage"
    else:
        for step in steps(pipeline):
            if step.get("coverage") == "only":
                step["skip"] = True

    prioritize_pipeline(pipeline, args.priority)

    switch_jobs_to_aws(pipeline, args.priority)

    permit_rerunning_successful_steps(pipeline)

    set_retry_on_agent_lost(pipeline)

    set_default_agents_queue(pipeline)

    set_parallelism_name(pipeline)

    if test_selection := os.getenv("CI_TEST_SELECTION"):
        trim_test_selection(pipeline, set(test_selection.split(",")))

    check_depends_on(pipeline, args.pipeline)

    add_version_to_preflight_tests(pipeline)

    trim_builds(pipeline, args.coverage, args.sanitizer, args.bazel_remote_cache)

    # Remove the Materialize-specific keys from the configuration that are
    # only used to inform how to trim the pipeline and for coverage runs.
    for step in steps(pipeline):
        if "inputs" in step:
            del step["inputs"]
        if "coverage" in step:
            del step["coverage"]
        if "sanitizer" in step:
            del step["sanitizer"]
        if (
            "timeout_in_minutes" not in step
            and "prompt" not in step
            and "wait" not in step
            and "group" not in step
            and "trigger" not in step
            and not step.get("async")
        ):
            raise UIError(
                f"Every step should have an explicit timeout_in_minutes value, missing in: {step}"
            )

    print("Uploading new pipeline:")
    print(yaml.dump(pipeline))
    spawn.runv(
        ["buildkite-agent", "pipeline", "upload"], stdin=yaml.dump(pipeline).encode()
    )

    return 0


class PipelineStep:
    def __init__(self, id: str):
        self.id = id
        self.extra_inputs: set[str] = set()
        self.image_dependencies: set[mzbuild.ResolvedImage] = set()
        self.step_dependencies: set[str] = set()

    def inputs(self) -> set[str]:
        inputs = set()
        inputs.update(self.extra_inputs)
        for image in self.image_dependencies:
            inputs.update(image.inputs(transitive=True))
        return inputs


def prioritize_pipeline(pipeline: Any, priority: int) -> None:
    """Prioritize builds against main or release branches"""

    tag = os.environ["BUILDKITE_TAG"]
    branch = os.getenv("BUILDKITE_BRANCH")
    # use the base priority of the entire pipeline
    priority += pipeline.get("priority", 0)

    # Release results are time sensitive
    if tag.startswith("v"):
        priority += 10

    # main branch is less time sensitive than results on PRs
    if branch == "main":
        priority -= 50

    def visit(config: Any) -> None:
        # Increase priority for larger Hetzner-based tests so that they get
        # preferential treatment on the agents which also accept smaller jobs.
        agent_priority = 0
        if "agents" in config:
            agent = config["agents"].get("queue", None)
            if agent == "hetzner-aarch64-8cpu-16gb":
                agent_priority = 1
            if agent == "hetzner-aarch64-16cpu-32gb":
                agent_priority = 2
        config["priority"] = config.get("priority", 0) + priority + agent_priority

    for config in pipeline["steps"]:
        if "trigger" in config or "wait" in config:
            # Trigger and Wait steps do not allow priorities.
            continue
        if "group" in config:
            for inner_config in config.get("steps", []):
                visit(inner_config)
            continue
        visit(config)


def switch_jobs_to_aws(pipeline: Any, priority: int) -> None:
    """Switch jobs to AWS if Hetzner is currently overloaded"""

    branch = os.getenv("BUILDKITE_BRANCH")

    # If Hetzner is entirely broken, you have to take these actions to switch everything back to AWS:
    # - CI_FORCE_SWITCH_TO_AWS env variable to 1
    # - Reconfigure the agent from hetzner-aarch64-4cpu-8gb to linux-aarch64-small in https://buildkite.com/materialize/test/settings/steps and other pipelines
    # - Reconfigure the agent from hetzner-aarch64-4cpu-8gb to linux-aarch64-small in ci/mkpipeline.sh
    if not ui.env_is_truthy("CI_FORCE_SWITCH_TO_AWS", "0"):
        # If priority has manually been set to be low, or on main branch, we can
        # wait for agents to become available
        if branch == "main" or priority < 0:
            return

        # Consider Hetzner to be overloaded when at least 600 jobs exist with priority >= 0
        try:
            builds = generic_api.get_multiple(
                "builds",
                params={
                    "state[]": [
                        "creating",
                        "scheduled",
                        "running",
                        "failing",
                        "canceling",
                    ],
                },
                max_fetches=None,
            )

            num_jobs = 0
            for build in builds:
                for job in build["jobs"]:
                    if "state" not in job:
                        continue
                    if "agent_query_rules" not in job:
                        continue
                    if job["state"] in ("scheduled", "running", "assigned", "accepted"):
                        queue = job["agent_query_rules"][0].removeprefix("queue=")
                        if not queue.startswith("hetzner-"):
                            continue
                        if job.get("priority", {}).get("number", 0) < 0:
                            continue
                        num_jobs += 1
            print(f"Number of high-priority jobs on Hetzner: {num_jobs}")
            if num_jobs < 600:
                return
        except Exception:
            print("switch_jobs_to_aws failed, ignoring:")
            traceback.print_exc()
            return

    def visit(config: Any) -> None:
        if "agents" in config:
            agent = config["agents"].get("queue", None)
            if agent in ("hetzner-aarch64-4cpu-8gb", "hetzner-aarch64-2cpu-4gb"):
                config["agents"]["queue"] = "linux-aarch64-small"
            if agent == "hetzner-aarch64-8cpu-16gb":
                config["agents"]["queue"] = "linux-aarch64"
            if agent == "hetzner-aarch64-16cpu-32gb":
                config["agents"]["queue"] = "linux-aarch64-medium"
            if agent in (
                "hetzner-x86-64-4cpu-8gb",
                "hetzner-x86-64-2cpu-4gb",
                "hetzner-x86-64-dedi-2cpu-8gb",
            ):
                config["agents"]["queue"] = "linux-x86_64-small"
            if agent in ("hetzner-x86-64-8cpu-16gb", "hetzner-x86-64-dedi-4cpu-16gb"):
                config["agents"]["queue"] = "linux-x86_64"
            if agent in ("hetzner-x86-64-16cpu-32gb", "hetzner-x86-64-dedi-8cpu-32gb"):
                config["agents"]["queue"] = "linux-x86_64-medium"
            if agent == "hetzner-x86-64-dedi-16cpu-64gb":
                config["agents"]["queue"] = "linux-x86_64-large"
            if agent in (
                "hetzner-x86-64-dedi-32cpu-128gb",
                "hetzner-x86-64-dedi-48cpu-192gb",
            ):
                config["agents"]["queue"] = "builder-linux-x86_64"

    for config in pipeline["steps"]:
        if "trigger" in config or "wait" in config:
            # Trigger and Wait steps don't have agents
            continue
        if "group" in config:
            for inner_config in config.get("steps", []):
                visit(inner_config)
            continue
        visit(config)


def permit_rerunning_successful_steps(pipeline: Any) -> None:
    def visit(step: Any) -> None:
        step.setdefault("retry", {}).setdefault("manual", {}).setdefault(
            "permit_on_passed", True
        )

    for config in pipeline["steps"]:
        if "trigger" in config or "wait" in config or "block" in config:
            continue
        if "group" in config:
            for inner_config in config.get("steps", []):
                visit(inner_config)
            continue
        visit(config)


def set_retry_on_agent_lost(pipeline: Any) -> None:
    def visit(step: Any) -> None:
        step.setdefault("retry", {}).setdefault("automatic", []).extend(
            [
                {
                    "exit_status": -1,  # Connection to agent lost
                    "signal_reason": "none",
                    "limit": 2,
                },
                {
                    "signal_reason": "agent_stop",  # Stopped by OS
                    "limit": 2,
                },
                {
                    "exit_status": 128,  # Temporary Github connection issue
                    "limit": 2,
                },
            ]
        )

    for config in pipeline["steps"]:
        if "trigger" in config or "wait" in config or "block" in config:
            continue
        if "group" in config:
            for inner_config in config.get("steps", []):
                visit(inner_config)
            continue
        visit(config)


def set_default_agents_queue(pipeline: Any) -> None:
    for step in steps(pipeline):
        if (
            "agents" not in step
            and "prompt" not in step
            and "wait" not in step
            and "group" not in step
            and "trigger" not in step
        ):
            step["agents"] = {"queue": DEFAULT_AGENT}


def set_parallelism_name(pipeline: Any) -> None:
    def visit(step: Any) -> None:
        if step.get("parallelism", 1) > 1:
            step["label"] += " %N"

    for config in pipeline["steps"]:
        if "trigger" in config or "wait" in config or "block" in config:
            continue
        if "group" in config:
            for inner_config in config.get("steps", []):
                visit(inner_config)
            continue
        visit(config)


def check_depends_on(pipeline: Any, pipeline_name: str) -> None:
    if pipeline_name not in ("test", "nightly", "release-qualification"):
        return

    for step in steps(pipeline):
        # From buildkite documentation:
        # Note that a step with an explicit dependency specified with the
        # depends_on attribute will run immediately after the dependency step
        # has completed, without waiting for block or wait steps unless those
        # are also explicit dependencies.
        if step.get("id") in ("analyze", "deploy", "coverage-pr-analyze"):
            return

        if (
            "depends_on" not in step
            and "prompt" not in step
            and "wait" not in step
            and "group" not in step
        ):
            raise UIError(
                f"Every step should have an explicit depends_on value, missing in: {step}"
            )


def add_version_to_preflight_tests(pipeline: Any) -> None:
    for step in steps(pipeline):
        if step.get("id", "") in (
            "test-preflight-check-rollback",
            "nightly-preflight-check-rollback",
        ):
            current_version = MzVersion.parse_cargo()
            version = get_previous_published_version(
                current_version, previous_minor=True
            )
            step["build"]["commit"] = str(version)
            step["build"]["branch"] = str(version)


def trim_test_selection(pipeline: Any, steps_to_run: set[str]) -> None:
    for step in steps(pipeline):
        ident = step.get("id") or step.get("command")
        if (
            ident not in steps_to_run
            and "prompt" not in step
            and "wait" not in step
            and "group" not in step
            and ident
            not in (
                "coverage-pr-analyze",
                "analyze",
                "build-x86_64",
                "build-aarch64",
                "rust-build-x86_64",
                "rust-build-aarch64",
                "build-wasm",
            )
            and not step.get("async")
        ):
            step["skip"] = True


def trim_tests_pipeline(
    pipeline: Any,
    coverage: bool,
    sanitizer: Sanitizer,
    bazel: bool,
    bazel_remote_cache: str,
) -> None:
    """Trim pipeline steps whose inputs have not changed in this branch.

    Steps are assigned inputs in two ways:

      1. An explicit glob in the `inputs` key.
      2. An implicit dependency on any number of mzbuild images via the
         mzcompose plugin. Any steps which use the mzcompose plugin will
         have inputs autodiscovered based on the images used in that
         mzcompose configuration.

    A step is trimmed if a) none of its inputs have changed, and b) there are
    no other untrimmed steps that depend on it.
    """
    repo = mzbuild.Repository(
        Path("."),
        coverage=coverage,
        sanitizer=sanitizer,
        bazel=bazel,
        bazel_remote_cache=bazel_remote_cache,
    )
    deps = repo.resolve_dependencies(image for image in repo)

    steps = OrderedDict()

    def to_step(config: dict[str, Any]) -> PipelineStep | None:
        if "wait" in config or "group" in config:
            return None
        step = PipelineStep(config["id"])
        if "inputs" in config:
            for inp in config["inputs"]:
                step.extra_inputs.add(inp)
        if "depends_on" in config:
            d = config["depends_on"]
            if isinstance(d, str):
                step.step_dependencies.add(d)
            elif isinstance(d, list):
                step.step_dependencies.update(d)
            else:
                raise ValueError(f"unexpected non-str non-list for depends_on: {d}")
        if "plugins" in config:
            for plugin in config["plugins"]:
                for plugin_name, plugin_config in plugin.items():
                    if plugin_name == "./ci/plugins/mzcompose":
                        name = plugin_config["composition"]
                        composition = Composition(repo, name)
                        for dep in composition.dependencies:
                            step.image_dependencies.add(dep)
                        step.extra_inputs.add(str(repo.compositions[name]))
                        # All (transitively) imported python modules are also implicitly dependencies
                        for file in get_imported_files(str(repo.compositions[name])):
                            step.extra_inputs.add(file)
                    elif plugin_name == "./ci/plugins/cloudtest":
                        step.image_dependencies.add(deps["environmentd"])
                        step.image_dependencies.add(deps["clusterd"])

        return step

    for config in pipeline["steps"]:
        if step := to_step(config):
            steps[step.id] = step
        if "group" in config:
            for inner_config in config.get("steps", []):
                if inner_step := to_step(inner_config):
                    steps[inner_step.id] = inner_step

    # Find all the steps whose inputs have changed with respect to main.
    # We delegate this hard work to Git.
    changed = set()
    for step in steps.values():
        inputs = step.inputs()
        if not inputs:
            # No inputs means there is no way this step can be considered
            # changed, but `git diff` with no pathspecs means "diff everything",
            # not "diff nothing", so explicitly skip.
            continue
        if have_paths_changed(inputs):
            changed.add(step.id)

    # Then collect all changed steps, and all the steps that those changed steps
    # depend on.
    needed = set()

    def visit(step: PipelineStep) -> None:
        if step.id not in needed:
            needed.add(step.id)
            for d in step.step_dependencies:
                visit(steps[d])

    for step_id in changed:
        visit(steps[step_id])

    # Print decisions, for debugging.
    for step in steps.values():
        print(f'{"✓" if step.id in needed else "✗"} {step.id}')
        if step.step_dependencies:
            print("    wait:", " ".join(step.step_dependencies))
        if step.extra_inputs:
            print("    globs:", " ".join(step.extra_inputs))
        if step.image_dependencies:
            print(
                "    images:", " ".join(image.name for image in step.image_dependencies)
            )

    # Restrict the pipeline to the needed steps.
    for step in pipeline["steps"]:
        if "group" in step:
            step["steps"] = [
                inner_step
                for inner_step in step.get("steps", [])
                if inner_step.get("id") in needed
            ]

    pipeline["steps"] = [
        step
        for step in pipeline["steps"]
        if "wait" in step
        or ("group" in step and step["steps"])
        or step.get("id") in needed
    ]


def trim_builds(
    pipeline: Any,
    coverage: bool,
    sanitizer: Sanitizer,
    bazel_remote_cache: str,
) -> None:
    """Trim unnecessary x86-64/aarch64 builds if all artifacts already exist. Also mark remaining builds with a unique concurrency group for the code state so that the same build doesn't happen multiple times."""

    def get_deps(arch: Arch, bazel: bool = False) -> tuple[mzbuild.DependencySet, bool]:
        repo = mzbuild.Repository(
            Path("."),
            arch=arch,
            coverage=coverage,
            sanitizer=sanitizer,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
        )
        deps = repo.resolve_dependencies(image for image in repo if image.publish)
        check = deps.check()
        return (deps, check)

    def hash(deps: mzbuild.DependencySet) -> str:
        h = hashlib.sha1()
        for dep in deps:
            h.update(dep.spec().encode())
        return h.hexdigest()

    for step in steps(pipeline):
        if step.get("id") == "rust-build-x86_64":
            (deps, check) = get_deps(Arch.X86_64)
            if check:
                step["skip"] = True
            else:
                # Make sure that builds in different pipelines for the same
                # hash at least don't run concurrently, leading to wasted
                # resources.
                step["concurrency"] = 1
                step["concurrency_group"] = f"rust-build-x86_64/{hash(deps)}"
        elif step.get("id") == "rust-build-aarch64":
            (deps, check) = get_deps(Arch.AARCH64)
            if check:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = f"rust-build-aarch64/{hash(deps)}"
        elif step.get("id") == "build-x86_64":
            (deps, check) = get_deps(Arch.X86_64, bazel=True)
            if check:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = f"build-x86_64/{hash(deps)}"
        elif step.get("id") == "build-aarch64":
            (deps, check) = get_deps(Arch.AARCH64, bazel=True)
            if check:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = f"build-aarch64/{hash(deps)}"


def have_paths_changed(globs: Iterable[str]) -> bool:
    """Reports whether the specified globs have diverged from origin/main."""
    diff = subprocess.run(
        ["git", "diff", "--no-patch", "--quiet", "origin/main...", "--", *globs]
    )
    if diff.returncode == 0:
        return False
    elif diff.returncode == 1:
        return True
    else:
        diff.check_returncode()
        raise RuntimeError("unreachable")


if __name__ == "__main__":
    sys.exit(main())
