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
pipeline.template.yml and the docstring on `trim_pipeline` below.
"""

import argparse
import copy
import hashlib
import os
import subprocess
import sys
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import yaml

from materialize import mzbuild, spawn
from materialize.ci_util.trim_pipeline import permit_rerunning_successful_steps
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition
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
CI_GLUE_GLOBS = ["bin", "ci", "misc/python"]


def steps(pipeline: Any) -> Iterator[dict[str, Any]]:
    for step in pipeline["steps"]:
        yield step
        if "group" in step:
            yield from step.get("steps", [])


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mkpipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
mkpipeline creates a Buildkite pipeline based on a template file and uploads it
so it is executed.""",
    )

    parser.add_argument("--coverage", action="store_true")
    parser.add_argument("pipeline", type=str)
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
    pipeline = yaml.safe_load(raw)

    if args.pipeline == "test":
        if args.coverage:
            print("Coverage build, not trimming pipeline")
        elif os.environ["BUILDKITE_BRANCH"] == "main" or os.environ["BUILDKITE_TAG"]:
            print("On main branch or tag, so not trimming pipeline")
        elif have_paths_changed(CI_GLUE_GLOBS):
            # We still execute pipeline trimming on a copy of the pipeline to
            # protect against bugs in the pipeline trimming itself.
            print("--- [DRY RUN] Trimming unchanged steps from pipeline")
            print(
                "Repository glue code has changed, so the trimmed pipeline below does not apply"
            )
            trim_tests_pipeline(copy.deepcopy(pipeline), args.coverage)
        else:
            print("--- Trimming unchanged steps from pipeline")
            trim_tests_pipeline(pipeline, args.coverage)

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
    else:
        for step in steps(pipeline):
            if step.get("coverage") == "only":
                step["skip"] = True

    prioritize_pipeline(pipeline)

    permit_rerunning_successful_steps(pipeline)

    set_default_agents_queue(pipeline)

    if test_selection := os.getenv("CI_TEST_SELECTION"):
        trim_test_selection(pipeline, set(test_selection.split(",")))
    else:
        add_test_selection_block(pipeline, args.pipeline)

    check_depends_on(pipeline, args.pipeline)

    add_version_to_preflight_tests(pipeline)

    trim_builds(pipeline, args.coverage)

    # Remove the Materialize-specific keys from the configuration that are
    # only used to inform how to trim the pipeline and for coverage runs.
    for step in steps(pipeline):
        if "inputs" in step:
            del step["inputs"]
        if "coverage" in step:
            del step["coverage"]
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


def prioritize_pipeline(pipeline: Any) -> None:
    """Prioritize builds against main or release branches"""

    tag = os.environ["BUILDKITE_TAG"]
    priority = None

    if tag.startswith("v"):
        priority = 10

    def visit(config: Any) -> None:
        config["priority"] = config.get("priority", 0) + priority

    if priority is not None:
        for config in pipeline["steps"]:
            if "trigger" in config or "wait" in config:
                # Trigger and Wait steps do not allow priorities.
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
            step["agents"] = {"queue": "linux-aarch64-small"}


def check_depends_on(pipeline: Any, pipeline_name: str) -> None:
    if pipeline_name != "test":
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
            "tests-preflight-check-rollback",
            "nightlies-preflight-check-rollback",
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
                "build-wasm",
            )
            and not step.get("async")
        ):
            step["skip"] = True


def add_test_selection_block(pipeline: Any, pipeline_name: str) -> None:
    selection_step = {
        "prompt": "What tests would you like to run? As a convenience, leaving all tests unchecked will run all tests.",
        "blocked_state": "running",
        "fields": [
            {
                "select": "Tests",
                "key": "tests",
                "options": [],
                "multiple": True,
                "required": False,
            }
        ],
        "if": 'build.source == "ui"',
    }

    if pipeline_name == "nightly":
        selection_step["block"] = "Nightly test selection"
    elif pipeline_name == "release-qualification":
        selection_step["block"] = "Release Qualification test selection"
    else:
        return

    for step in steps(pipeline):
        if (
            "id" in step
            and step["id"] not in ("analyze", "build-x86_64")
            and "skip" not in step
        ):
            selection_step["fields"][0]["options"].append({"value": step["id"]})

    pipeline["steps"].insert(0, selection_step)


def trim_tests_pipeline(pipeline: Any, coverage: bool) -> None:
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
    repo = mzbuild.Repository(Path("."), coverage=coverage)
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


def trim_builds(pipeline: Any, coverage: bool) -> None:
    """Trim unnecessary x86-64/aarch64 builds if all artifacts already exist. Also mark remaining builds with a unique concurrency group for the code state so that the same build doesn't happen multiple times."""

    def deps_publish(arch: Arch) -> mzbuild.DependencySet:
        repo = mzbuild.Repository(Path("."), arch=arch, coverage=coverage)
        return repo.resolve_dependencies(image for image in repo if image.publish)

    def hash(deps: mzbuild.DependencySet) -> str:
        h = hashlib.sha1()
        for dep in deps:
            h.update(dep.spec().encode())
        return h.hexdigest()

    for step in steps(pipeline):
        if step.get("id") == "build-x86_64":
            deps = deps_publish(Arch.X86_64)
            if deps.check():
                step["skip"] = True
            else:
                # Make sure that builds in different pipelines for the same
                # hash at least don't run concurrently, leading to wasted
                # resources.
                step["concurrency"] = 1
                step["concurrency_group"] = f"build-x86_64/{hash(deps)}"
        if step.get("id") == "build-aarch64":
            deps = deps_publish(Arch.AARCH64)
            if deps.check():
                step["skip"] = True
            else:
                # Make sure that builds in different pipelines for the same
                # hash at least don't run concurrently, leading to wasted
                # resources.
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
