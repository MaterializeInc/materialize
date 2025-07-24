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
import threading
import traceback
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
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
CI_GLUE_GLOBS = ["bin", "ci", "misc/python/materialize/cli/ci_annotate_errors.py"]

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

    bazel = pipeline.get("env", {}).get("CI_BAZEL_BUILD", 1) == 1
    bazel_lto = pipeline.get("env", {}).get("CI_BAZEL_LTO", 0) == 1

    hash_check: dict[Arch, tuple[str, bool]] = {}

    def hash(deps: mzbuild.DependencySet) -> str:
        h = hashlib.sha1()
        for dep in deps:
            h.update(dep.spec().encode())
        return h.hexdigest()

    def get_hashes(arch: Arch) -> tuple[str, bool]:
        repo = mzbuild.Repository(
            Path("."),
            arch=arch,
            coverage=args.coverage,
            sanitizer=args.sanitizer,
            bazel=bazel,
            bazel_remote_cache=bazel_remote_cache,
            bazel_lto=bazel_lto,
        )
        deps = repo.resolve_dependencies(image for image in repo if image.publish)
        check = deps.check()
        return (hash(deps), check)

    def fetch_hashes() -> None:
        for arch in [Arch.AARCH64, Arch.X86_64]:
            hash_check[arch] = get_hashes(arch)

    trim_builds_prep_thread = threading.Thread(target=fetch_hashes)
    trim_builds_prep_thread.start()

    # This has to run before other cutting steps because it depends on the id numbers
    if test_selection := os.getenv("CI_TEST_IDS"):
        trim_test_selection_id(pipeline, {int(i) for i in test_selection.split(",")})
    elif test_selection := os.getenv("CI_TEST_SELECTION"):
        trim_test_selection_name(pipeline, set(test_selection.split(",")))

    if args.pipeline == "test" and not os.getenv("CI_TEST_IDS"):
        if args.coverage or args.sanitizer != Sanitizer.none:
            print("Coverage/Sanitizer build, not trimming pipeline")
        elif os.environ["BUILDKITE_BRANCH"] == "main" or os.environ["BUILDKITE_TAG"]:
            print("On main branch or tag, so not trimming pipeline")
        elif have_paths_changed(CI_GLUE_GLOBS):
            # We still execute pipeline trimming on a copy of the pipeline to
            # protect against bugs in the pipeline trimming itself.
            print("[DRY RUN] Trimming unchanged steps from pipeline")
            print(
                "Repository glue code has changed, so the trimmed pipeline below does not apply"
            )
            trim_tests_pipeline(
                copy.deepcopy(pipeline),
                args.coverage,
                args.sanitizer,
                bazel,
                args.bazel_remote_cache,
                bazel_lto,
            )
        else:
            print("Trimming unchanged steps from pipeline")
            trim_tests_pipeline(
                pipeline,
                args.coverage,
                args.sanitizer,
                bazel,
                args.bazel_remote_cache,
                bazel_lto,
            )
    handle_sanitizer_skip(pipeline, args.sanitizer)
    increase_agents_timeouts(pipeline, args.sanitizer, args.coverage)
    prioritize_pipeline(pipeline, args.priority)
    switch_jobs_to_aws(pipeline, args.priority)
    permit_rerunning_successful_steps(pipeline)
    set_retry_on_agent_lost(pipeline)
    set_default_agents_queue(pipeline)
    set_parallelism_name(pipeline)
    check_depends_on(pipeline, args.pipeline)
    add_version_to_preflight_tests(pipeline)
    trim_builds_prep_thread.join()
    trim_builds(pipeline, hash_check)
    add_cargo_test_dependency(
        pipeline,
        args.pipeline,
        args.coverage,
        args.sanitizer,
        args.bazel_remote_cache,
        bazel_lto,
    )
    remove_dependencies_on_prs(pipeline, args.pipeline, hash_check)
    remove_mz_specific_keys(pipeline)

    print("--- Uploading new pipeline:")
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
    build_author = os.getenv("BUILDKITE_BUILD_AUTHOR")
    # use the base priority of the entire pipeline
    priority += pipeline.get("priority", 0)

    # Release results are time sensitive
    if tag.startswith("v"):
        priority += 10

    # main branch is less time sensitive than results on PRs
    if branch == "main":
        priority -= 50

    # Dependabot is less urgent than manual PRs
    if build_author == "Dependabot":
        priority -= 40

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


def handle_sanitizer_skip(pipeline: Any, sanitizer: Sanitizer) -> None:
    if sanitizer != Sanitizer.none:
        pipeline.setdefault("env", {})["CI_SANITIZER"] = sanitizer.value

        def visit(step: dict[str, Any]) -> None:
            if step.get("sanitizer") == "skip":
                step["skip"] = True

    else:

        def visit(step: dict[str, Any]) -> None:
            if step.get("sanitizer") == "only":
                step["skip"] = True

    for step in pipeline["steps"]:
        visit(step)
        if "group" in step:
            for inner_step in step.get("steps", []):
                visit(inner_step)


def increase_agents_timeouts(
    pipeline: Any, sanitizer: Sanitizer, coverage: bool
) -> None:
    if sanitizer != Sanitizer.none or os.getenv("CI_SYSTEM_PARAMETERS", "") == "random":

        def visit(step: dict[str, Any]) -> None:
            # Most sanitizer runs, as well as random permutations of system
            # parameters, are slower and need more memory. The default system
            # parameters in CI are chosen to be efficient for execution, while
            # a random permutation might take way longer and use more memory.
            if "timeout_in_minutes" in step:
                step["timeout_in_minutes"] *= 10

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
                elif agent == "hetzner-aarch64-8cpu-16gb":
                    agent = "hetzner-aarch64-16cpu-32gb"
                elif agent == "hetzner-x86-64-2cpu-4gb":
                    agent = "hetzner-x86-64-4cpu-8gb"
                elif agent == "hetzner-x86-64-4cpu-8gb":
                    agent = "hetzner-x86-64-8cpu-16gb"
                elif agent == "hetzner-x86-64-8cpu-16gb":
                    agent = "hetzner-x86-64-16cpu-32gb"
                elif agent == "hetzner-x86-64-16cpu-32gb":
                    agent = "hetzner-x86-64-dedi-16cpu-64gb"
                elif agent == "hetzner-x86-64-16cpu-64gb":
                    agent = "hetzner-x86-64-dedi-32cpu-128gb"
                elif agent == "hetzner-x86-64-dedi-32cpu-128gb":
                    agent = "hetzner-x86-64-dedi-48cpu-192gb"
                step["agents"] = {"queue": agent}

        for step in pipeline["steps"]:
            visit(step)
            # Groups can't be nested, so handle them explicitly here instead of recursing
            if "group" in step:
                for inner_step in step.get("steps", []):
                    visit(inner_step)

    if coverage:
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


def switch_jobs_to_aws(pipeline: Any, priority: int) -> None:
    """Switch jobs to AWS if Hetzner is currently overloaded"""
    # If Hetzner is entirely broken, you have to take these actions to switch everything back to AWS:
    # - CI_FORCE_SWITCH_TO_AWS env variable to 1
    # - Reconfigure the agent from hetzner-aarch64-4cpu-8gb to linux-aarch64-small in https://buildkite.com/materialize/test/settings/steps and other pipelines
    # - Reconfigure the agent from hetzner-aarch64-4cpu-8gb to linux-aarch64-small in ci/mkpipeline.sh

    stuck: set[str] = set()
    # TODO(def-): Remove me when Hetzner fixes its aarch64 availability
    stuck.update(
        [
            "hetzner-aarch64-16cpu-32gb",
            "hetzner-aarch64-8cpu-16gb",
            "hetzner-aarch64-4cpu-8gb",
            "hetzner-aarch64-2cpu-4gb",
        ]
    )

    if ui.env_is_truthy("CI_FORCE_SWITCH_TO_AWS", "0"):
        stuck = set(
            {
                "hetzner-x86-64-16cpu-32gb",
                "hetzner-x86-64-8cpu-16gb",
                "hetzner-x86-64-4cpu-8gb",
                "hetzner-x86-64-2cpu-4gb",
                "hetzner-aarch64-16cpu-32gb",
                "hetzner-aarch64-8cpu-16gb",
                "hetzner-aarch64-4cpu-8gb",
                "hetzner-aarch64-2cpu-4gb",
                "hetzner-x86-64-dedi-48cpu-192gb",
                "hetzner-x86-64-dedi-32cpu-128gb",
                "hetzner-x86-64-dedi-16cpu-64gb",
                "hetzner-x86-64-dedi-8cpu-32gb",
                "hetzner-x86-64-dedi-4cpu-16gb",
                "hetzner-x86-64-dedi-2cpu-8gb",
            }
        )
    else:
        # TODO(def-): Reenable me when Hetzner fixes its aarch64 availability
        # If priority has manually been set to be low, or on main branch, we can
        # wait for agents to become available
        # if branch == "main" or priority < 0:
        #     return

        # Consider Hetzner to be overloaded/broken when an important job is stuck waiting for an agent for > 20 minutes
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

            for build in builds:
                for job in build["jobs"]:
                    if "state" not in job:
                        continue
                    if "agent_query_rules" not in job:
                        continue
                    queue = job["agent_query_rules"][0].removeprefix("queue=")
                    if not queue.startswith("hetzner-"):
                        continue

                    if queue in stuck:
                        continue

                    if job.get("state") != "scheduled":
                        continue

                    runnable = job.get("runnable_at")
                    if not runnable or job.get("started_at"):
                        continue
                    if datetime.now(timezone.utc) - datetime.fromisoformat(
                        runnable
                    ) < timedelta(minutes=20):
                        continue

                    print(
                        f"Job {job.get('id')} ({job.get('web_url')}) with priority {priority} is runnable since {runnable} on {queue}, considering {queue} stuck"
                    )
                    stuck.add(queue)
        except Exception:
            print("switch_jobs_to_aws failed, ignoring:")
            traceback.print_exc()
            return

    if not stuck:
        return

    print(f"Queues stuck in Hetzner, switching to AWS or another arch: {stuck}")

    def visit(config: Any) -> None:
        if "agents" not in config:
            return

        agent = config["agents"].get("queue", None)
        if not agent in stuck:
            return

        if agent == "hetzner-aarch64-2cpu-4gb":
            if "hetzner-x86-64-2cpu-4gb" not in stuck:
                config["agents"]["queue"] = "hetzner-x86-64-2cpu-4gb"
                if config.get("depends_on") == "build-aarch64":
                    config["depends_on"] = "build-x86_64"
            else:
                config["agents"]["queue"] = "linux-aarch64"
        elif agent == "hetzner-aarch64-4cpu-8gb":
            if "hetzner-x86-64-4cpu-8gb" not in stuck:
                config["agents"]["queue"] = "hetzner-x86-64-4cpu-8gb"
                if config.get("depends_on") == "build-aarch64":
                    config["depends_on"] = "build-x86_64"
            else:
                config["agents"]["queue"] = "linux-aarch64"
        elif agent == "hetzner-aarch64-8cpu-16gb":
            if "hetzner-x86-64-8cpu-16gb" not in stuck:
                config["agents"]["queue"] = "hetzner-x86-64-8cpu-16gb"
                if config.get("depends_on") == "build-aarch64":
                    config["depends_on"] = "build-x86_64"
            else:
                config["agents"]["queue"] = "linux-aarch64-medium"

        elif agent == "hetzner-aarch64-16cpu-32gb":
            if "hetzner-x86-64-16cpu-32gb" not in stuck:
                config["agents"]["queue"] = "hetzner-x86-64-16cpu-32gb"
                if config.get("depends_on") == "build-aarch64":
                    config["depends_on"] = "build-x86_64"
            else:
                config["agents"]["queue"] = "linux-aarch64-medium"

        elif agent in ("hetzner-x86-64-4cpu-8gb", "hetzner-x86-64-2cpu-4gb"):
            config["agents"]["queue"] = "linux-x86_64"
        elif agent in ("hetzner-x86-64-8cpu-16gb", "hetzner-x86-64-16cpu-32gb"):
            config["agents"]["queue"] = "linux-x86_64-medium"
        elif agent == "hetzner-x86-64-dedi-2cpu-8gb":
            config["agents"]["queue"] = "linux-x86_64"
        elif agent == "hetzner-x86-64-dedi-4cpu-16gb":
            config["agents"]["queue"] = "linux-x86_64-medium"
        elif agent in (
            "hetzner-x86-64-dedi-8cpu-32gb",
            "hetzner-x86-64-dedi-16cpu-64gb",
        ):
            config["agents"]["queue"] = "linux-x86_64-large"
        elif agent in (
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


def trim_test_selection_id(pipeline: Any, step_ids_to_run: set[int]) -> None:
    for i, step in enumerate(steps(pipeline)):
        ident = step.get("id") or step.get("command")
        if (
            (i not in step_ids_to_run or len(step_ids_to_run) == 0)
            and "prompt" not in step
            and "wait" not in step
            and "group" not in step
            and ident
            not in (
                "coverage-pr-analyze",
                "analyze",
                "build-x86_64",
                "build-aarch64",
                "build-x86_64-lto",
                "build-aarch64-lto",
            )
            and not step.get("async")
        ):
            step["skip"] = True


def trim_test_selection_name(pipeline: Any, steps_to_run: set[str]) -> None:
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
                "build-x86_64-lto",
                "build-aarch64-lto",
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
    bazel_lto: bool,
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
    print("--- Resolving dependencies")
    repo = mzbuild.Repository(
        Path("."),
        coverage=coverage,
        sanitizer=sanitizer,
        bazel=bazel,
        bazel_remote_cache=bazel_remote_cache,
        bazel_lto=bazel_lto,
    )
    deps = repo.resolve_dependencies(image for image in repo)

    steps = OrderedDict()

    composition_paths: set[str] = set()

    for config in pipeline["steps"]:
        if "plugins" in config:
            for plugin in config["plugins"]:
                for plugin_name, plugin_config in plugin.items():
                    if plugin_name != "./ci/plugins/mzcompose":
                        continue
                    name = plugin_config["composition"]
                    composition_paths.add(str(repo.compositions[name]))
        if "group" in config:
            for inner_config in config.get("steps", []):
                if not "plugins" in inner_config:
                    continue
                for plugin in inner_config["plugins"]:
                    for plugin_name, plugin_config in plugin.items():
                        if plugin_name != "./ci/plugins/mzcompose":
                            continue
                        name = plugin_config["composition"]
                        composition_paths.add(str(repo.compositions[name]))

    imported_files: dict[str, list[str]] = {}

    with ThreadPoolExecutor(max_workers=len(composition_paths)) as executor:
        futures = {
            executor.submit(get_imported_files, path): path
            for path in composition_paths
        }
        for future in futures:
            path = futures[future]
            files = future.result()
            imported_files[path] = files

    compositions: dict[str, Composition] = {}

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
                        if name not in compositions:
                            compositions[name] = Composition(repo, name)
                        for dep in compositions[name].dependencies:
                            step.image_dependencies.add(dep)
                        composition_path = str(repo.compositions[name])
                        step.extra_inputs.add(composition_path)
                        # All (transitively) imported python modules are also implicitly dependencies
                        for file in imported_files[composition_path]:
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


def add_cargo_test_dependency(
    pipeline: Any,
    pipeline_name: str,
    coverage: bool,
    sanitizer: Sanitizer,
    bazel_remote_cache: str,
    bazel_lto: bool,
) -> None:
    """Cargo Test normally doesn't have to wait for the build to complete, but it requires a few images (ubuntu-base, postgres), which are rarely changed. So only add a dependency when those images are not on Dockerhub yet."""
    if pipeline_name not in ("test", "nightly"):
        return
    if ui.env_is_truthy("BUILDKITE_PULL_REQUEST") and pipeline_name == "test":
        for step in steps(pipeline):
            if step.get("id") == "cargo-test":
                step["depends_on"] = "build-x86_64"
        return

    repo = mzbuild.Repository(
        Path("."),
        arch=Arch.X86_64,
        coverage=coverage,
        sanitizer=sanitizer,
        bazel=True,
        bazel_remote_cache=bazel_remote_cache,
        bazel_lto=bazel_lto,
    )
    composition = Composition(repo, name="cargo-test")
    deps = composition.dependencies
    if deps.check():
        # We already have the dependencies available, no need to add a build dependency
        return

    for step in steps(pipeline):
        if step.get("id") == "cargo-test":
            step["depends_on"] = "build-x86_64"
        if step.get("id") == "miri-test":
            step["depends_on"] = "build-aarch64"


def remove_dependencies_on_prs(
    pipeline: Any,
    pipeline_name: str,
    hash_check: dict[Arch, tuple[str, bool]],
) -> None:
    """On PRs in test pipeline remove dependencies on the build, start up tests immediately, they keep retrying for the Docker image"""
    if pipeline_name != "test":
        return
    if not ui.env_is_truthy("BUILDKITE_PULL_REQUEST"):
        return
    for step in steps(pipeline):
        if step.get("id") in (
            "upload-debug-symbols-x86_64",
            "upload-debug-symbols-aarch64",
        ):
            continue
        if step.get("depends_on") in ("build-x86_64", "build-aarch64"):
            if step["depends_on"] == "build-x86_64" and hash_check[Arch.X86_64][1]:
                continue
            if step["depends_on"] == "build-aarch64" and hash_check[Arch.AARCH64][1]:
                continue
            step.setdefault("env", {})["CI_WAITING_FOR_BUILD"] = step["depends_on"]
            del step["depends_on"]


def trim_builds(
    pipeline: Any,
    hash_check: dict[Arch, tuple[str, bool]],
) -> None:
    """Trim unnecessary x86-64/aarch64 builds if all artifacts already exist. Also mark remaining builds with a unique concurrency group for the code state so that the same build doesn't happen multiple times."""
    for step in steps(pipeline):
        if step.get("id") in ("build-x86_64", "upload-debug-symbols-x86_64"):
            if hash_check[Arch.X86_64][1]:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = f"build-x86_64/{hash_check[Arch.X86_64][0]}"
        elif step.get("id") in ("build-aarch64", "upload-debug-symbols-aarch64"):
            if hash_check[Arch.AARCH64][1]:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = (
                    f"build-aarch64/{hash_check[Arch.AARCH64][0]}"
                )


_github_changed_files: set[str] | None = None


def have_paths_changed(globs: Iterable[str]) -> bool:
    """Reports whether the specified globs have diverged from origin/main."""
    global _github_changed_files
    try:
        if not _github_changed_files:
            head = spawn.capture(["git", "rev-parse", "HEAD"]).strip()
            headers = {"Accept": "application/vnd.github+json"}
            if token := os.getenv("GITHUB_TOKEN"):
                headers["Authorization"] = f"Bearer {token}"

            resp = requests.get(
                f"https://api.github.com/repos/materializeinc/materialize/compare/main...{head}",
                headers=headers,
            )
            resp.raise_for_status()
            _github_changed_files = {
                f["filename"] for f in resp.json().get("files", [])
            }

        for file in spawn.capture(["git", "ls-files", *globs]).splitlines():
            if file in _github_changed_files:
                return True
        return False
    except Exception as e:
        # Try locally if Github is down or the change has not been pushed yet when running locally
        print(f"Failed to get changed files from Github, running locally: {e}")

        # Make sure we have an up to date view of main.
        spawn.runv(["git", "fetch", "origin", "main"])

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


def remove_mz_specific_keys(pipeline: Any) -> None:
    """Remove the Materialize-specific keys from the configuration that are only used to inform how to trim the pipeline and for coverage runs."""
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


if __name__ == "__main__":
    sys.exit(main())
