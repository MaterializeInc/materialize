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
# err on the side of including too much rather than too little.
CI_GLUE_GLOBS = ["bin", "ci", "misc/python/materialize/cli/ci_annotate_errors.py"]


def ci_glue_globs(current_pipeline: str) -> list[str]:
    """The glue globs relevant to `current_pipeline`, i.e. CI_GLUE_GLOBS with
    the `pipeline.template.yml` of every *other* pipeline excluded.

    A pipeline template only affects its own pipeline, so a change to, say, the
    Nightly template must not count as glue for the test pipeline and force it
    to run every step untrimmed. The current pipeline's own template stays in
    scope: a template edit can change a step's command in ways the input-based
    trimming cannot detect."""
    ci_dir = Path(__file__).parent
    return CI_GLUE_GLOBS + [
        f":(exclude)ci/{template.parent.name}/pipeline.template.yml"
        for template in sorted(ci_dir.glob("*/pipeline.template.yml"))
        if template.parent.name != current_pipeline
    ]


DEFAULT_AGENT = "hetzner-aarch64-4cpu-8gb"


def steps(pipeline: Any) -> Iterator[dict[str, Any]]:
    for step in pipeline["steps"]:
        yield step
        if "group" in step:
            yield from step.get("steps", [])


def get_imported_files(composition: str) -> list[str]:
    return spawn.capture(["bin/ci-python-imports", composition]).splitlines()


def post_ci_trigger_status() -> None:
    """Post a GitHub commit status linking to the CI trigger page for PRs."""
    if not ui.env_is_truthy("BUILDKITE_PULL_REQUEST"):
        return
    pr_number = os.environ["BUILDKITE_PULL_REQUEST"]
    token = os.getenv("GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN") or os.getenv(
        "GITHUB_TOKEN"
    )
    if not token:
        return
    commit = os.getenv("BUILDKITE_COMMIT", "")
    if not commit:
        return
    try:
        resp = requests.post(
            f"https://api.github.com/repos/MaterializeInc/materialize/statuses/{commit}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github.v3+json",
            },
            json={
                "state": "success",
                "target_url": f"https://ci.dev.materialize.com/trigger/{pr_number}",
                "description": "Trigger Nightly/Release Qualification/...",
                "context": "Additional CI runs",
            },
        )
        resp.raise_for_status()
        print("Posted CI trigger link status to GitHub")
    except Exception as e:
        print(f"Failed to post CI trigger link status: {e}")


def get_pull_request_labels() -> set[str]:
    """Return the GitHub label names on the PR being built, or an empty set when
    this is not a PR build or the lookup fails."""
    pr = os.getenv("BUILDKITE_PULL_REQUEST")
    if not pr or pr == "false":
        return set()
    try:
        headers = {"Accept": "application/vnd.github+json"}
        token = os.getenv("GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN") or os.getenv(
            "GITHUB_TOKEN"
        )
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = requests.get(
            f"https://api.github.com/repos/MaterializeInc/materialize/pulls/{pr}",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        return {label["name"] for label in resp.json().get("labels", [])}
    except Exception as e:
        print(f"Failed to get PR labels from GitHub, assuming none: {e}")
        return set()


def enable_nightly_for_labeled_pr(
    pipeline: Any, pipeline_name: str, labels: set[str]
) -> bool:
    """Activate the test pipeline's Nightly trigger step on a PR carrying the
    `ci-nightly` label. Returns whether it was activated."""
    if pipeline_name != "test" or not ui.env_is_truthy("BUILDKITE_PULL_REQUEST"):
        return False
    if "ci-nightly" not in labels:
        return False
    for step in steps(pipeline):
        if step.get("id") == "nightly":
            step.pop("if", None)
            env = step.setdefault("build", {}).setdefault("env", {})
            # Propagating the PR number flips the triggered nightly's `lto`
            # computation to False, so it builds the OPTIMIZED profile and reuses
            # the test pipeline's already-built images instead of doing a fresh,
            # far more expensive RELEASE/LTO build. Not a duplicate of any other
            # env var. Do not drop it.
            env["BUILDKITE_PULL_REQUEST"] = "$BUILDKITE_PULL_REQUEST"
            # Signals the triggered Nightly build to trim to this PR's changes.
            # The trim gates on this, not BUILDKITE_PULL_REQUEST, because a
            # manual /trigger run also sets BUILDKITE_PULL_REQUEST but must run
            # the full pipeline.
            env["CI_TRIM_PIPELINE"] = "1"
            return True
    return False


def annotate(style: str, context: str, markdown: str) -> None:
    """Post a Buildkite annotation. Best-effort: a failure here must not break
    pipeline generation."""
    try:
        spawn.runv(
            ["buildkite-agent", "annotate", f"--style={style}", f"--context={context}"],
            stdin=markdown.encode(),
        )
    except Exception as e:
        print(f"Failed to post annotation: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mkpipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
mkpipeline creates a Buildkite pipeline based on a template file and uploads it
so it is executed.""",
    )

    parser.add_argument("--dry-run", action="store_true")
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
    args = parser.parse_args()

    if not args.dry_run:
        ci_trigger_thread = threading.Thread(target=post_ci_trigger_status, daemon=True)
        ci_trigger_thread.start()

    print(f"Pipeline is: {args.pipeline}")

    with open(Path(__file__).parent / args.pipeline / "pipeline.template.yml") as f:
        raw = f.read()
    raw = raw.replace("$RUST_VERSION", rust_version())

    pipeline = yaml.safe_load(raw)

    lto = (
        pipeline.get("env", {}).get("CI_LTO", 0) == 1
        or bool(os.environ["BUILDKITE_TAG"])
        or args.pipeline == "spec-sheet"
        or (
            not ui.env_is_truthy("BUILDKITE_PULL_REQUEST")
            and args.pipeline in ("nightly", "release-qualification")
        )
        or ui.env_is_truthy("CI_RELEASE_LTO_BUILD")
    )

    hash_check: dict[Arch, tuple[str, bool]] = {}

    def hash(deps: mzbuild.DependencySet) -> str:
        h = hashlib.sha1()
        for dep in deps:
            h.update(dep.spec().encode())
        return h.hexdigest()

    def get_hashes(arch: Arch) -> tuple[str, bool]:
        repo = mzbuild.Repository(
            Path("."),
            profile=mzbuild.Profile.RELEASE if lto else mzbuild.Profile.OPTIMIZED,
            arch=arch,
            coverage=args.coverage,
            sanitizer=args.sanitizer,
        )
        deps = repo.resolve_dependencies(image for image in repo if image.publish)
        check = deps.check()
        return (hash(deps), check)

    def fetch_hashes() -> None:
        # Resolve both architectures in parallel since they are independent
        # and each involves expensive fingerprinting.
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                pool.submit(get_hashes, arch): arch
                for arch in [Arch.AARCH64, Arch.X86_64]
            }
            for future in futures:
                arch = futures[future]
                hash_check[arch] = future.result()

    trim_builds_prep_thread = threading.Thread(target=fetch_hashes)
    trim_builds_prep_thread.start()

    # This has to run before other cutting steps because it depends on the id numbers
    if test_selection := os.getenv("CI_TEST_IDS"):
        trim_test_selection_id(pipeline, {int(i) for i in test_selection.split(",")})
    elif test_selection := os.getenv("CI_TEST_SELECTION"):
        trim_test_selection_name(pipeline, set(test_selection.split(",")))

    pr_labels = get_pull_request_labels() if args.pipeline == "test" else set()

    keep_steps = (
        {"nightly"}
        if enable_nightly_for_labeled_pr(pipeline, args.pipeline, pr_labels)
        else set()
    )

    fail_build_reason = None
    if "ci-no-build" in pr_labels:
        skip_all_steps(pipeline)
        fail_build_reason = "ci-no-build"
    elif "ci-no-test" in pr_labels:
        # The build steps are exempt from this trim, so they still run.
        trim_test_selection_id(pipeline, set())
        fail_build_reason = "ci-no-test"

    # Surface label-driven changes as a Buildkite annotation, since otherwise
    # they are only visible buried in this step's log. Skipped under --dry-run
    # (the check-pipeline lint) so it doesn't annotate the lint's own build.
    if not args.dry_run:
        if fail_build_reason == "ci-no-build":
            annotate(
                "error",
                "ci-labels",
                "**`ci-no-build`** GitHub label: skipping build and all tests",
            )
        elif fail_build_reason == "ci-no-test":
            annotate(
                "error",
                "ci-labels",
                "**`ci-no-test`** GitHub label: skipping all tests",
            )
        elif "ci-no-trim" in pr_labels:
            annotate(
                "info",
                "ci-labels",
                "**`ci-no-trim`** GitHub label: not trimming unchanged steps",
            )
        if keep_steps and fail_build_reason is None:
            annotate(
                "info",
                "ci-nightly",
                "**`ci-nightly`** GitHub label: also triggering Nightly",
            )

    trim_for_pr_nightly = args.pipeline == "nightly" and ui.env_is_truthy(
        "CI_TRIM_PIPELINE"
    )
    if (
        (args.pipeline == "test" or trim_for_pr_nightly)
        and not os.getenv("CI_TEST_IDS")
        and not os.getenv("CI_TEST_SELECTION")
        and fail_build_reason is None
    ):
        if "ci-no-trim" in pr_labels:
            print("ci-no-trim label set, not trimming pipeline")
        elif args.coverage or args.sanitizer != Sanitizer.none:
            print("Coverage/Sanitizer build, not trimming pipeline")
        elif os.environ["BUILDKITE_BRANCH"] == "main" or os.environ["BUILDKITE_TAG"]:
            print("On main branch or tag, so not trimming pipeline")
        elif have_paths_changed(ci_glue_globs(args.pipeline)):
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
                lto,
                keep_steps,
            )
            trim_ci_glue_exempt_steps(pipeline)
        else:
            print("Trimming unchanged steps from pipeline")
            trim_tests_pipeline(
                pipeline,
                args.coverage,
                args.sanitizer,
                lto,
                keep_steps,
            )
    truncate_skip_length(pipeline)
    handle_sanitizer_skip(pipeline, args.sanitizer)
    increase_agents_timeouts(pipeline, args.sanitizer, args.coverage)
    prioritize_pipeline(pipeline, args.priority)
    switch_jobs_to_aws(pipeline, args.priority)
    permit_rerunning_successful_steps(pipeline)
    set_retry_on_agent_lost(pipeline)
    set_default_agents_queue(pipeline)
    unparallelize(pipeline)
    expand_parallel_concurrency_groups(pipeline)
    set_parallelism_name(pipeline)
    check_depends_on(pipeline, args.pipeline)
    add_version_to_preflight_tests(pipeline)
    move_build_to_lto(pipeline, lto)
    trim_builds_prep_thread.join()
    trim_builds(pipeline, hash_check)
    add_cargo_test_dependency(
        pipeline,
        args.pipeline,
        args.coverage,
        args.sanitizer,
        lto,
    )
    add_nightly_deploy_dependency(pipeline, args.pipeline)
    remove_dependencies_on_prs(pipeline, args.pipeline, hash_check)
    remove_mz_specific_keys(pipeline)

    print("--- Uploading new pipeline:")
    print(yaml.dump(pipeline))
    cmd = ["buildkite-agent", "pipeline", "upload"]
    if args.dry_run:
        cmd.append("--dry-run")
    spawn.runv(cmd, stdin=yaml.dump(pipeline).encode())

    if fail_build_reason is not None and not args.dry_run:
        # We return before the pipeline does its usual long-running work, so the
        # daemon thread posting the "Additional CI runs" link may not have
        # finished yet. Wait for it so the link still shows up on the skipped
        # build. Bounded so an unresponsive GitHub API cannot stall the build.
        ci_trigger_thread.join(timeout=30)
        print(f"+++ `{fail_build_reason}` GitHub label set, failing the build")
        return 1

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

    for step in steps(pipeline):
        if "trigger" in step or "wait" in step or "group" in step:
            # Trigger and Wait steps do not allow priorities.
            continue
        # Increase priority for larger Hetzner-based tests so that they get
        # preferential treatment on the agents which also accept smaller jobs.
        agent_priority = 0
        if "agents" in step:
            agent = step["agents"].get("queue", None)
            if agent == "hetzner-aarch64-8cpu-16gb":
                agent_priority = 1
            if agent == "hetzner-aarch64-16cpu-32gb":
                agent_priority = 2
        step["priority"] = step.get("priority", 0) + priority + agent_priority


def truncate_skip_length(pipeline: Any) -> None:
    for step in steps(pipeline):
        if len(str(step.get("skip", ""))) > 70:
            step["skip"] = step["skip"][:70]


def handle_sanitizer_skip(pipeline: Any, sanitizer: Sanitizer) -> None:
    if sanitizer != Sanitizer.none:
        pipeline.setdefault("env", {})["CI_SANITIZER"] = sanitizer.value

        for step in steps(pipeline):
            if step.get("sanitizer") == "skip":
                step["skip"] = True

    else:
        for step in steps(pipeline):
            if step.get("sanitizer") == "only":
                step["skip"] = True


def increase_agents_timeouts(
    pipeline: Any, sanitizer: Sanitizer, coverage: bool
) -> None:
    if sanitizer != Sanitizer.none or os.getenv("CI_SYSTEM_PARAMETERS", "") == "random":
        for step in steps(pipeline):
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
                elif agent == "hetzner-x86-64-12cpu-24gb":
                    agent = "hetzner-x86-64-dedi-16cpu-64gb"
                elif agent == "hetzner-x86-64-16cpu-32gb":
                    agent = "hetzner-x86-64-dedi-16cpu-64gb"
                elif agent == "hetzner-x86-64-16cpu-64gb":
                    agent = "hetzner-x86-64-dedi-32cpu-128gb"
                elif agent == "hetzner-x86-64-dedi-32cpu-128gb":
                    agent = "hetzner-x86-64-dedi-48cpu-192gb"
                step["agents"] = {"queue": agent}

    if coverage:
        pipeline.setdefault("env", {})["CI_COVERAGE_ENABLED"] = 1

        for step in steps(pipeline):
            # Coverage runs are slower
            if "timeout_in_minutes" in step:
                step["timeout_in_minutes"] *= 3

            if step.get("coverage") == "skip":
                step["skip"] = True
            if step.get("id") == "build-x86_64":
                step["name"] = "Build x86_64 with coverage"
            if step.get("id") == "build-aarch64":
                step["name"] = "Build aarch64 with coverage"
            if step.get("id") == "cargo-test":
                step["agents"]["queue"] = "hetzner-x86-64-dedi-32cpu-128gb"
                del step["parallelism"]
    else:
        for step in steps(pipeline):
            if step.get("coverage") == "only":
                step["skip"] = True


def rewrite_step_dependency(step: dict[str, Any], old: str, new: str) -> None:
    """Rewrite a single dependency of a step, handling both the string and
    list forms of depends_on."""
    d = step.get("depends_on")
    if d == old:
        step["depends_on"] = new
    elif isinstance(d, list):
        step["depends_on"] = [new if dep == old else dep for dep in d]


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
                "hetzner-x86-64-12cpu-24gb",
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
                    if not job.get("agent_query_rules"):
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

    for step in steps(pipeline):
        # Trigger and Wait steps don't have agents
        if "trigger" in step or "wait" in step or "group" in step:
            continue

        if "agents" not in step:
            continue

        agent = step["agents"].get("queue", None)
        if not agent in stuck:
            continue

        if agent == "hetzner-aarch64-2cpu-4gb":
            if "hetzner-x86-64-2cpu-4gb" not in stuck:
                step["agents"]["queue"] = "hetzner-x86-64-2cpu-4gb"
                rewrite_step_dependency(step, "build-aarch64", "build-x86_64")
            else:
                step["agents"]["queue"] = "linux-aarch64"
        elif agent == "hetzner-aarch64-4cpu-8gb":
            if "hetzner-x86-64-4cpu-8gb" not in stuck:
                step["agents"]["queue"] = "hetzner-x86-64-4cpu-8gb"
                rewrite_step_dependency(step, "build-aarch64", "build-x86_64")
            else:
                step["agents"]["queue"] = "linux-aarch64"
        elif agent == "hetzner-aarch64-8cpu-16gb":
            if "hetzner-x86-64-8cpu-16gb" not in stuck:
                step["agents"]["queue"] = "hetzner-x86-64-8cpu-16gb"
                rewrite_step_dependency(step, "build-aarch64", "build-x86_64")
            else:
                step["agents"]["queue"] = "linux-aarch64-medium"

        elif agent == "hetzner-aarch64-16cpu-32gb":
            if "hetzner-x86-64-12cpu-24gb" not in stuck:
                step["agents"]["queue"] = "hetzner-x86-64-12cpu-24gb"
                rewrite_step_dependency(step, "build-aarch64", "build-x86_64")
            else:
                step["agents"]["queue"] = "linux-aarch64-medium"

        elif agent in ("hetzner-x86-64-4cpu-8gb", "hetzner-x86-64-2cpu-4gb"):
            step["agents"]["queue"] = "linux-x86_64"
        elif agent in (
            "hetzner-x86-64-8cpu-16gb",
            "hetzner-x86-64-12cpu-24gb",
            "hetzner-x86-64-16cpu-32gb",
        ):
            step["agents"]["queue"] = "linux-x86_64-medium"
        elif agent == "hetzner-x86-64-dedi-2cpu-8gb":
            step["agents"]["queue"] = "linux-x86_64"
        elif agent == "hetzner-x86-64-dedi-4cpu-16gb":
            step["agents"]["queue"] = "linux-x86_64-medium"
        elif agent in (
            "hetzner-x86-64-dedi-8cpu-32gb",
            "hetzner-x86-64-dedi-16cpu-64gb",
        ):
            step["agents"]["queue"] = "linux-x86_64-large"
        elif agent in (
            "hetzner-x86-64-dedi-32cpu-128gb",
            "hetzner-x86-64-dedi-48cpu-192gb",
        ):
            step["agents"]["queue"] = "builder-linux-x86_64"


def permit_rerunning_successful_steps(pipeline: Any) -> None:
    for step in steps(pipeline):
        if "trigger" in step or "wait" in step or "group" in step or "block" in step:
            continue
        step.setdefault("retry", {}).setdefault("manual", {}).setdefault(
            "permit_on_passed", True
        )


def set_retry_on_agent_lost(pipeline: Any) -> None:
    for step in steps(pipeline):
        if "trigger" in step or "wait" in step or "group" in step or "block" in step:
            continue
        retry = step.setdefault("retry", {})
        if "automatic" in retry:
            continue
        retry.setdefault("automatic", []).extend(
            [
                {
                    "exit_status": -1,  # Agent lost or job timed out during checkout/setup
                    "limit": 2,
                },
                {
                    "signal_reason": "agent_stop",  # Stopped by OS
                    "limit": 2,
                },
                {
                    "exit_status": 128,  # Temporary Github/GHCR/DockerHub connection issue
                    "limit": 2,
                },
                {
                    "exit_status": 199,  # Rust ICE https://github.com/rust-lang/rust/issues/148581
                    "limit": 2,
                },
            ]
        )


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
    for step in steps(pipeline):
        if step.get("parallelism", 1) > 1:
            step["label"] += " %N"


def _resolve_shard_marker(value: str, index: int) -> str:
    # A template may write the Buildkite variable as `$$BUILDKITE_PARALLEL_JOB`
    # (escaped `$`, the natural form) or `$BUILDKITE_PARALLEL_JOB`; resolve both
    # to the concrete shard index.
    return value.replace("$$BUILDKITE_PARALLEL_JOB", str(index)).replace(
        "$BUILDKITE_PARALLEL_JOB", str(index)
    )


def _resolve_pool_slot_marker(value: str, slot: int) -> str:
    return value.replace("$$CI_CONCURRENCY_POOL_SLOT", str(slot)).replace(
        "$CI_CONCURRENCY_POOL_SLOT", str(slot)
    )


def expand_parallel_concurrency_groups(pipeline: Any) -> None:
    """Fan out a `parallelism` step whose concurrency group is per-shard.

    Buildkite resolves `concurrency_group` once, when the pipeline is processed --
    not per parallel job. `BUILDKITE_PARALLEL_JOB` only exists at job runtime and
    is never interpolated into `concurrency_group`. So a single `parallelism: N`
    step with `concurrency_group: "...-$$BUILDKITE_PARALLEL_JOB"` puts all N jobs
    in one group, and `concurrency: 1` then runs them serially rather than in
    parallel -- defeating the point of sharding.

    To get a distinct concurrency group per shard, we expand such a step here into
    N explicit, non-parallel steps, substituting the shard index into the group
    name and passing `BUILDKITE_PARALLEL_JOB` / `BUILDKITE_PARALLEL_JOB_COUNT` via
    `env` so the mzcompose-side sharding (`buildkite.shard_list`) and any per-shard
    resource selection keep working unchanged.

    A step may additionally declare `concurrency_pool: P` and use the
    `$$CI_CONCURRENCY_POOL_SLOT` marker in its concurrency group. The N shards
    then occupy a window of N slots out of a pool of P, rotated by N each build:
    shard I of build B gets slot `(B*N + I) % P`. Concurrent builds thus land on
    disjoint slots (up to P // N fully concurrent builds) instead of queueing on
    the previous build's groups. The slot is passed to the job via the
    `CI_CONCURRENCY_POOL_SLOT` env variable so it can select the matching
    resource (e.g. a staging account). The group name must contain nothing
    shard-specific besides the slot, so that a group uniquely identifies the
    pooled resource across builds.
    """

    def fan_out(step: dict[str, Any]) -> list[dict[str, Any]]:
        group = step.get("concurrency_group")
        pool = step.pop("concurrency_pool", None)
        if not isinstance(group, str) or (
            "BUILDKITE_PARALLEL_JOB" not in group
            and "CI_CONCURRENCY_POOL_SLOT" not in group
        ):
            return [step]
        count = step.pop("parallelism", 1)

        def slot(index: int) -> int:
            assert pool is not None
            build_number = int(os.environ.get("BUILDKITE_BUILD_NUMBER", "0"))
            return (build_number * count + index) % pool

        def resolve(value: str, index: int) -> str:
            value = _resolve_shard_marker(value, index)
            if pool is not None:
                value = _resolve_pool_slot_marker(value, slot(index))
            return value

        if count <= 1:
            # Nothing to fan out (e.g. CI_UNPARALLELIZE stripped parallelism);
            # just resolve the markers to a single concrete group.
            step["concurrency_group"] = resolve(group, 0)
            if pool is not None:
                step.setdefault("env", {})["CI_CONCURRENCY_POOL_SLOT"] = str(slot(0))
            return [step]
        shards = []
        for index in range(count):
            shard = copy.deepcopy(step)
            shard["concurrency_group"] = resolve(group, index)
            if "id" in shard:
                shard["id"] = f"{step['id']}-{index}"
            if "key" in shard:
                shard["key"] = f"{step['key']}-{index}"
            if "label" in shard:
                shard["label"] = f"{step['label']} {index + 1}/{count}"
            env = shard.setdefault("env", {})
            env["BUILDKITE_PARALLEL_JOB"] = str(index)
            env["BUILDKITE_PARALLEL_JOB_COUNT"] = str(count)
            if pool is not None:
                env["CI_CONCURRENCY_POOL_SLOT"] = str(slot(index))
            shards.append(shard)
        return shards

    def expand(step_list: list[Any]) -> list[Any]:
        expanded: list[Any] = []
        for step in step_list:
            expanded.extend(fan_out(step))
        return expanded

    pipeline["steps"] = expand(pipeline["steps"])
    for step in pipeline["steps"]:
        if "group" in step and "steps" in step:
            step["steps"] = expand(step["steps"])


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
            continue

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
                "build-x86_64-no-lto",
                "build-aarch64-no-lto",
                "build-x86_64-asan",
                "build-aarch64-asan",
                "devel-docker-tags",
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
                "build-x86_64-no-lto",
                "build-aarch64-no-lto",
                "build-x86_64-asan",
                "build-aarch64-asan",
                "devel-docker-tags",
            )
            and not step.get("async")
        ):
            step["skip"] = True


def skip_all_steps(pipeline: Any) -> None:
    for step in steps(pipeline):
        if "wait" in step or "group" in step or "prompt" in step:
            continue
        step["skip"] = True


def trim_tests_pipeline(
    pipeline: Any,
    coverage: bool,
    sanitizer: Sanitizer,
    lto: bool,
    always_keep: Iterable[str] = frozenset(),
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

    `always_keep` lists step ids to retain regardless of input changes, along
    with their transitive dependencies. Used to keep a step with no inputs of
    its own (e.g. the Nightly trigger activated for a labeled PR).
    """
    print("--- Resolving dependencies")
    repo = mzbuild.Repository(
        Path("."),
        profile=mzbuild.Profile.RELEASE if lto else mzbuild.Profile.OPTIMIZED,
        coverage=coverage,
        sanitizer=sanitizer,
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

    # Cache compositions loaded with munge_services=False to extract image
    # names from their service configs. This avoids expensive fingerprinting
    # and dependency resolution that munge_services=True triggers.
    compositions: dict[str, Composition] = {}

    def get_composition_image_deps(
        name: str,
    ) -> list[mzbuild.ResolvedImage]:
        """Get the mzbuild image dependencies for a composition without
        doing expensive fingerprinting/dependency resolution."""
        if name not in compositions:
            compositions[name] = Composition(repo, name, munge_services=False)
        comp = compositions[name]
        image_deps = []
        for _svc_name, config in comp.compose.get("services", {}).items():
            if "mzbuild" in config:
                try:
                    image_deps.append(deps[config["mzbuild"]])
                except KeyError:
                    pass
        return image_deps

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
                        for dep in get_composition_image_deps(name):
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

    for step_id in always_keep:
        if step_id in steps:
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


def add_nightly_deploy_dependency(pipeline: Any, pipeline_name: str) -> None:
    """In release builds orchestratord and terraform tests in Nightly require
    the release tag to exist already, so we have to wait for the Deploy step to
    finish before triggering Nightly. But we don't want to make the Deploy step
    synchronous in non-release builds since it would make the test run take
    significantly longer to finish."""
    tag = os.environ["BUILDKITE_TAG"]
    if pipeline_name != "test" or not tag:
        return

    previous_step: dict | None = None
    for step in steps(pipeline):
        if step.get("id") == "deploy":
            step["async"] = False
            assert previous_step and "wait" in previous_step
            previous_step["skip"] = True

        if step.get("id") == "nightly":
            step["depends_on"] = "deploy"

        previous_step = step


def add_cargo_test_dependency(
    pipeline: Any,
    pipeline_name: str,
    coverage: bool,
    sanitizer: Sanitizer,
    lto: bool,
) -> None:
    """Cargo Test normally doesn't have to wait for the build to complete, but it requires a few images (debian-base, postgres), which are rarely changed. So only add a dependency when those images are not on Dockerhub yet."""
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
        profile=mzbuild.Profile.RELEASE if lto else mzbuild.Profile.OPTIMIZED,
        coverage=coverage,
        sanitizer=sanitizer,
    )
    composition = Composition(repo, name="cargo-test")
    deps = composition.dependencies
    if deps.check():
        # We already have the dependencies available, no need to add a build dependency
        return

    for step in steps(pipeline):
        if step.get("id") in ("cargo-test", "miri-test"):
            step["depends_on"] = (
                "build-x86_64" if "x86" in step["agents"]["queue"] else "build-aarch64"
            )


def remove_dependencies_on_prs(
    pipeline: Any,
    pipeline_name: str,
    hash_check: dict[Arch, tuple[str, bool]],
) -> None:
    """On test-pipeline PRs, let single-architecture consumers retry for their image instead of waiting for its build."""
    if pipeline_name != "test":
        return
    if (
        not ui.env_is_truthy("BUILDKITE_PULL_REQUEST")
        or os.environ["BUILDKITE_TAG"]
        or ui.env_is_truthy("CI_RELEASE_LTO_BUILD")
        or os.environ["BUILDKITE_BRANCH"].startswith("dependabot/")
    ):
        return
    build_image_exists = {
        "build-x86_64": hash_check[Arch.X86_64][1],
        "build-aarch64": hash_check[Arch.AARCH64][1],
    }
    for step in steps(pipeline):
        if step.get("id") in (
            "upload-debug-symbols-x86_64",
            "upload-debug-symbols-aarch64",
        ):
            continue
        d = step.get("depends_on")
        if d is None:
            continue
        deps = [d] if isinstance(d, str) else list(d)
        build_deps = [dep for dep in deps if dep in build_image_exists]
        # CI_WAITING_FOR_BUILD tracks one build's status. A multi-architecture
        # consumer needs every build, so it cannot safely use that contract.
        if len(build_deps) > 1:
            continue
        # Drop the build dependency whose image isn't published yet, so the
        # consumer starts immediately and keeps retrying for the Docker image.
        missing_build_deps = [dep for dep in build_deps if not build_image_exists[dep]]
        if not missing_build_deps:
            continue
        waiting_for_build = missing_build_deps[0]
        step.setdefault("env", {})["CI_WAITING_FOR_BUILD"] = waiting_for_build
        remaining = [dep for dep in deps if dep != waiting_for_build]
        if remaining:
            step["depends_on"] = remaining
        else:
            del step["depends_on"]


def move_build_to_lto(pipeline: Any, lto: bool) -> None:
    if not lto:
        return
    pipeline.setdefault("env", {})["CI_LTO"] = 1
    for step in steps(pipeline):
        # Use different queue so we don't block the fast dedicated builder
        # agents with their caches, LTO builds are very slow at linking, and
        # need a lot of memory for it too
        if step.get("id") == "build-x86_64":
            step["agents"]["queue"] = "builder-linux-x86_64"
        elif step.get("id") == "build-aarch64":
            step["agents"]["queue"] = "builder-linux-aarch64-mem"


def trim_builds(
    pipeline: Any,
    hash_check: dict[Arch, tuple[str, bool]],
) -> None:
    """Trim unnecessary x86-64/aarch64 builds if all artifacts already exist. Also mark remaining builds with a unique concurrency group for the code state so that the same build doesn't happen multiple times."""
    for step in steps(pipeline):
        if step.get("id") == "build-x86_64":
            if hash_check[Arch.X86_64][1]:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = f"build-x86_64/{hash_check[Arch.X86_64][0]}"
        elif step.get("id") == "upload-debug-symbols-x86_64":
            if hash_check[Arch.X86_64][1]:
                step["skip"] = True
        elif step.get("id") == "build-aarch64":
            if hash_check[Arch.AARCH64][1]:
                step["skip"] = True
            else:
                step["concurrency"] = 1
                step["concurrency_group"] = (
                    f"build-aarch64/{hash_check[Arch.AARCH64][0]}"
                )
        elif step.get("id") == "upload-debug-symbols-aarch64":
            if hash_check[Arch.AARCH64][1]:
                step["skip"] = True
        elif step.get("id") == "devel-docker-tags":
            step["concurrency"] = 1
            step["concurrency_group"] = (
                f"devel-docker-tags/{hash_check[Arch.X86_64][0]}"
            )


_github_changed_files: set[str] | None = None


def _changed_files_from_main() -> set[str]:
    """Files that diverged from origin/main, via the GitHub compare API with a
    local git fallback."""
    try:
        head = spawn.capture(["git", "rev-parse", "HEAD"]).strip()
        headers = {"Accept": "application/vnd.github+json"}
        if token := os.getenv("GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN") or os.getenv(
            "GITHUB_TOKEN"
        ):
            headers["Authorization"] = f"Bearer {token}"

        resp = requests.get(
            f"https://api.github.com/repos/materializeinc/materialize/compare/main...{head}",
            headers=headers,
        )
        resp.raise_for_status()
        files = resp.json().get("files", [])
        # The compare API caps its file list at 300. Beyond that the list is
        # truncated, and trusting it would silently treat changed files as
        # unchanged and trim their tests, so fall back to a local diff.
        if len(files) >= 300:
            raise RuntimeError("compare API file list truncated at 300 files")
        return {f["filename"] for f in files}
    except Exception as e:
        # Try locally if Github is down or the change has not been pushed yet when running locally
        print(f"Failed to get changed files from Github, running locally: {e}")

        # Make sure we have an up to date view of main.
        command = ["git", "fetch"]
        if (
            spawn.capture(["git", "rev-parse", "--is-shallow-repository"]).strip()
            == "true"
        ):
            command.append("--unshallow")
        spawn.runv(command + ["origin", "main"])

        return set(
            spawn.capture(["git", "diff", "--name-only", "origin/main..."]).splitlines()
        )


def have_paths_changed(globs: Iterable[str]) -> bool:
    """Reports whether the specified globs have diverged from origin/main."""
    global _github_changed_files
    if _github_changed_files is None:
        _github_changed_files = _changed_files_from_main()

    for file in spawn.capture(["git", "ls-files", *globs]).splitlines():
        if file in _github_changed_files:
            return True
    return False


def trim_ci_glue_exempt_steps(pipeline: Any) -> None:
    for step in steps(pipeline):
        if not step.get("ci_glue_exempt"):
            continue
        inputs = step.get("inputs", [])
        if inputs and not have_paths_changed(inputs):
            step["skip"] = "No changes in inputs"


def remove_mz_specific_keys(pipeline: Any) -> None:
    """Remove the Materialize-specific keys from the configuration that are only used to inform how to trim the pipeline and for coverage runs."""
    for step in steps(pipeline):
        if "inputs" in step:
            del step["inputs"]
        if "coverage" in step:
            del step["coverage"]
        if "sanitizer" in step:
            del step["sanitizer"]
        if "ci_glue_exempt" in step:
            del step["ci_glue_exempt"]
        if "topics" in step:
            del step["topics"]
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


def unparallelize(pipeline: Any) -> None:
    """Removes parallelism in the test, which will run longer, but exposes some interesting parallelism in some tests."""
    if not ui.env_is_truthy("CI_UNPARALLELIZE"):
        return

    for step in steps(pipeline):
        if "parallelism" not in step:
            continue

        step["timeout_in_minutes"] *= step["parallelism"]
        del step["parallelism"]


if __name__ == "__main__":
    sys.exit(main())
