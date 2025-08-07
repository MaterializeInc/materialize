# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Buildkite utilities."""

import os
import subprocess
from collections.abc import Callable
from enum import Enum, auto
from pathlib import Path
from typing import Any, TypeVar

import yaml

from materialize import git, spawn, ui

T = TypeVar("T")


class BuildkiteEnvVar(Enum):
    # environment
    BUILDKITE_AGENT_META_DATA_AWS_INSTANCE_TYPE = auto()
    BUILDKITE_AGENT_META_DATA_INSTANCE_TYPE = auto()

    # build
    BUILDKITE_PULL_REQUEST = auto()
    BUILDKITE_BUILD_NUMBER = auto()
    BUILDKITE_BUILD_ID = auto()
    BUILDKITE_PIPELINE_DEFAULT_BRANCH = auto()
    BUILDKITE_PULL_REQUEST_BASE_BRANCH = auto()
    BUILDKITE_ORGANIZATION_SLUG = auto()
    BUILDKITE_PIPELINE_SLUG = auto()
    BUILDKITE_BRANCH = auto()
    BUILDKITE_COMMIT = auto()
    BUILDKITE_BUILD_URL = auto()

    # step
    BUILDKITE_PARALLEL_JOB = auto()
    BUILDKITE_PARALLEL_JOB_COUNT = auto()
    BUILDKITE_STEP_KEY = auto()
    # will be the same for sharded and retried build steps
    BUILDKITE_STEP_ID = auto()
    # assumed to be unique
    BUILDKITE_JOB_ID = auto()
    BUILDKITE_LABEL = auto()
    BUILDKITE_RETRY_COUNT = auto()


def get_var(var: BuildkiteEnvVar, fallback_value: Any = None) -> Any:
    return os.getenv(var.name, fallback_value)


def is_in_buildkite() -> bool:
    return ui.env_is_truthy("BUILDKITE")


def is_in_pull_request() -> bool:
    """Note that this is a heuristic."""

    if not is_in_buildkite():
        return False

    if is_pull_request_marker_set():
        return True

    if is_on_default_branch():
        return False

    if git.is_on_release_version():
        return False

    if git.contains_commit("HEAD", "main"):
        return False

    return True


def is_pull_request_marker_set() -> bool:
    # If set, this variable will contain either the ID of the pull request or the string "false".
    return get_var(BuildkiteEnvVar.BUILDKITE_PULL_REQUEST, "false") != "false"


def is_on_default_branch() -> bool:
    current_branch = get_var(BuildkiteEnvVar.BUILDKITE_BRANCH, "unknown")
    default_branch = get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_DEFAULT_BRANCH, "main")
    return current_branch == default_branch


def get_pull_request_base_branch(fallback: str = "main"):
    return get_var(BuildkiteEnvVar.BUILDKITE_PULL_REQUEST_BASE_BRANCH, fallback)


def get_pipeline_default_branch(fallback: str = "main"):
    return get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_DEFAULT_BRANCH, fallback)


def get_merge_base(url: str = "https://github.com/MaterializeInc/materialize") -> str:
    base_branch = get_pull_request_base_branch() or get_pipeline_default_branch()
    merge_base = git.get_common_ancestor_commit(
        remote=git.get_remote(url), branch=base_branch
    )
    return merge_base


def inline_link(url: str, label: str | None = None) -> str:
    """See https://buildkite.com/docs/pipelines/links-and-images-in-log-output"""
    link = f"url='{url}'"

    if label:
        link = f"{link};content='{label}'"

    # These escape codes are not supported by terminals
    return f"\033]1339;{link}\a" if is_in_buildkite() else f"{label},{url}"


def inline_image(url: str, alt: str) -> str:
    """See https://buildkite.com/docs/pipelines/links-and-images-in-log-output#images-syntax-for-inlining-images"""
    content = f"url='{url}';alt='{alt}'"
    # These escape codes are not supported by terminals
    return f"\033]1338;{content}\a" if is_in_buildkite() else f"{alt},{url}"


def find_modified_lines() -> set[tuple[str, int]]:
    """
    Find each line that has been added or modified in the current pull request.
    """
    merge_base = get_merge_base()
    print(f"Merge base: {merge_base}")
    result = spawn.capture(["git", "diff", "-U0", merge_base])

    modified_lines: set[tuple[str, int]] = set()
    file_path = None
    for line in result.splitlines():
        # +++ b/src/adapter/src/coord/command_handler.rs
        if line.startswith("+++"):
            file_path = line.removeprefix("+++ b/")
        # @@ -641,7 +640,6 @@ impl Coordinator {
        elif line.startswith("@@ "):
            # We only care about the second value ("+640,6" in the example),
            # which contains the line number and length of the modified block
            # in new code state.
            parts = line.split(" ")[2]
            if "," in parts:
                start, length = map(int, parts.split(","))
            else:
                start = int(parts)
                length = 1
            for line_nr in range(start, start + length):
                assert file_path
                modified_lines.add((file_path, line_nr))
    return modified_lines


def upload_artifact(path: Path | str, cwd: Path | None = None, quiet: bool = False):
    spawn.runv(
        [
            "buildkite-agent",
            "artifact",
            "upload",
            "--log-level",
            "fatal" if quiet else "notice",
            path,
        ],
        cwd=cwd,
    )


def get_parallelism_index() -> int:
    _validate_parallelism_configuration()
    return int(get_var(BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB, 0))


def get_parallelism_count() -> int:
    _validate_parallelism_configuration()
    return int(get_var(BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB_COUNT, 1))


def _upload_shard_info_metadata(items: list[str]) -> None:
    label = get_var(BuildkiteEnvVar.BUILDKITE_LABEL) or get_var(
        BuildkiteEnvVar.BUILDKITE_STEP_KEY
    )
    spawn.runv(
        ["buildkite-agent", "meta-data", "set", f"Shard for {label}", ", ".join(items)]
    )


def notify_qa_team_about_failure(failure: str) -> None:
    if not is_in_buildkite():
        return

    label = get_var(BuildkiteEnvVar.BUILDKITE_LABEL)
    message = f"{label}: {failure}"
    print(message)
    pipeline = {
        "notify": [
            {
                "slack": {
                    "channels": ["#team-testing-bots"],
                    "message": message,
                },
                "if": 'build.state == "passed" || build.state == "failed" || build.state == "canceled"',
            }
        ]
    }
    spawn.runv(
        ["buildkite-agent", "pipeline", "upload"], stdin=yaml.dump(pipeline).encode()
    )


def shard_list(items: list[T], to_identifier: Callable[[T], str]) -> list[T]:
    if len(items) == 0:
        return []

    parallelism_index = get_parallelism_index()
    parallelism_count = get_parallelism_count()

    if parallelism_count == 1:
        return items

    accepted_items = [
        item
        for i, item in enumerate(items)
        if i % parallelism_count == parallelism_index
    ]

    if is_in_buildkite() and accepted_items:
        _upload_shard_info_metadata(list(map(to_identifier, accepted_items)))
    return accepted_items


def _validate_parallelism_configuration() -> None:
    job_index = get_var(BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB)
    job_count = get_var(BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB_COUNT)

    if job_index is None and job_count is None:
        # OK
        return

    job_index_desc = f"${BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB.name} (= '{job_index}')"
    job_count_desc = (
        f"${BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB_COUNT.name} (= '{job_count}')"
    )
    assert (
        job_index is not None and job_count is not None
    ), f"{job_index_desc} and {job_count_desc} need to be either both specified or not specified"

    job_index = int(job_index)
    job_count = int(job_count)

    assert job_count > 0, f"{job_count_desc} not valid"
    assert (
        0 <= job_index < job_count
    ), f"{job_index_desc} out of valid range with {job_count_desc}"


def truncate_annotation_str(text: str, max_length: int = 900_000) -> str:
    # 400 Bad Request: The annotation body must be less than 1 MB
    return text if len(text) <= max_length else text[:max_length] + "..."


def get_artifact_url(artifact: dict[str, Any]) -> str:
    org = get_var(BuildkiteEnvVar.BUILDKITE_ORGANIZATION_SLUG)
    pipeline = get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_SLUG)
    build = get_var(BuildkiteEnvVar.BUILDKITE_BUILD_NUMBER)
    return f"https://buildkite.com/organizations/{org}/pipelines/{pipeline}/builds/{build}/jobs/{artifact['job_id']}/artifacts/{artifact['id']}"


def add_annotation_raw(style: str, markdown: str) -> None:
    """
    Note that this does not trim the data.
    :param markdown: must not exceed 1 MB
    """
    spawn.runv(
        [
            "buildkite-agent",
            "annotate",
            f"--style={style}",
            f"--context={os.environ['BUILDKITE_JOB_ID']}-{style}",
        ],
        stdin=markdown.encode(),
    )


def add_annotation(style: str, title: str, content: str) -> None:
    if style == "info":
        markdown = f"""<details><summary>{title}</summary>

{truncate_annotation_str(content)}
</details>"""
    else:
        markdown = f"""{title}

{truncate_annotation_str(content)}"""
    add_annotation_raw(style, markdown)


def get_job_url_from_build_url(build_url: str, build_job_id: str) -> str:
    return f"{build_url}#{build_job_id}"


def get_job_url_from_pipeline_and_build(
    pipeline: str, build_number: str | int, build_job_id: str
) -> str:
    build_url = f"https://buildkite.com/materialize/{pipeline}/builds/{build_number}"
    return get_job_url_from_build_url(build_url, build_job_id)


def is_build_failed(build: str) -> bool:
    try:
        return (
            spawn.capture(
                ["buildkite-agent", "meta-data", "get", build],
                stderr=subprocess.DEVNULL,
            )
            == "failed"
        )
    except subprocess.CalledProcessError:
        return False
