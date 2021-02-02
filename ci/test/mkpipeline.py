# Copyright Materialize, Inc. All rights reserved.
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

from collections import OrderedDict
from materialize import mzbuild
from materialize import mzcompose
from materialize import spawn
from materialize import git
from pathlib import Path
from tempfile import TemporaryFile
from typing import Any, Iterable, List, Set, Sequence
import os
import subprocess
import sys
import yaml

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


def main() -> int:
    # Make sure we have an up to date view of main.
    spawn.runv(["git", "fetch", "origin", "main"])

    # Print out a summary of all changes.
    os.environ["GIT_PAGER"] = ""
    spawn.runv(["git", "diff", "--stat", "origin/main..."])

    with open(Path(__file__).parent / "pipeline.template.yml") as f:
        pipeline = yaml.safe_load(f)

    if os.environ["BUILDKITE_BRANCH"] == "main" or os.environ["BUILDKITE_TAG"]:
        print("On main branch or tag, so not trimming pipeline")
    elif have_paths_changed(CI_GLUE_GLOBS):
        print("Repository glue code has changed, so not trimming pipeline")
    else:
        print("--- Trimming unchanged steps from pipeline")
        trim_pipeline(pipeline)

    # Remove the Materialize-specific keys from the configuration that are
    # only used to inform how to trim the pipeline.
    for step in pipeline["steps"]:
        if "inputs" in step:
            del step["inputs"]

    f = TemporaryFile()
    yaml.dump(pipeline, f, encoding="utf-8")  # type: ignore
    f.seek(0)
    spawn.runv(["buildkite-agent", "pipeline", "upload"], stdin=f)

    return 0


class PipelineStep:
    def __init__(self, id: str):
        self.id = id
        self.extra_inputs: Set[str] = set()
        self.image_dependencies: Set[mzbuild.ResolvedImage] = set()
        self.step_dependencies: Set[str] = set()

    def inputs(self) -> Set[str]:
        inputs = set()
        inputs.update(self.extra_inputs)
        for image in self.image_dependencies:
            inputs.update(image.inputs(transitive=True))
        return inputs


def trim_pipeline(pipeline: Any) -> None:
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
    repo = mzbuild.Repository(Path("."))
    images = repo.resolve_dependencies(image for image in repo)

    steps = OrderedDict()
    for config in pipeline["steps"]:
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
                        composition = mzcompose.Composition(repo, name)
                        for image in composition.images:
                            step.image_dependencies.add(images[image.name])
                        step.extra_inputs.add(str(repo.compositions[name]))
        steps[step.id] = step

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
    pipeline["steps"] = [step for step in pipeline["steps"] if step["id"] in needed]


def have_paths_changed(globs: Iterable[str]) -> bool:
    """Reports whether the specified globs have diverged from origin/main."""
    diff = subprocess.run(
        ["git", "diff", "--no-patch", "--quiet", "origin/main...", *globs]
    )
    return diff.returncode != 0


if __name__ == "__main__":
    sys.exit(main())
