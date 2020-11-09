# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
from datetime import date, timedelta
from typing import Optional

import click
import semver

from materialize import errors
from materialize import git
from materialize import spawn
from materialize import ui

BIN_CARGO_TOML = "src/materialized/Cargo.toml"
LICENSE = "LICENSE"

say = ui.speaker("")


@click.command()
@click.argument("version")
@click.option(
    "-b", "--create-branch", default=None, help="Create a branch and check it out",
)
@click.option("-c", "--checkout", default=None, help="Commit or branch to check out")
@click.option("--tag/--no-tag", default=True, help="")
def main(version: str, checkout: Optional[str], create_branch: str, tag: bool) -> None:
    """Update documents for a release and create tags

    If both `-b` and `-c` are specified, the checkout happens before the branch creation,
    meaning that the new branch is created on the target of `-c`.

    For example make release::

        mkrelease -b prepare-v0.1.2 -c v0.1.1-rc1 v0.1.2-dev

    Has the same git semantics as::

        git checkout -b prepare-v0.1.2 v0.1.1-rc1

    \b
    Arguments:
        version: The version to release. The `v` prefix is optional
    """
    if git.is_dirty():
        raise errors.MzConfigurationError(
            "working directory is not clean, stash or commit your changes"
        )
        sys.exit(1)

    version = version.lstrip("v")
    the_tag = f"v{version}"
    confirm_version_is_next(version)

    if checkout is not None:
        git.checkout(checkout)
    if create_branch is not None:
        git.create_branch(create_branch)

    confirm_on_latest_rc()

    change_line(BIN_CARGO_TOML, "version", f'version = "{version}"')
    change_line(
        LICENSE,
        "Licensed Work:",
        f"Licensed Work:             Materialize Version {version}",
    )
    # Don't update the change date unless some code has changed
    if "-rc" in version or "-dev" in version:
        future = four_years_hence()
        change_line(LICENSE, "Change Date", f"Change Date:               {future}")

    say("Updating Cargo.lock")
    spawn.runv(["cargo", "check", "-p", "materialized"])
    if tag:
        git.commit_all_changed(f"release: {the_tag}")
        git.tag_annotated(the_tag)
    else:
        git.commit_all_changed(f"Prepare next phase of development: {the_tag}")

    matching = git.first_remote_matching("MaterializeInc/materialize")
    if tag:
        if matching is not None:
            if ui.confirm(f"\nWould you like me to run: git push {matching} {the_tag}"):
                spawn.runv(["git", "push", matching, the_tag])
        else:
            say("")
            say(
                f"Next step is to push {the_tag} to the MaterializeInc/materialize repo"
            )
    else:
        branch = git.rev_parse("HEAD", abbrev=True)
        say("")
        say(f"Create a PR with your branch: '{branch}'")


def change_line(fname: str, line_start: str, replacement: str) -> None:
    with open(fname, "r") as fh:
        content = fh.read().splitlines()

    changes = 0
    for i, line in enumerate(content):
        if line.startswith(line_start):
            content[i] = replacement
            changes += 1
    with open(fname, "w") as fh:
        fh.write("\n".join(content))
        fh.write("\n")

    if changes != 1:
        raise errors.MzConfigurationError(f"Found {changes} {line_start}s in {fname}")


def four_years_hence() -> str:
    today = date.today()
    try:
        future = today.replace(year=today.year + 4)
    except ValueError:
        # today must be a leap day
        future = today.replace(month=2, day=28, year=today.year + 4)
    return future.strftime("%B %d, %Y")


def confirm_version_is_next(version: str) -> None:
    """Check if the passed-in tag is the logical next tag"""
    tags = git.get_version_tags()
    latest_tag = tags[0]
    this_tag = semver.VersionInfo.parse(version)
    if this_tag.minor == latest_tag.minor:
        if (
            this_tag.patch == latest_tag.patch
            and this_tag.prerelease is not None
            and latest_tag.prerelease is not None
        ):
            # rc bump
            pass
        elif (
            this_tag.patch == latest_tag.patch + 1
            and this_tag.prerelease is not None
            and latest_tag.prerelease is None
        ):
            # first rc
            pass
        elif (
            this_tag.patch == latest_tag.patch
            and this_tag.prerelease is None
            and latest_tag.prerelease is not None
        ):
            say("Congratulations on the successful release!")
        else:
            say(f"ERROR: {this_tag} is not the next release after {latest_tag}")
            sys.exit(1)
    elif this_tag.minor == latest_tag.minor + 1 and this_tag.patch == 0:
        click.confirm("Are you sure you want to bump the minor version?", abort=True)
    else:
        click.confirm(
            f"The bump {latest_tag} -> {this_tag} is suspicious, are you sure?",
            abort=True,
        )


def confirm_on_latest_rc() -> None:
    tags = git.get_version_tags()
    latest_tag = tags[0]
    if not git.is_ancestor(f"v{latest_tag}", "HEAD"):
        ancestor_tag = git.describe()
        click.confirm(
            f"You are about to create a release based on: {ancestor_tag}\n"
            f"Which is not the latest prerelease:         v{latest_tag}\n"
            "Are you sure?",
            abort=True,
        )


if __name__ == "__main__":
    with errors.error_handler(say):
        main()
