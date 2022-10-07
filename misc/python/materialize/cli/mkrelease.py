# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import concurrent.futures
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import click
import requests
from semver.version import Version

from materialize import git, spawn, ui
from materialize.ui import UIError

BIN_CARGO_TOML = "src/materialized/Cargo.toml"
LICENSE = "LICENSE"
USER_DOC_CONFIG = "doc/user/config.toml"

OPT_CREATE_BRANCH = click.option(
    "-b", "--create-branch", default=None, help="Create a branch and check it out"
)
OPT_CHECKOUT = click.option(
    "-c",
    "--checkout",
    default=None,
    help="Commit or branch to check out (before creating a new branch)",
)
OPT_AFFECT_REMOTE = click.option(
    "--affect-remote/--no-affect-remote",
    default=True,
    help="Whether or not to interact with origin at all",
)


@click.group()
def cli() -> None:
    """
    Manage the release process

    You should be interacting with this because you opened a github "release"
    issue, which has all the steps that you should take in order.

    See the <repo_root>/.github/ISSUE_TEMPLATE/release.md file for full instructions.
    """


@cli.command()
@OPT_CREATE_BRANCH
@OPT_CHECKOUT
@OPT_AFFECT_REMOTE
@click.argument(
    "level",
    type=click.Choice(["major", "weekly", "patch", "rc"]),
)
def new_rc(
    create_branch: Optional[str],
    checkout: Optional[str],
    affect_remote: bool,
    level: str,
) -> None:
    """Start a brand new release

    \b
    Arguments:
        level    Which part of the version to change:
                 * patch    - The Z in X.Y.Z
                 * weekly   - The Y in X.Y.Z
                 * major    - The X in X.Y.Z
                 * rc       - increases the N in -rcN, should only be used if
                              you need to create a second or greater release candidate
    """
    tag = get_latest_tag(fetch=True)
    tag = Version.parse("0.26.4")
    new_version = None
    if level == "rc":
        if tag.prerelease is None or not tag.prerelease.startswith("rc"):
            raise UIError("Attempted to bump an rc version without starting an RC")
        next_rc = int(tag.prerelease[2:]) + 1
        new_version = tag.replace(prerelease=f"rc{next_rc}")
    elif level == "patch":
        new_version = tag.bump_patch().replace(prerelease="rc1")
    elif level == "weekly":
        new_version = tag.bump_minor().replace(prerelease="rc1")
    elif level == "major":
        new_version = tag.bump_major().replace(prerelease="rc1")
    assert new_version is not None

    release(new_version, checkout, create_branch, True, affect_remote)


@cli.command()
@OPT_CREATE_BRANCH
@OPT_CHECKOUT
@OPT_AFFECT_REMOTE
def incorporate(
    create_branch: Optional[str], checkout: Optional[str], affect_remote: bool
) -> None:
    """Update to the next patch version  with a -dev suffix"""
    incorporate_inner(
        create_branch, checkout, affect_remote, fetch=True, is_finish=False
    )


def incorporate_inner(
    create_branch: Optional[str],
    checkout: Optional[str],
    affect_remote: bool,
    fetch: bool,
    is_finish: bool,
) -> None:

    tag = get_latest_tag(fetch=fetch)
    new_version = tag.bump_patch().replace(prerelease="dev")
    if not create_branch and not checkout:
        if is_finish:
            create_branch = f"continue-{new_version}"
        else:
            create_branch = f"prepare-{new_version}"

    release(
        new_version,
        checkout=checkout,
        create_branch=create_branch,
        tag=False,
        affect_remote=affect_remote,
    )


@cli.command()
@OPT_CREATE_BRANCH
@OPT_AFFECT_REMOTE
@OPT_CHECKOUT
def finish(
    create_branch: Optional[str], checkout: Optional[str], affect_remote: bool
) -> None:
    """Create the final non-rc tag and a branch to incorporate into the repo"""
    if checkout is not None:
        tags = git.get_version_tags()
        tag = Version.parse(checkout.lstrip("v"))
        if tag not in tags:
            click.confirm(
                f"This version: {tag} doesn't look like an existing tag, "
                "are you sure you want to create a release from it?",
                abort=True,
            )
    else:
        tag = get_latest_tag(fetch=True)
    if not tag.prerelease or not tag.prerelease.startswith("rc"):
        click.confirm(
            f"This version: {tag} doesn't look like a prerelease, "
            "are you sure you want to continue?",
            abort=True,
        )
    new_version = tag.replace(prerelease=None)
    checkout = f"v{tag}"
    release(
        new_version,
        checkout=checkout,
        create_branch=None,
        tag=True,
        affect_remote=affect_remote,
    )

    update_upgrade_tests_inner(new_version, force=False)
    checkout = None
    incorporate_inner(
        create_branch, checkout, affect_remote, fetch=False, is_finish=True
    )


@cli.command()
@click.argument("start-time")
@click.option("--env", default="scratch", type=click.Choice(["dev", "scratch"]))
def dashboard_links(start_time: str, env: str) -> None:
    """
    Create the Grafana dashboard links for the release qualification tests

    START_TIME can be either an HH:MM (in 24 hour time) in which the current
    day is assumed, or a full datetime specifier with the format YYYY-MM-DDThh:mm
    """
    try:
        start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    except Exception:
        today = date.today().isoformat()
        start_today = f"{today}T{start_time}"
        try:
            start = datetime.strptime(start_today, "%Y-%m-%dT%H:%M")
        except Exception:
            raise click.BadParameter(
                "START_TIME must be a string with format HH:MM or YYYY-mm-ddTHH:MM"
            )

    tag = get_latest_tag(fetch=False)

    end = start + timedelta(hours=26)
    time_from = int(start.timestamp()) * 1000
    time_to = int(end.timestamp()) * 1000

    template = (
        "https://grafana.i.mtrlz.dev/d/materialize-overview/materialize-overview-load-tests?"
        + "orgId=1&from={time_from}&to={time_to}&var-test={test}&var-purpose={purpose}"
        + "&var-env={env}&var-git_ref={tag}"
    )
    purpose = "load_test"

    tests = []
    for test in (
        "chbench",
        "perf-kinesis",
        "kafka-ingest-open-loop",
        "kafka-ingest-open-loop-persist",
    ):
        url = template.format(
            time_from=time_from,
            time_to=time_to,
            test=test,
            purpose=purpose,
            env=env,
            tag=tag,
        )
        tests.append((test, url))

    print(f"Load tests for release v{tag}")
    for test, url in tests:
        print(f"* {test}: {url}")


def release(
    version: Version,
    checkout: Optional[str],
    create_branch: Optional[str],
    tag: bool,
    affect_remote: bool,
) -> None:
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
    the_tag = f"v{version}"
    confirm_version_is_next(version, affect_remote)

    if checkout is not None:
        git.checkout(checkout)
    if create_branch is not None:
        git.create_branch(create_branch)

    if not version.prerelease:
        confirm_on_latest_rc(affect_remote)

    change_line(BIN_CARGO_TOML, "version", f'version = "{version}"')
    change_line(
        LICENSE,
        "Licensed Work:",
        f"Licensed Work:             Materialize Version {version}",
    )
    # Don't update the change date unless some code has changed
    if version.prerelease:
        future = four_years_hence()
        change_line(LICENSE, "Change Date", f"Change Date:               {future}")

    ui.say("Updating Cargo.lock")
    spawn.runv(["cargo", "check", "-p", "materialized"])
    spawn.runv(["cargo", "check", "-p", "materialized"])
    spawn.runv(["cargo", "check", "-p", "materialized", "--locked"])
    if tag:
        git.commit_all_changed(f"release: {the_tag}")
        git.tag_annotated(the_tag)
    else:
        git.commit_all_changed(f"Prepare next phase of development: {the_tag}")
        latest_tag = get_latest_tag(fetch=False)
        # we have made an actual release
        if latest_tag.prerelease is None and click.confirm(
            f"Update doc/user/config.toml marking v{latest_tag} as released"
        ):
            update_versions_list(latest_tag)
            git.commit_all_changed(f"Update released versions to include v{latest_tag}")

    matching = git.first_remote_matching("MaterializeInc/materialize")
    if tag:
        if matching is not None:
            spawn.runv(["git", "show", "HEAD"])
            if affect_remote and ui.confirm(
                f"\nWould you like to push the above changes as: git push {matching} {the_tag}"
            ):
                spawn.runv(["git", "push", matching, the_tag])
        else:
            ui.say("")
            ui.say(
                f"Next step is to push {the_tag} to the MaterializeInc/materialize repo"
            )
    else:
        branch = git.rev_parse("HEAD", abbrev=True)
        ui.say("")
        ui.say(f"Create a PR with your branch: '{branch}'")


def update_versions_list(released_version: Version) -> None:
    """Update the doc config with the passed-in version"""
    today = date.today().strftime("%d %B %Y")
    toml_line = (
        f'  {{ name = "v{released_version}", date = "{today}", '
        + 'targets = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", '
        + '"x86_64-apple-darwin", "aarch64-apple-darwin"] },\n'
    )
    with open(USER_DOC_CONFIG) as fh:
        docs = fh.readlines()
    wrote_line = False
    with open(USER_DOC_CONFIG, "w") as fh:
        for line in docs:
            fh.write(line)
            if line == "versions = [\n":
                fh.write(toml_line)
                wrote_line = True
    if not wrote_line:
        raise UIError("Couldn't determine where to insert new version")


@cli.command()
@click.argument("released_version", type=Version.parse, default=None)
@click.option(
    "--force",
    default=False,
    is_flag=True,
    help="Always update list of possible upgrade tests, "
    "whether or not there are any current_source files to rename",
)
def update_upgrade_tests(released_version: Optional[Version], force: bool) -> None:
    """
    Update the test/upgrade/mzcompose.yml file

    This is done automatically as part of the 'finish' step, this command only
    exists for testing or in case things go wrong.
    """
    if released_version is None:
        released_version = get_latest_tag(fetch=False)
    update_upgrade_tests_inner(released_version, force=force)


def update_upgrade_tests_inner(released_version: Version, force: bool = False) -> None:
    if released_version.prerelease is not None:
        ui.say("Not updating upgrade tests for prerelease")
        return

    upgrade_dir = Path("./test/upgrade")
    version = f"v{released_version}"
    need_upgrade = [
        str(p) for p in upgrade_dir.glob("*current_source*") if "example" not in str(p)
    ]
    if not need_upgrade and not force:
        return
    for path in need_upgrade:
        spawn.runv(["git", "mv", path, path.replace("current_source", version)])

    git.commit_all_changed(
        f"Rename {len(need_upgrade)} current_source upgrade tests to {version}"
    )


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
        raise UIError(f"Found {changes} {line_start}s in {fname}")


def four_years_hence() -> str:
    today = date.today()
    try:
        future = today.replace(year=today.year + 4)
    except ValueError:
        # today must be a leap day
        future = today.replace(month=2, day=28, year=today.year + 4)
    return future.strftime("%B %d, %Y")


def confirm_version_is_next(this_tag: Version, affect_remote: bool) -> None:
    """Check if the passed-in tag is the logical next tag"""
    latest_tag = get_latest_tag(affect_remote)
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
            ui.say("Congratulations on the successful release!")
        elif (
            this_tag.minor == latest_tag.minor
            and this_tag.patch == latest_tag.patch + 1
            and this_tag.prerelease == "dev"
        ):
            # prepare next
            pass
        else:
            ui.say(f"ERROR: {this_tag} is not the next release after {latest_tag}")
            sys.exit(1)
    elif this_tag.minor == latest_tag.minor + 1 and this_tag.patch == 0:
        click.confirm("Are you sure you want to bump the minor version?", abort=True)
    else:
        click.confirm(
            f"The bump {latest_tag} -> {this_tag} is suspicious, are you sure?",
            abort=True,
        )


def confirm_on_latest_rc(affect_remote: bool) -> None:
    """Confirm before making a release on e.g. -rc1 when -rc2 exists"""
    latest_tag = get_latest_tag(affect_remote)
    if not git.is_ancestor(f"v{latest_tag}", "HEAD"):
        ancestor_tag = git.describe()
        click.confirm(
            f"You are about to create a release based on: {ancestor_tag}\n"
            f"Which is not the latest prerelease:         v{latest_tag}\n"
            "Are you sure?",
            abort=True,
        )


def get_latest_tag(fetch: bool) -> Version:
    """Get the most recent tag in this repo"""
    tags = git.get_version_tags(fetch=fetch)
    return tags[0]


@cli.command()
@click.argument("recent-ref", required=False)
@click.argument("ancestor-ref", required=False)
def list_prs(recent_ref: Optional[str], ancestor_ref: Optional[str]) -> None:
    """
    List PRs between a range of refs

    If no refs are specified, then this will find the refs between the most
    recent tag and the previous semver tag (i.e. excluding RCs)
    """
    git.fetch()
    if recent_ref is None or ancestor_ref is None:
        tags = git.get_version_tags(fetch=False)
        if recent_ref is None:
            recent = tags[0]
            recent_ref = str(tags[0])
        else:
            recent = Version.parse(recent_ref)
        if ancestor_ref is None:
            for ref in tags[1:]:
                ancestor = ref
                if (
                    ancestor.major < recent.major
                    or ancestor.minor < recent.minor
                    or ancestor.patch < recent.patch
                ):
                    ancestor_ref = str(ref)
                    break

            ui.say(
                f"Using recent_ref={recent_ref}  ancestor_ref={ancestor_ref}",
            )

    commit_range = f"v{ancestor_ref}..v{recent_ref}"
    commits = spawn.capture(
        [
            "git",
            "log",
            "--pretty=format:%d %s",
            "--abbrev-commit",
            "--date=iso",
            commit_range,
            "--",
        ],
    )

    pattern = re.compile(r"^\s*\(refs/pullreqs/(\d+)|\(#(\d+)")
    prs = []
    found_ref = False
    for commit in commits.splitlines():
        if "build(deps)" in commit:
            continue

        match = pattern.search(commit)
        if match is not None:
            pr = match.group(1)
            if pr:
                found_ref = True
            else:
                pr = match.group(2)
            prs.append(pr)

    if not found_ref:
        ui.say(
            "WARNING: you probably don't have pullreqs configured for your repo",
        )
        ui.say(
            "Add the following line to the MaterializeInc/materialize remote section in your .git/config",
        )
        ui.say("  fetch = +refs/pull/*/head:refs/pullreqs/*")

    username = input("Enter your github username: ")
    creds_path = os.path.expanduser("~/.config/materialize/dev-tools-access-token")

    try:
        with open(creds_path) as fh:
            token = fh.read().strip()
    except FileNotFoundError:
        raise UIError(
            f"""No developer tool api token at {creds_path!r}
    please create an access token at https://github.com/settings/tokens"""
        )

    def get(pr: str) -> Any:
        return requests.get(
            f"https://{username}:{token}@api.github.com/repos/MaterializeInc/materialize/pulls/{pr}",
            headers={
                "Accept": "application/vnd.github.v3+json",
            },
        ).json()

    collected = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(get, pr): pr for pr in prs}
        for future in concurrent.futures.as_completed(futures):
            pr = futures[future]
            contents = future.result()
            try:
                url = contents["html_url"]
                title = contents["title"]
                collected.append((url, title))
            except KeyError:
                raise UIError(contents)
    for url, title in sorted(collected):
        print(url, title)


if __name__ == "__main__":
    with ui.error_handler("mkrelease"):
        cli()
