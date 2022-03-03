---
name: "Internal: release checklist"
about: >
  A tracking issue for a new release of Materialize. Contributor use only.
title: "Release: v"
labels: release-tracker
---

## Current status: 🚢 Progressing 🚢

<!--
## Current status: 🛑 Blocked 🛑
## Current status: 🚀 [Released](https://github.com/MaterializeInc/materialize/releases/tag/vX.Y.Z) 🚀
-->

## Announce the imminent release internally

- [x] Create this release issue. Bump the middle component of the version
  number (e.g., v0.11.0 -> v0.12.0) unless you are issuing an emergency patch
  release.

- [ ] Verify that there are no open [`M-release-blocker`][rel-blockers] issues.

[rel-blockers]: https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+label%3AM-release-blocker

## Release candidate

A release candidate is the Materialize codebase at a given commit, tested for
production readiness.

### Create the release candidate

- [ ] Choose the commit for the release. The commit should *always* be the last
  commit before 05:00 UTC on Wednesday, even though you won't start the release
  process until Wednesday morning in your timezone.

  From an updated `main` branch, use this command to find the release commit:

  **Note:** If you're on macOS you need to use `gdate` instead of `date` which
  you can install with `brew install coreutils`.

  ```shell
  # If you run the release on Wednesday
  git log -n1 --before $(date -Iminutes -d 'this wednesday 05:00 UTC') main

  # If you run the release after Wednesday
  git log -n1 --before $(date -Iminutes -d 'last wednesday 05:00 UTC') main
  ```

  If there are open release blockers, what to do depends on the scope of the
  fix. If the fix lands soon and is small, cherry-pick the fix onto the chosen
  commit. If the fix is delayed or large, abandon the release until next week.

- [ ] Create a new release candidate, specifying what kind of release this
  should be (see `--help` for all the release types):

  - If there are no cherry-picks you can run the following command:

    ```shell
    bin/mkrelease new-rc weekly --checkout <commit_from_previous_step>
    ```

  - Otherwise, you need to check out the commit from the previous step, run the
    appropriate `git cherry-pick` commands, and then run:

    ```shell
    bin/mkrelease new-rc weekly
    ```

- [ ] Incorporate this tag into `main`'s history by preparing dev on top of it.

  Without checking out `main`, perform:

  ```shell
  bin/mkrelease incorporate
  ```

  Open a PR with this change, and land it.

  If you have [GitHub cli][gh] this is as easy as typing the following command
  and following some prompts:

  ```console
  $ gh pr create --web
  ```

- [ ] Required reviews:

  An issue should be automatically created with unreviewed PRs linked inside of it. [This
  search should reveal it][required-reviews]. Review (or get review on) all unreviewed
  PRs, and link to that issue here. Once all PRs have been reviewed, close the issue.


[migrations]: https://github.com/MaterializeInc/materialize/commits/main/src/coord/src/catalog/migrate.rs
[required-reviews]: https://github.com/MaterializeInc/materialize/issues?q=is%3Aissue+is%3Aopen+%22required+reviews%22
[gh]: https://cli.github.com/

### Review Release Notes

Release notes should be added to the "unstable" section by engineers as they
merge PRs. Before announcing the release, the release notes team (Nikhil)
migrates the relevant release notes to the dedicated "v0.X.Y" section header,
based on which commits actually made it into the release, then reviews the list
of PRs and adds release notes for any features or bugs that were missed.

- [ ] Post the following message to the `#release` channel in slack, modifying the two
  issue links, one to point to the unreviewed PRs issue you went through above, and one
  to point to this issue:

  ```text
  @relnotes-team the release is in progress; now's a great time to verify or prepare the release notes and any announcement posts.
  * release notes: https://github.com/MaterializeInc/materialize/blob/main/doc/user/content/release-notes.md
  * All PRs in this release: https://github.com/MaterializeInc/materialize/issues/<unreviewed PRs issue>
  * Release: https://github.com/MaterializeInc/materialize/issues/<this issue>
  ```

### Test the release candidate

- [ ] [A full SQL logic test run][slts] will be automatically triggered when the [deploy
  job][] for your release has been completed. Find it [here][slts] and link to it, don't
  check this checkmark off until it has succeeded.

- [ ] [A full Nightly run][nightlies] will be automatically triggered as well. Find it
[here][nightlies] and link to it. Make sure all failures are accounted for or ask in #eng-testing .

- [ ] Wait for the [deploy job][] for the currently-releasing version tag to
  complete. Then launch the load tests using the `bin/scratch` script:

  ```
  bin/scratch create < misc/scratch/release.json
  ```

[slts]: https://buildkite.com/materialize/sql-logic-tests
[nightlies]: https://buildkite.com/materialize/nightlies
[deploy job]: https://buildkite.com/materialize/deploy
[This commit]: https://github.com/MaterializeInc/infra/commit/fd7f594d6f9fb2fda3a604f21b730f8d401fe81c

- [ ] Create the Grafana links for the load-test results using the `mkrelease
  dashboard-links` subcommand.

  Verify that each of the load tests started correctly and paste the entire
  output of the command into slack in the #release channel.

  <details>
  <summary>Example invocation and usage instructions</summary>
  The only required argument is the time that the load tests
  started, so if you started them at 10:00 AM you can enter `10:00` or
  `2021-08-30T10:00`.

  The `--env` option can also be supplied, if you are running in scratch instead
  of dev -- the future release process uses scratch, the current infra repo
  process runs in dev.

  ```console
  $ bin/mkrelease dashboard-links --env dev 10:00
  Load tests for release v0.9.2-rc1
  * chbench: https://grafana.i.mtrlz.dev/d/materialize-overview/materialize-overview-load-tests?orgId=1&from=1630418400000&to=1630512000000&var-test=chbench&var-purpose=load_test&var-env=scratch
  * kinesis: https://grafana.i.mtrlz.dev/d/materialize-overview/materialize-overview-load-tests?orgId=1&from=1630418400000&to=1630512000000&var-test=kinesis&var-purpose=load_test&var-env=scratch
  ```
  </details>

- [ ] Let the tests run for at least 24 hours, with the following success criteria:

  - [ ] chbench: The ingest rate should not be slower than the previous release.
  - [ ] perf-kinesis: The "Time behind external source" dashboard panel in Grafana should
    have remained at 0ms or similar for the entirety of the run.

- [ ] Remove all load test machines:

  ```
  bin/scratch mine --output-format csv | grep -E 'chbench|kinesis' | tail -n +2 | cut -d, -f2 | xargs bin/scratch destroy
  ```

[teleport cluster]: https://tsh.i.mtrlz.dev/cluster/tsh/nodes

## Final release

### Create Git tag

- [ ] Create the final release based on the final RC tag. For example, if there
  was only one RC, then the final RC tag would be `-rc1`.

  ```shell
  $ bin/mkrelease finish
  ```

  That will create a new branch named `continue-<version>`, the exact name will
  be output as the last line of the script.

  Open a PR with that branch, and land it. This is possible using [gh] from the
  terminal: `gh pr create --web`

### Verify the Release Build and Deploy

- [ ] Find your build in buildkite, for example
  https://buildkite.com/materialize/tests/builds?branch=v0.4.3

  Wait for the completion of the "Deploy", your tag will be listed as the branch at:
  https://buildkite.com/materialize/deploy/builds

  **NOTE:** the deploy step will appear green in the "tests" page before it is actually run,
  because the tests only mark that the async job got kicked off, so check that second link.

### Create Homebrew bottle and update tap

- [ ] Follow the instructions in [MaterializeInc/homebrew-materialize's
  CONTRIBUTING.md][homebrew-guide].

### Verify the Debian package and Docker images


- Run the following commands on a Docker-enabled machine and verify you see
  the correct version.

  - [ ]
    ```shell
    docker run --rm -i ubuntu:bionic <<EOF
    apt-get update && apt-get install -y gpg ca-certificates
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 79DEC5E1B7AE7694
    echo "deb http://apt.materialize.com/ generic main" > /etc/apt/sources.list.d/materialize.list
    apt-get update && apt-get install -y materialized
    materialized --version
    EOF
    ```
  - [ ] `docker pull materialize/materialized:latest && docker run --rm materialize/materialized:latest --version`
  - [ ] substitute in the correct version in this one: `docker run --rm materialize/materialized:v0.X.Y --version`

[bintray]: https://bintray.com/beta/#/materialize/apt/materialized

### Open a PR on the cloud repo enabling the new version

- [ ] Issue a PR to the cloud repo to bless the released version following [the
  instructions][], tag people from the
  [@MaterializeInc/cloud-deployers][deployers] team to review it.

[the instructions]: https://github.com/MaterializeInc/cloud/blob/main/doc/misc.md#blessing-a-new-materialize-release
[deployers]: https://github.com/orgs/MaterializeInc/teams/cloud-deployers/members

### Convert the GitHub Tag into a GitHub Release

- [ ] Go to [the GitHub releases][releases] page and find the tag that you
  created as part of the "Final Release" step.

  - Click "Edit" on the top right
  - Leave the name blank, so that the tag's version is used as the name.
  - Use the following contents as the description, updating the version
    number appropriately:
    ```markdown
    * [Release notes](https://materialize.com/docs/release-notes/#v0.#.#)
    * [Binary tarballs](https://materialize.com/docs/versions/)
    * [Installation instructions](https://materialize.com/docs/install/)
    * [Documentation](https://materialize.com/docs/)
    ```
    You do not need to manually add assets to the release, they will be included
    by GitHub automatically.

### Update the main branch for the next version

- [ ] On `main`, verify the various files that must be up to date:

  - Ensure that the [release notes] are up to date, including the current version.

  - Ensure that [`doc/user/config.toml`] has the correct version, as updated in the "Final
    Release / create git tag" step

  - Ping `#release` again:

    > @relnotes-team the release is complete! we can post to the community slack
    > channel and publish any appropriate blog posts

  - Post a link to the release tag in the `#general` channel, something like the
    following, substituting in the correct version for the X and Y:

    > `🎉🤘 Release v0.X.Y is complete! https://github.com/MaterializeInc/materialize/releases/tag/v0.X.Y 🤘🎉`

## Finish

- (optional) Update the current status at the top of this issue.
- Close this issue.

[`doc/user/config.toml`]: https://github.com/MaterializeInc/materialize/blob/main/doc/user/config.toml
[`LICENSE`]: https://github.com/MaterializeInc/materialize/tree/main/LICENSE
[`src/materialized/Cargo.toml`]: https://github.com/MaterializeInc/materialize/tree/main/src/materialized/Cargo.toml
[homebrew-guide]: https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md
[release notes]: https://github.com/MaterializeInc/materialize/tree/main/doc/user/content/release-notes.md
[releases]: https://github.com/MaterializeInc/materialize/releases
[v0.1.2]: https://github.com/MaterializeInc/materialize/releases/tag/v0.1.2
[schedule]: https://docs.google.com/spreadsheets/d/1kd0Tlkr-dKClFLMJuSxhXgcikiZUcKrEFwdc53CBHy0
