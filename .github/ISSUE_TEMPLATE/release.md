---
name: "Internal: release checklist"
about: >
  A tracking issue for a new release of Materialize. Contributor use only.
---

## Current status: 🚢 Progressing 🚢

<!--
## Current status: 🛑 Blocked 🛑
## Current status: 🚀 [Released](https://github.com/MaterializeInc/materialize/releases/tag/vX.Y.Z) 🚀
-->

## Announce the imminent release internally

- [x] Create this release issue.
- [ ] Check for open blocking issues:
  - [ ] For any release, check if there are any open [`M-release-blocker`][rel-blockers]
    issues or PRs
  - [ ] If this is a major or minor release (any X.Y release) check if there are open
    issue or PRs with the [`M-milestone-blocker`][blocker-search] label, include them in
    a list here, or state that there are none:

  > unknown number of milestone blockers or release blockers
- [ ] Link to this issue in the #release Slack channel and be sure to directly notify the @relnotes-team.

  If there are open blockers, clarify if they should block this release until they're
  merged when you link to this issue.

[rel-blockers]: https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+label%3AM-release-blocker
[blocker-search]: https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+label%3AM-milestone-blocker

## Release candidate

A release candidate is the Materialize codebase at a given commit, tested for
production readiness.

### Create the release candidate

- [ ] Choose the commit for the release. Unless there are open release blockers above,
  this should just be the head commit of the main branch. Checkout that commit.
- [ ] Choose the name for the release candidate following the format
  `v<VERSION>-rc<N>`, where _N_ starts at 1. For example, the first RC for
  v0.2.3 would be called v0.2.3-rc1. Use that to run the `bin/mkrelease`
  script:

  ```shell
  tag=v<VERSION>-rc<N>
  bin/mkrelease -b rel-$tag $tag
  ```

- [ ] Incorporate this tag into `main`'s history by preparing dev on top of it.

  Without checking out main to `main`, perform:

  ```shell
  next=v<NEXT_VERSION>-dev
  bin/mkrelease --no-tag -b prepare-$next $next
  ```

  > **NOTE:** `NEXT_VERSION` should always be a patch-level change, even if the next release is
  > anticipated to be a minor or major version bump.
  >
  > For example, if the version being released is `v0.5.9`, `<NEXT_VERSION>` should always be
  > `0.5.10`, not `0.6.0`, even if we expect that to be the true next version.
  >
  > The rationale is that minor (and major) versions are feature releases and may slip. If folks
  > are using a development release we expect that they will be less surprised to go from
  > `v0.5.10-dev` -> `v0.6.0` when they upgrade than upgrading and going from `v0.6.0-dev` ->
  > `v0.5.10`.

  - [ ] Open a PR with this change, and land it.

    If you have [GitHub cli][gh] this is as easy as typing the following command and following
    some prompts:

    ```console
    $ gh pr create --web
    ```

[gh]: https://cli.github.com/

### Review Release Notes

Release notes should be updated by engineers as they merge PRs. The release notes
team is responsible for reviewing release notes and the release announcement before
a release is published.

- [ ] Comment on this issue and ping the @MaterializeInc/release-notes team to
  remind them to review the [release notes][] and the release announcement. All
  members of the team should leave a comment stating that the release looks
  good.

### Test the release candidate

Cloud engineers run many of the steps in this section, their steps are prefixed with **cloud
engineer**.

To run the required load tests on the release candidate tag, Materialize employees
can follow [these instructions for running semi-automatic load tests][load-instr]
in the infrastructure repository. All of these tests can be run in parallel.

[load-instr]: https://github.com/MaterializeInc/infra/blob/main/doc/starting-a-load-test.md

- [ ] Kick off a [full SQL logic test run](https://buildkite.com/materialize/sql-logic-tests)
  by clicking the 'New Build' button. For the values requested, use the following:

    - Message: Leave blank
    - Commit - Use the default `HEAD`
    - Branch - Enter the release candidate tag (i.e. `v0.4.2-rc1`)

  You can continue on to the next step, but remember to verify that this test passes. Link to it
  here, and don't check this off until you've verified that it passed:

  - [ ] "link to test run"

- [ ] **cloud engineer** Start the load tests according to [the same instructions][load-instr],
  using your release tag as the `git-ref` value for the release benchmarks. You can use [This
  commit][] as an example to follow.

[This commit]: https://github.com/MaterializeInc/infra/commit/fd7f594d6f9fb2fda3a604f21b730f8d401fe81c

- [ ] **cloud engineer** Find the load tests in
  https://grafana.i.mtrlz.dev/d/materialize-overview, and link to them in #release, validating
  that data is present. Note that the default view of that dashboard is of a full day, so it may
  look like the test started and aborted suddenly:

  - [ ] chbench
  - [ ] billing-demo
  - [ ] perf-kinesis

- [ ] **cloud engineer** Let the tests run for at least 24 hours, with the following success
  criteria:

  - [ ] chbench: The ingest rate should not be slower than the previous release.
  - [ ] billing-demo: The container should run and finish without error. You can get the exit code
    from Docker:

    ```bash
    docker ps -a | grep -E 'STATUS|materialized|billing-demo'
    ```

    * `materialized` should be `up` (although see #4753 for the current breakage)
    * `billing-demo` should be `Exited (0)`
    * Note: it is not necessary to verify the graphs of this test if the above criteria are
      met. The graph output is dissimilar to the other tests, and can only be compared to historical
      runs posted in the #release Slack channel.

  - [ ] perf-kinesis: The "Time behind external source" dashboard panel in Grafana should
    have remained at 0ms or similar for the entirety of the run.

- [ ] **cloud engineer** Let the chaos test run for 24 hours. The test's `chaos_run` container
  should complete with a `0` exit code. To check this, SSH into the EC2 instance running the chaos
  test and run `docker ps -a`. You can ssh in using our [teleport cluster][], the chaos test has a
  `purpose=chaos` label.

- [ ] **cloud engineer** Remove all load test machines, documented on [the same page][load-instr].

[teleport cluster]: https://tsh.i.mtrlz.dev/cluster/tsh/nodes

## Final release

### Create Git tag

- [ ] Create the final release based on the final RC tag. For example, if there
  was only one RC, then the final RC tag would be `-rc1`.

  ```shell
  $ tag=v<VERSION>
  $ bin/mkrelease --checkout ${tag}-rcN $tag
  git push origin $tag
  ```
- [ ] Incorporate this tag into `main`'s history, and update the user doc config to mark this as
  released:

  ```console
  $ next=v<NEXT_VERSION>-dev
  $ bin/mkrelease --no-tag -b continue-$next $next
  ... snip ...
  Update doc/user/config.toml with 0.6.1 [y/N]: y
  ...
  Create a PR with your branch: 'continue-$next'
  ```

  - [ ] Open a PR with that branch, and land it. Using [gh] from the terminal: `gh pr create
    --web`

### Verify the Release Build and Deploy

- [ ] Find your build in buildkite, for example
  https://buildkite.com/materialize/tests/builds?branch=v0.4.3
- [ ] Wait for the completion of the "Deploy", your tag will be listed as the branch at:
  https://buildkite.com/materialize/deploy/builds

  **NOTE:** the deploy step will appear green in the "tests" page before it is actually run,
  because the tests only mark that the async job got kicked off, so check that second link.

### Create Homebrew bottle and update tap

- [ ] Follow the instructions in [MaterializeInc/homebrew-materialize's
  CONTRIBUTING.md][homebrew-guide].

### Verify the Debian package

- [ ] Run the following command on a Docker-enabled machine and verify you see
  the correct version.

  ```shell
  docker run --rm -i ubuntu:bionic <<EOF
  apt-get update && apt-get install -y gpg ca-certificates
  apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 379CE192D401AB61
  sh -c 'echo "deb http://packages.materialize.io/apt/ /" > /etc/apt/sources.list.d/materialize.list'
  apt-get update && apt-get install -y materialized
  materialized --version
  EOF
  ```

[bintray]: https://bintray.com/beta/#/materialize/apt/materialized

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

- [ ] On `main`, update the various files that must be up to date:

  - [ ] Ensure that the [release notes] are up to date, including the current version.

  - [ ] Ensure that [`doc/user/config.toml`] has the correct version, as updated in the "Final
    Release / create git tag" step

  - [ ] Ping `@relnotes-team` in `#release` to let them know the release process
    is finished, and that they should publish any appropriate blog posts and
    release announcements

## Finish

- [ ] Update the current status at the top of this issue.
- [ ] Create a slack alert for the next person to start the release (according to [the release
  schedule][schedule]) in the #release channel.
- [ ] Ask the product manager to make the release announcement in our community Slack.
- [ ] Close this issue.

[`doc/user/config.toml`]: https://github.com/MaterializeInc/materialize/blob/main/doc/user/config.toml
[`LICENSE`]: https://github.com/MaterializeInc/materialize/tree/main/LICENSE
[`src/materialized/Cargo.toml`]: https://github.com/MaterializeInc/materialize/tree/main/src/materialized/Cargo.toml
[homebrew-guide]: https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md
[release notes]: https://github.com/MaterializeInc/materialize/tree/main/doc/user/content/release-notes.md
[releases]: https://github.com/MaterializeInc/materialize/releases
[v0.1.2]: https://github.com/MaterializeInc/materialize/releases/tag/v0.1.2
[schedule]: https://docs.google.com/spreadsheets/d/1kd0Tlkr-dKClFLMJuSxhXgcikiZUcKrEFwdc53CBHy0
