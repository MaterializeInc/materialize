---
name: "Internal: release checklist"
about: >
  A tracking issue for a new release of Materialize. Contributor use only.
---

## Current Status: 🚢 Progressing 🚢

<!--
## Current Status: 🛑 Blocked 🛑
## Current Status: 🚀 [Released](https://github.com/MaterializeInc/materialize/releases/tag/vX.Y.Z) 🚀
-->

## Announce the imminent release internally

- [x] create this release issue
- [ ] Check for open issues with the [`M-milestone-blocker`][blocker-search] label,
  include them in a list here, or state that there are none. Update the current status.
  > unknown
- [ ] Link to this issue in the `#release` slack channel

[blocker-search]: https://github.com/MaterializeInc/materialize/issues?q=is%3Aissue+is%3Aopen+label%3AM-milestone-blocker

## Release candidate

A release candidate is the Materialize codebase at a given commit, tested for
production readiness.

### Create the release candidate

- [ ] Choose the name for the release candidate following the format
  `v<VERSION>-rc<N>`, where _N_ starts at 1. For example, the first RC for
  v0.2.3 would be called v0.2.3-rc1. Use that to run the `bin/mkrelease`
  script:

  ```shell
  tag=v<VERSION>-rc<N>
  bin/mkrelease $tag
  git push origin $tag
  ```

- [ ] *After* you have pushed that tag, on **main**, do the same thing for the dev
  version, without creating a tag:

  ```shell
  next=v<NEXT_VERSION>-dev
  bin/mkrelease --no-tag -b prepare-$next $next
  ```

  This must be done after the tag has been pushed, or git will delete the tag that isn't
  on the server.

  - [ ] Open a PR, and land it.

- [ ] Create an issue and assign it to the @MaterializeInc/relnotes team to review the
  Release Notes and (if applicable) the Release Announcement. All members of the team
  should leave a comment stating that the release looks good.


### Test the release candidate

To run the required load tests on the release candidate tag, Materialize employees
can follow [these instructions for running semi-automatic load tests][load-instr]
in the infrastructure repository. All of these tests can be run in parallel.

[load-instr]: https://github.com/MaterializeInc/infra/tree/main/cloud#starting-a-load-test

- [ ] Kick off a [full SQL logic test run](https://buildkite.com/materialize/sql-logic-tests)
  and verify that it passes.

- [ ] Start the load tests according to the instructions above

- [ ] Find the load tests in grafana.mz , and link to them in #release, validating that
    data is showing up:

  - [ ] chbench
  - [ ] billing-demo
  - [ ] perf-kinesis

- [ ] Let the tests run for at least 24 hours, with the following success criteria:

  - [ ] Chbench should not have slower ingest than the previous release
  - [ ] The billing-demo container should run and finish without error
  - [ ] The "Time behind external source" dashboard panel in Grafana should have remained
    at 0ms or similar for the entirety of the run.

- [ ] Remove all load test machines.

## Final release

### Create git tag

- [ ] Create the final release based on the final RC tag. For example, if there was only
  one RC then that would be `-rc1`

  ```console
  $ tag=v<VERSION>
  $ bin/mkrelease --checkout ${tag}-rc1 $tag
  git push origin $tag
  ```

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
    * [Release notes](https://materialize.io/docs/release-notes/#v0.#.#)
    * [Binary tarballs](https://materialize.io/docs/versions/)
    * [Installation instructions](https://materialize.io/docs/install/)
    * [Documentation](https://materialize.io/docs/)
    ```

### Update the main branch for the next version

- [ ] On **main**, update the various files that must be up to date:

  - [ ] Ensure that the [Release Notes] are up to date, including the current version.

  - [ ] Add the version to the website's list of versions in [`doc/user/config.toml`].

  - [ ] Ensure that the issue for checking the release notes has been closed.

  - [ ] (If applicable) Ensure that the announcement blog post has been published and
    announced, by pinging the PM in `#release`.

## Finish

- [ ] Update the current status at the top of this issue.
- [ ] Close this issue.

[`doc/user/config.toml`]: https://github.com/MaterializeInc/materialize/blob/main/doc/user/config.toml
[`LICENSE`]: https://github.com/MaterializeInc/materialize/tree/main/LICENSE
[`src/materialized/Cargo.toml`]: https://github.com/MaterializeInc/materialize/tree/main/src/materialized/Cargo.toml
[homebrew-guide]: https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md
[Release Notes]: https://github.com/MaterializeInc/materialize/tree/main/doc/user/content/release-notes.md
[releases]: https://github.com/MaterializeInc/materialize/releases
[v0.1.2]: https://github.com/MaterializeInc/materialize/releases/tag/v0.1.2
[our blog]: https://materialize.io/wp-admin/edit.php?post_type=post
[makes sense]: https://github.com/MaterializeInc/materialize/pull/3769#pullrequestreview-456713779
