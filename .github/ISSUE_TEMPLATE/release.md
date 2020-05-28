---
name: "Internal: release checklist"
about: >
  A tracking issue for a new release of Materialize. Contributor use only.
---

## Release candidate

A release candidate is the Materialize codebase at a given commit, tested for
production readiness.

### Create the release candidate

- [ ] Choose the desired commit. Most often, this will be the latest commit to
  master.

  ```shell
  git checkout <SHA>
  ```

- [ ] Choose the name for the release candidate following the format
  `v<VERSION>-rc<N>`, where _N_ starts at 1. For example, the first RC for
  v0.2.3 would be called v0.2.3-rc1.

  ```shell
  tag=v<VERSION>-rc<N>
  ```

- [ ] Update the version field in [`src/materialized/Cargo.toml`].

- [ ] Update the [`LICENSE`] file.

  - [ ] Set the version field.
  - [ ] Set the "Change Date" to four years beyond the expected release date.

- [ ] Run `cargo check` to update `Cargo.lock` with the new version.

- [ ] Commit those changes.

   ```shell
   git commit -m "release: $tag"
   ```

- [ ] Create the release tag on the current commit.

  ```shell
  git tag -a $tag -m $tag
  git push origin $tag  # where 'origin' is your MaterializeInc/materialize remote
  ```

- [ ] On **master**:

  - [ ] Update the version field in
    [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml) to
    `vNEXT-dev`. For example, if releasing v0.1.2, bump the version on master to
    `v0.1.3-dev`.

  - [ ] Update the [`LICENSE`](/LICENSE) file to use the `-dev` version, and
    set the "Change Date" to four years beyond the tentative next release.
    For example, if releasing `v0.1.2` set the change date to the tentative
    release date of `v0.1.3` plus four years.

  - [ ] Run `cargo check` to update `Cargo.lock` with the new version.

  - [ ] Commit that change, open a PR, and land it.


### Test the release candidate

All of these tests can be run in parallel.

- [ ] Run the chbench load test on the release candidate tag.

  - [ ] Spin up a fresh VM and start the load test.

    Materialize employees can follow [these instructions for running semi-
    automatic load tests][load-instr] in the infrastructure
    repository.

    [load-instr]: https://github.com/MaterializeInc/infra/tree/master/cloud#starting-a-load-test

  - [ ] From the VM, ensure all containers are running:

    ```shell script
    docker ps -a
    ```

  - [ ] Let the test run for 24 hours.

  - [ ] Take a screenshot of the Grafana dashboard with the full 24 hours of
    data and share it in the #release channel in Slack.

  - [ ] Stop Prometheus, and upload a backup of the data to the #release
    channel in Slack.

    ```shell
    ./dc.sh stop prometheus
    ./dc.sh backup
    ```

- [ ] Run the billing-demo load test on the tag.

  - [ ] Spin up a fresh VM and start the load test.

    Materialize employees can follow [these instructions for running semi-
    automatic load tests][load-instr] in the infrastructure
    repository.

    [load-instr]: https://github.com/MaterializeInc/infra/tree/master/cloud#starting-a-load-test

  - [ ] From the VM, ensure all containers are running:

    ```shell script
    docker ps -a
    ```

  - [ ] The billing-demo container should run and finish without error.

- [ ] Run the perf-kinesis load test on the tag.

  - [ ] Spin up a fresh VM and start the load test. To start the load test, ssh
    into the VM and run:

    ```shell
    cd materialize/test/performance/perf-kinesis
    ./mzcompose up -d
    ```

  - [ ] Ensure all containers started successfully and are running:

    ```shell
    docker ps -a
    ```

  - [ ] Let the test run for 24 hours.

  - [ ] Take a screenshot of the "Time behind external source" dashboard panel
    in Grafana. (This metric should have remained at 0ms or similar for the entirety
    of the run). Share the screenshot in the Slack channel.

## Final release

### Create git tag

- [ ] Check out the final RC tag.

- [ ] Update the version field in
  [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml) and commit
  that change.

- [ ] Ensure the change date in [`LICENSE`](/../) is correct.

- [ ] Run `cargo check` to update `Cargo.lock` with the new version.

- [ ] Create the release tag on that commit.

  ```shell
  tag=v<VERSION>
  git tag -a $tag -m $tag
  git push origin $tag  # where 'origin' is your MaterializeInc/materialize remote
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

### Update the master branch for the next version

- [ ] On **master**, update the various files that must be up to date:

  - [ ] Ensure that the [Release Notes] are up
    to date, including the current version.

  - [ ] Add the version to the website's list of versions in
    [`doc/user/config.toml`].

## Finish

- [ ] Remove all load test machines.

- [ ] Close this issue.

[`doc/user/config.toml`]: https://github.com/MaterializeInc/materialize/blob/master/doc/user/config.toml
[`LICENSE`]: https://github.com/MaterializeInc/materialize/tree/master/LICENSE
[`src/materialized/Cargo.toml`]: https://github.com/MaterializeInc/materialize/tree/master/src/materialized/Cargo.toml
[homebrew-guide]: https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md
[Release Notes]: https://github.com/MaterializeInc/materialize/tree/master/doc/user/content/release-notes.md
[releases]: https://github.com/MaterializeInc/materialize/releases
[v0.1.2]: https://github.com/MaterializeInc/materialize/releases/tag/v0.1.2
