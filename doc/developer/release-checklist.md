# Release checklist

- [ ] Create a new github issue that you copy this checklist into, named `Release:
  vX.Y.Z`. (Copy this checklist [from raw markdown][release-checklist-raw], and create [a
  new issue][new-issue].)

[release-checklist-raw]: https://raw.githubusercontent.com/MaterializeInc/materialize/master/doc/developer/release-checklist.md
[new-issue]: https://github.com/MaterializeInc/materialize/issues/new/

## Release candidate

A release candidate is the Materialize codebase at a given commit, tested for
production readiness.

### Create the release candidate

- [ ] Choose the desired commit. Most often, this will be the latest commit to master.

  ```shell
  git checkout <SHA>
  ```

- [ ] Choose the name for the release candidate following the format
  `v<VERSION>-rc<N>`, where _N_ starts at 1. For example, the first RC for
  v0.2.3 would be called v0.2.3-rc1.

- Update the version fields in these files, and commit that change:

  - [ ] [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml)
  - [ ] [`LICENSE`](/LICENSE)

  <details><summary>Example Diff</summary>
  <p>

  ```diff
  diff --git a/src/materialized/Cargo.toml b/src/materialized/Cargo.toml
  index b0c561ff..a0c49bf4 100644
  --- a/src/materialized/Cargo.toml
  +++ b/src/materialized/Cargo.toml
  @@ -1,7 +1,7 @@
  [package]
  name = "materialized"
  description = "Streaming SQL materialized views."
  -version = "0.1.1"
  +version = "0.1.1-rc1"
  edition = "2018"
  publish = false
  default-run = "materialized"

  diff --git a/LICENSE b/LICENSE
  index 9ca12b61..04c40820 100644
  --- a/LICENSE
  +++ b/LICENSE
  @@ -13,7 +13,7 @@ Business Source License 1.1

   Licensor:                  Materialize, Inc.

  -Licensed Work:             Materialize Version 0.1
  +Licensed Work:             Materialize Version 0.1.1
                              The Licensed Work is © 2020 Materialize, Inc.

   Additional Use Grant:      You may use one single server instance of the
  @@ -29,7 +29,7 @@ Additional Use Grant:      You may use one single server instance of the
                              functionality of the Licensed Work by creating views
                              whose schemas are controlled by such third parties.

  -Change Date:               February 1, 2024
  +Change Date:               <RELEASE DATE + 4 YEARS>

   Change License:            Apache License, Version 2.0
  ```
  </p>
  </details>

- [ ] Create the release tag on the current commit.

  ```shell
  tag=v<VERSION>-rc<N>
  git tag -a $tag -m $tag
  git push origin $tag  # where 'origin' is your MaterializeInc/materialize remote
  ```

- [ ] On **master**:
  - Update the version field in
    [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml) to `vNEXT-dev`.
    For example, if releasing v0.1.2, bump the version on master to `v0.1.3-dev`.
  - Run `cargo check` (or any other build command) to ensure that that `Cargo.lock` file
    is correct.


### Test the release candidate

All of these can be run in parallel.

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

  - [ ] Spin up the billing-demo with an updated `--message-count` argument.

    ```shell
    cd demo/billing
    # Manually edit the mzcompose.yml to have `--message-count 100000000`
    # Follow the rest of the instructions in README.md
    ```

  - [ ] The billing-demo container should run and finish without error.


## Final release

### Create git tag

- [ ] Check out the final RC tag.

- [ ] Update the version field in [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml)
  and commit that change.

- [ ] Create the release tag on that commit.

  ```shell
  tag=v<VERSION>
  git tag -a $tag -m $tag
  git push origin $tag  # where 'origin' is your MaterializeInc/materialize remote
  ```

### Create Homebrew Bottle and update tap

- [ ] Follow the instructions in [MaterializeInc/homebrew-materialize's
  CONTRIBUTING.md][homebrew-guide].

### Verify the Debian package.

- [ ] Go to [the bintray `materialized` apt repo][bintray] and just check that
  the version you have published is present and published

[bintray]: https://bintray.com/beta/#/materialize/apt/materialized


### Convert the github Tag into a GitHub Release

- [ ] Go to [the materialize releases][releases] page and find the tag that you
  created as part of the "Final Release" step.

  - Click "Edit" on the top right
  - Use the version as the name.
  - Fill out the description by copying the description from an earlier release
    ([v0.1.2., for example][v0.1.2]) and updating the links appropriately.

### Update the master branch for the next version

- [ ] On **master**, update the various files that must be up to date:

  - [ ] Update the [`LICENSE`](/LICENSE) file with the changes from the
    release branch, so that it reflects the latest release of Materialize.
    <details><summary>Example git command</summary>

    ```console
    $ git checkout v0.2.1 -- LICENSE
    ```

    </details>

  - [ ] Ensure that the [Release Notes](../../doc/user/release-notes.md) are up
    to date, including the current version.

## Finish

- [ ] Close the release issue you opened as the first step

[homebrew-guide]: https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md
[releases]: https://github.com/MaterializeInc/materialize/releases
[v0.1.2]: https://github.com/MaterializeInc/materialize/releases/tag/v0.1.2
