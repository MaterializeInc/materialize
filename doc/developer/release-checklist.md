# Release checklist

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

- [ ] Update the version field in [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml)
      and [`LICENSE`](/LICENSE) and commit that change.

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


- [ ] Create the release tag on the current commit.

  ```shell
  tag=v<VERSION>-rc<N>
  git tag -a $tag -m $tag
  git push origin $tag  # where 'origin' is your MaterializeInc/materialize remote
  ```

### Test the release candidate

- [ ] Run the chbench load test on the release candidate tag.

  - [ ] Spin up a fresh VM and start the load test.

    ```shell
    cd demo/chbench/terraform
    terraform init
    terraform apply
    # SSH into the machine that is created.
    cd materialize/demo/chbench
    git checkout $tag
    ./dc.sh clean-load-test
    ```

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
    cd src/billing-demo
    # Manually edit the mzcompose.yml to have `--message-count 100000000`
    # Follow the rest of the instructions in README.md
    ```

  - [ ] The billing-demo container should run and finish without error.


## Final release

- [ ] Check out the final RC tag.

- [ ] Update the version field in [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml)
      and commit that change.

- [ ] Create the release tag on that commit.

  ```shell
  tag=v<VERSION>
  git tag -a $tag -m $tag
  git push origin $tag  # where 'origin' is your MaterializeInc/materialize remote
  ```

- [ ] Create Homebrew bottle and update Homebrew tap.

  Follow the instructions in [MaterializeInc/homebrew-materialize's
  CONTRIBUTING.md](homebrew-guide).

- [ ] Create Debian package.

  - [ ] Invert the `name` and `conflicts` fields in [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml).

  - [ ] Run `cargo-deb` inside the CI builder. Note that <VERSION-NO-V> below
    must not include the `v` prefix.

    ```shell
    bin/ci-builder run stable cargo-deb --no-strip -p materialized --deb-version <VERSION-NO-V>
    ```

    Upload the resulting `.deb` file to [GemFury](https://fury.io) by dragging-
    and-dropping onto the administration console. (Yes, this is really the UI.)

- [ ] Create a new [GitHub release][new-github-release].

  Use the version as the name. Fill out the description by copying the
  description from an earlier release ([v0.1.2., for example][v0.1.2]) and
  updating the links appropriately.

- [ ] On **master**, update the version field in [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml)
  to `vNEXT-dev`. For example, if releasing v0.1.2, bump the version on
  master to `v0.1.3-dev`.

   Also update the [`LICENSE`](/LICENSE) file with the changes from the
   release branch, so that it reflects the latest release of Materialize.

[homebrew-guide]: https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md
[new-github-release]: https://github.com/MaterializeInc/materialize/releases/new
[v0.1.2]: https://github.com/MaterializeInc/materialize/releases/tag/v0.1.2
