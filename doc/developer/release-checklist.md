# Release Tasks

## Code Changes

- [ ] update the version field in [`src/materialized/Cargo.toml`](../../src/materialized/Cargo.toml)

## Verification

- [ ] load/soak tests run on -rc tagged commit
  - command should be:
    ```
    dc.sh clean-load-test
    ```
  - [ ] take a screenshot of the grafana dashboard with full 24 hours of data and share
        it in #release in slack
  - [ ] take a backup of the prometheus dashboard once you have stopped prometheus:
    ```
    dc.sh stop prometheus
    dc.sh backup
    ```

## Artifacts

- [ ] Create release tag
- [ ] Update homebrew
- [ ] Create release deb
- [ ] Format git tag as github release
  - [ ] Link to docker tag
  - [ ] Attach binary tarballs
  - [ ] Attach .deb package
