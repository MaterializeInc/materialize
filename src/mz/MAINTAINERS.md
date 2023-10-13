# Maintainer instructions for `mz`

## Testing the CLI

All the tests for the CLI reside in a single function inside `./tests/e2e.rs`. To test locally, run:

```
cargo test -p mz
```

## Cutting a new release

1. Update the version in [src/mz/Cargo.toml](/src/mz/Cargo.toml).

2. Update the `mz` release notes in the docs with any changes since the last
   version.

3. Run `cargo run -p mz` once to update `Cargo.lcok`

4. Open a PR with your change and get it merged:

   ```
   VERSION=vX.Y.Z
   git branch -D mz-release
   git checkout -b mz-release
   git commit -am "mz: release $VERSION"
   git push -u your-fork
   gh pr create
   ```

5. Once the PR is merged to main, check out the merged SHA, tag it, and push
   the tag to GitHub.

   ```
   git checkout MERGED-SHA
   git tag -am mz-$VERSION mz-$VERSION
   git push --set-upstream upstream mz-$VERSION
   ```

6. Find the [Deploy mz](https://buildkite.com/materialize/deploy-mz) Buildkite
   build for your branch.

7. Once it completes, ensure that the new version is available on Docker Hub:

   ```
   docker run materialize/mz:$VERSION
   ```

8. Update the [Homebrew tap](https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md).
