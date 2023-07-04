# Maintainer instructions for `mz`

## Cutting a new release

1. Update the version in [src/mz/Cargo.toml](/src/mz/Cargo.toml).

2. Update the `mz` release notes in the docs with any changes since the last
   version.

3. Open a PR with your change and get it merged:

   ```
   VERSION=vX.Y.Z
   git checkout -b mz-release
   git commit -am "mz: release $VERSION"
   git push -u your-fork
   gh pr create
   ```

4. Once the PR is merged to main, check out the merged SHA, tag it, and push
   the tag to GitHub.

   ```
   git checkout MERGED-SHA
   git tag -am $VERSION $VERSION
   git push origin $VERSION
   ```

5. Find the [Deploy mz](https://buildkite.com/materialize/deploy-mz) Buildkite
   build for your branch.

6. Once it completes, ensure that the new version is available on Docker Hub:

   ```
   docker run materialize/mz:$VERSION
   ```

7. Update the [Homebrew tap](https://github.com/MaterializeInc/homebrew-materialize/blob/master/CONTRIBUTING.md).
