
# Maintainer instructions for `mz-lsp-server`

## Testing the Language Server Protocol (LSP)

All the tests for the LSP reside in a single function inside `./tests/test.rs`. To test locally, run:

```
cargo test -p mz-lsp-server
```

## Cutting a new release

1. Update the version in [src/lsp-server/Cargo.toml](/src/lsp-server/Cargo.toml).

2. Update the `lsp-server` release notes in the docs with any changes since the last version.

3. Run `cargo run -p mz-lsp-server` once to update `Cargo.lock`

4. Open a PR with your change and get it merged:

   ```
   VERSION=vX.Y.Z
   git branch -D mz-lsp-server-release
   git checkout -b mz-lsp-server-release
   git commit -am "mz-lsp-server: release $VERSION"
   git push -u your-fork
   gh pr create
   ```

5. Once the PR is merged to main, check out the merged SHA, tag it, and push
   the tag to GitHub.

   ```
   git checkout MERGED-SHA
   git tag -am mz-lsp-server-$VERSION mz-lsp-server-$VERSION
   git push --set-upstream upstream mz-lsp-server-$VERSION
   ```

6. Find the [Deploy mz-lsp-server](https://buildkite.com/materialize/deploy-mz-lsp-server) Buildkite
   build for your branch. Make sure the macOS release runs; it tends to get stuck. If it happens, leave a message in Slack so it can be manually addressed.

7. The release is complete!
