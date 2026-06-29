# Deploy mz-deploy

The CI process will build the mz-deploy CLI and upload release tarballs to the
materialize-binaries S3 bucket, served at <https://binaries.materialize.com>.

## Deploy through Buildkite

To deploy a new version through Buildkite:

1. Update the version number in the `mz-deploy` Cargo.toml:
   - For new features: increment the minor version (0.X.0)
   - For bug fixes: increment the patch version (0.0.X)
   - For breaking changes: increment the major version

2. Run `cargo check` once to update `Cargo.lock`, then open a PR with the
   bump and get it merged to `main`.

3. Create and push a git tag in the format `mz-deploy-vX.Y.Z` on the merged
   commit:

   ```bash
   git checkout MERGED-SHA
   git tag -am mz-deploy-vX.Y.Z mz-deploy-vX.Y.Z
   git push upstream mz-deploy-vX.Y.Z
   ```

4. Navigate to the Buildkite pipelines page and trigger a new build:
   - Set the `BUILDKITE_TAG` environment variable to match your git tag
   - Start the build to deploy the new version

5. Once it completes, verify the tarballs are available:

   ```bash
   curl -fL "https://binaries.materialize.com/mz-deploy-latest-$(uname -m)-apple-darwin.tar.gz" | tar -tz
   ```

## Deploy manually

You can manually deploy by following steps 1-3 above and running the
following commands:

```bash
# Set a tag version.
export BUILDKITE_TAG=mz-deploy-vX.Y.Z

# macOS
bin/pyactivate -m ci.deploy_mz-deploy.macos

# Linux
bin/pyactivate -m ci.deploy_mz-deploy.linux
```

**Important Notes:**

- When running on macOS, modify `linux.py` to use `target` instead of
  `target-xcompile`
- For any new changes, regardless of how small, create a new version (patch
  if small) instead of overwriting the current git tag. Otherwise, local
  `git fetch --tag`s may error due to stale references of the old git tag.

## Homebrew

After the tarballs are live, update the
[Homebrew tap](https://github.com/MaterializeInc/homebrew-materialize)
following its CONTRIBUTING.md. The formula installs the prebuilt binaries
(tarballs contain the binary at `mz/bin/mz-deploy`):

```ruby
class MzDeploy < Formula
  desc "Declarative SQL project tooling for Materialize"
  homepage "https://materialize.com"
  version "X.Y.Z"
  license "BUSL-1.1"

  on_macos do
    on_arm do
      url "https://binaries.materialize.com/mz-deploy-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "<sha256 of the macOS tarball>"
    end
  end

  on_linux do
    on_intel do
      url "https://binaries.materialize.com/mz-deploy-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "<sha256 of the Linux x86_64 tarball>"
    end
    on_arm do
      url "https://binaries.materialize.com/mz-deploy-v#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "<sha256 of the Linux aarch64 tarball>"
    end
  end

  def install
    bin.install "mz/bin/mz-deploy" => "mz-deploy"
  end

  test do
    system "#{bin}/mz-deploy", "--version"
  end
end
```

Compute each `sha256` with `curl -fL <url> | shasum -a 256`.
