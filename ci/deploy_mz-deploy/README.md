# Deploy mz-deploy

mz-deploy ships on the Materialize release train. It carries the same version as
`environmentd` (`src/mz-deploy/Cargo.toml` is bumped by `bin/bump-version`), and
its release tarballs are built and uploaded by the `deploy` pipeline
(`ci/deploy/pipeline.template.yml`) whenever a `vX.Y.Z` tag is pushed.

**There is nothing to do to cut an mz-deploy release.** It goes out with the
database.

Tarballs land in the materialize-binaries S3 bucket, served at
<https://binaries.materialize.com>:

- Release candidate tags (`vX.Y.Z-rc.N`) publish
  `mz-deploy-vX.Y.Z-rc.N-<target>.tar.gz` but leave the `mz-deploy-latest-*`
  redirect alone. Our install docs point users at that redirect, so it only ever
  names a GA build.
- Final tags publish the versioned tarball and move `mz-deploy-latest-*`, unless
  the tag is a back-ported patch older than the newest release.

The Linux targets extract the binary from the `mz-deploy` Docker image the
release build already produced. macOS has no such image and builds from source.

Verify a release:

```bash
curl -fL "https://binaries.materialize.com/mz-deploy-latest-$(uname -m)-apple-darwin.tar.gz" | tar -tz
```

## Debugging a failed deploy

Run a target by hand from a checkout of the tag. The version assertion fails
unless `src/mz-deploy/Cargo.toml` agrees with `BUILDKITE_TAG`:

```bash
export BUILDKITE_TAG=vX.Y.Z

bin/pyactivate -m ci.deploy_mz-deploy.macos
bin/pyactivate -m ci.deploy_mz-deploy.linux
```

## Homebrew

A GA-only step, run after the tarballs are live. Update the
[Homebrew tap](https://github.com/MaterializeInc/homebrew-materialize) following
its CONTRIBUTING.md. Homebrew needs a stable URL and checksum, so the formula
pins an exact version rather than using the `latest` redirect. Never point it at
a release candidate. The tarballs contain the binary at `mz/bin/mz-deploy`:

```ruby
class MzDeploy < Formula
  desc "Declarative SQL project tooling for Materialize"
  homepage "https://materialize.com"
  version "26.X.Y"
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
