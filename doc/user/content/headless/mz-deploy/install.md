---
headless: true
---

`mz-deploy` is available for macOS and Linux. Install it with Homebrew
(recommended) or from a release archive.

{{< tabs >}}

{{< tab "Homebrew (recommended)" >}}

1. Install `mz-deploy` with [Homebrew](https://brew.sh/):

   ```shell
   brew install materializeinc/materialize/mz-deploy
   ```

1. Verify the installation:

   ```shell
   mz-deploy --version
   ```

{{< /tab >}}

{{< tab "Release archive" >}}

If you'd rather not use Homebrew, download the latest release for your platform:

{{< tabs >}}

{{< tab "macOS" >}}

1. Download and extract the latest release:

   ```shell
   ARCH=$(uname -m)
   sudo -v
   curl -L "https://binaries.materialize.com/mz-deploy-latest-$ARCH-apple-darwin.tar.gz" \
   | sudo tar -xzC /usr/local --strip-components=1
   ```

1. Verify the installation:

   ```shell
   mz-deploy --version
   ```

{{< /tab >}}

{{< tab "Linux" >}}

1. Download and extract the latest release:

   ```shell
   ARCH=$(uname -m)
   sudo -v
   curl -L "https://binaries.materialize.com/mz-deploy-latest-$ARCH-unknown-linux-gnu.tar.gz" \
   | sudo tar -xzC /usr/local --strip-components=1
   ```

1. Verify the installation:

   ```shell
   mz-deploy --version
   ```

{{< /tab >}}

{{< /tabs >}}

{{< /tab >}}

{{< /tabs >}}
