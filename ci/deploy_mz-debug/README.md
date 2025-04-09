# Deploy the Materialize debug tool.

The CI process will build and deploy the Materialize debug tool to the materialize-binaries S3 bucket.
You can try the process by running the following commands:

```bash
# Set a tag version.
export BUILDKITE_TAG=mz-debug-vx.y.z

# macOS
bin/pyactivate -m ci.deploy_mz-debug.macos

# Linux
bin/pyactivate -m ci.deploy_mz-debug.linux
```

**Important Notes:**

- Update the version for `mz-debug`'s `Cargo.toml` to match the `BUILDKITE_TAG` version before deploying
- When running on macOS, modify `linux.py` to use `target` instead of `target-xcompile`
