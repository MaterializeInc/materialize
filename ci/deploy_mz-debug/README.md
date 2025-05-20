# Deploy the Materialize debug tool.

The CI process will build and deploy the Materialize debug tool to the materialize-binaries S3 bucket.

## Deploy through Buildkite
To deploy a new version through Buildkite:

1. Update the version number in the `mz-debug` Cargo.toml:
   - For new features: increment the minor version (0.X.0)
   - For bug fixes: increment the patch version (0.0.X)
   - For breaking changes: increment the major version. However, this should be rare given the tool is meant to be backwards compatible

2. Create and push a git tag in the format `mz-debug-vX.X.X` on the final commit of your changes in `main`.

3. Navigate to the Buildkite pipelines page and trigger a new build:
   - Set the `BUILDKITE_TAG` environment variable to match your git tag
   - Start the build to deploy the new version

## Deploy manually
You can manually deploy by following steps 1-2 above and running the following commands:

```bash
# Set a tag version.
export BUILDKITE_TAG=mz-debug-vx.y.z

# macOS
bin/pyactivate -m ci.deploy_mz-debug.macos

# Linux
bin/pyactivate -m ci.deploy_mz-debug.linux
```

**Important Notes:**
- When running on macOS, modify `linux.py` to use `target` instead of `target-xcompile`
- For any new changes, regardless of how small, create a new version (patch if small) instead of overwriting the current git tag. This is because otherwise, local `git fetch --tag`s may error due to stale references of the old git tag.
