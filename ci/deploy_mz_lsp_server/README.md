# Deploy Materialize Language Server Protocol (LSP) Server.

The CI process will build and deploy the LSP server to the binaries S3 bucket.
You can try the process by running the following commands:

```bash
# Set a tag version.
export BUILDKITE_TAG=mz-lsp-server-vx.y.z

# macOS
bin/pyactivate -m ci.deploy_mz_lsp_server.macos

# Linux
bin/pyactivate -m ci.deploy_mz_lsp_server.linux
```
