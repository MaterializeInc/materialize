#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# deploy.sh — deploys docs to mtrlz.dev in CI.

set -euo pipefail

mkdir ~/.ssh
cat >> ~/.ssh/known_hosts <<EOF
mtrlz.dev ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCsJdMnBVoCu1StAcoSH9iMIdM94V3if6U87gdrucDV/NHeWSTZa3lxHV4mGAFfSBV0oF3y82Sx4gspTjBxAeqwLcm70DrtSzl4cEPiJo4Hgj9U54kuZcIpDP8He/rNYzncrzKoeKzOwtVH5+oaJPcfeZ2CI1BVGj/Xt2A44d8Bixtzc6r+Q9czzYIwTCSxN4+rr7wPnNT3QXF+EvmmIsChXfsjVJpFHQKygVd9+9hk+b/bnqjsMG0O2gHmXSh4FQgbjeJvdTLEqC6SN8VEkaLLosbn79gxjei+U8pHcnossNgJQiaj4N5MDQwzWwGV9dxFBvvxcpdUknIBFlML5Joj
mtrlz.dev ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBMl4Vm8V+nUA677qNBKcIjUACY1tZuUa74MVMScGUp+szeuPbkqHpYqSkGnU+ONYVT0nRmXFhIOU2cO6hi2lGs4=
mtrlz.dev ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPbUN6RkJgvRv3Y7pmnwth5nribjue21mf8BeD12qr2C
EOF

bin/doc
rsync misc/www/index.html buildkite@mtrlz.dev:/var/www/html/index.html
rsync --archive target/doc/ buildkite@mtrlz.dev:/var/www/html/api
