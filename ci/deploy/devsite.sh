#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# devsite.sh — deploys docs to dev.materialize.com in CI.

set -euo pipefail

. misc/shlib/shlib.bash

ci_collapsed_heading "Generate license breakdown"
cargo about generate ci/deploy/licenses.hbs > misc/www/licenses.html

ci_collapsed_heading "Upload misc/www to S3"
aws s3 cp --recursive misc/www/ s3://materialize-dev-website/

# We exclude all of these pages from search engines for SEO purposes. We don't
# want to spend our crawl budget on these pages, nor have these pages appear
# ahead of our marketing content.
ci_collapsed_heading "Build public Rust docs (bin/doc)"
RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html" bin/doc

ci_collapsed_heading "Build private Rust docs (bin/doc --document-private-items)"
RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html" bin/doc --document-private-items

ci_collapsed_heading "Sync public Rust docs to S3"
aws s3 sync --size-only target-xcompile/doc/ s3://materialize-dev-website/api/rust

ci_collapsed_heading "Sync private Rust docs to S3"
aws s3 sync --size-only target-xcompile/doc/ s3://materialize-dev-website/api/rust-private

ci_collapsed_heading "Build Python docs (bin/pydoc)"
bin/pydoc

ci_collapsed_heading "Sync Python docs to S3"
aws s3 sync --size-only --delete target/pydoc/ s3://materialize-dev-website/api/python
