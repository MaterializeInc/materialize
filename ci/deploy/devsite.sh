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

aws configure set default.s3.max_concurrent_requests 32

cargo about generate ci/deploy/licenses.hbs > misc/www/licenses.html

aws s3 cp --recursive --only-show-errors misc/www/ s3://materialize-dev-website/

# We exclude all of these pages from search engines for SEO purposes. We don't
# want to spend our crawl budget on these pages, nor have these pages appear
# ahead of our marketing content.
export RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html"
bin/doc
rm -rf target-xcompile/doc-public
cp -a target-xcompile/doc target-xcompile/doc-public
bin/doc --document-private-items

aws s3 sync --size-only --delete --only-show-errors target-xcompile/doc-public/ s3://materialize-dev-website/api/rust &
rust_pid=$!
aws s3 sync --size-only --delete --only-show-errors target-xcompile/doc/ s3://materialize-dev-website/api/rust-private &
rust_private_pid=$!
wait "$rust_pid"
wait "$rust_private_pid"

bin/pydoc
aws s3 sync --size-only --delete --only-show-errors target/pydoc/ s3://materialize-dev-website/api/python
