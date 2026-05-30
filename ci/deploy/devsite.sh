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

# rustdoc emits ~13k tiny HTML files per docset, so the S3 syncs below are bound
# by per-object request latency rather than bandwidth. The aws CLI defaults to
# only 10 concurrent requests; bump it so the many small files upload in
# parallel.
aws configure set default.s3.max_concurrent_requests 100

cargo about generate ci/deploy/licenses.hbs > misc/www/licenses.html

aws s3 cp --recursive misc/www/ s3://materialize-dev-website/

# We exclude all of these pages from search engines for SEO purposes. We don't
# want to spend our crawl budget on these pages, nor have these pages appear
# ahead of our marketing content.
#
# `api/rust` gets the public docs; `api/rust-private` gets the docs with private
# items. We build each docset into its own target dir and build+upload them in
# parallel. --delete makes each prefix a clean mirror (and purges private pages
# previously uploaded to api/rust).
export RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html"

(
  CARGO_TARGET_DIR=target-xcompile bin/doc
  aws s3 sync --size-only --delete target-xcompile/doc/ s3://materialize-dev-website/api/rust
) &
rust_pid=$!

(
  CARGO_TARGET_DIR=target-xcompile-private bin/doc --document-private-items
  aws s3 sync --size-only --delete target-xcompile-private/doc/ s3://materialize-dev-website/api/rust-private
) &
rust_private_pid=$!

wait "$rust_pid"
wait "$rust_private_pid"

bin/pydoc
aws s3 sync --size-only --delete target/pydoc/ s3://materialize-dev-website/api/python
