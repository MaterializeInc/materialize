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

aws s3 cp --recursive misc/www/ s3://materialize-dev-website/

bin/doc
aws s3 sync --size-only target-xcompile/doc/ s3://materialize-dev-website/api/rust

# Documenting private items causes broken links in many crates we don't control.
# So exclude all the pages from search engine indexes to avoid harming our
# SEO score with a number of broken links.
RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html" bin/doc --document-private-items
aws s3 sync --size-only target-xcompile/doc/ s3://materialize-dev-website/api/rust-private

bin/pydoc
aws s3 sync --size-only --delete target/pydoc/ s3://materialize-dev-website/api/python
