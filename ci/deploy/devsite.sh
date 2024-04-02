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

cargo about generate ci/deploy/licenses.hbs > misc/www/licenses.html

aws s3 cp --recursive misc/www/ s3://materialize-dev-website/

# We set `noindex` on our rustdocs to avoid burning crawl budget [1]
# or diverting search results to internal docs rather than public-facing
# content. Additionally, the private items docs have many broken links
# that would actively harm our SEO score.
#
# [1]: https://developers.google.com/search/blog/2017/01/what-crawl-budget-means-for-googlebot
RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html" bin/doc
aws s3 sync --size-only target-xcompile/doc/ s3://materialize-dev-website/api/rust

RUSTDOCFLAGS="--html-in-header $PWD/ci/deploy/noindex.html" bin/doc --document-private-items
aws s3 sync --size-only target-xcompile/doc/ s3://materialize-dev-website/api/rust-private

bin/pydoc
aws s3 sync --size-only --delete target/pydoc/ s3://materialize-dev-website/api/python
