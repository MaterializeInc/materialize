// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// CloudFront Function (cloudfront-js-2.0, viewer request) for the /docs/*
// behavior of materialize.com. It serves a page's Markdown rendition, the
// index.md that bin/docs-markdown writes beside each index.html, to clients
// that send `Accept: text/markdown`.
//
// It must run on viewer request, not origin request: the rewritten URI is
// then the cache key, so the HTML and Markdown renditions of a page are cached
// separately. Responses on the behavior must carry `Vary: Accept` (set by its
// response headers policy) so that caches downstream of CloudFront do the
// same.
//
// This file is not deployed by CI. Publish changes to the function attached
// to the distribution by hand.

function handler(event) {
  var request = event.request;
  var accept = request.headers.accept ? request.headers.accept.value : "";
  if (request.uri.endsWith("/") && accept.indexOf("text/markdown") !== -1) {
    request.uri += "index.md";
  }
  return request;
}
