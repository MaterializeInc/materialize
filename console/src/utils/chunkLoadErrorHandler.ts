// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";

export function addChunkLoadErrorListener() {
  // Vite triggers this event if it fails to load a chunk, which can happen when we deploy
  // during a user's session,then they try to load a chunk that no longer exists in the
  // current deployment.
  // https://vitejs.dev/guide/build.html#load-error-handling
  window.addEventListener("vite:preloadError", (event) => {
    Sentry.captureException(event.payload);
    window.location.reload();
  });
}
