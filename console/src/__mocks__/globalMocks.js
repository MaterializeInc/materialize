// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/* eslint-disable no-undef */
window.crypto.getRandomValues = () => new Uint32Array(1);
// eslint-disable-next-line @typescript-eslint/no-empty-function
window.scrollTo = function () {};
window.__BASENAME__ = "";
window.__CONSOLE_DEPLOYMENT_MODE__ = "mz-cloud";
window.__DEFAULT_STACK__ = "test";
window.__FORCE_OVERRIDE_STACK__ = undefined;
window.__IMPERSONATION_HOSTNAME__ = undefined;
window.__SENTRY_ENABLED__ = "false";
window.__SENTRY_RELEASE__ = "sentry-release";
/* eslint-enable */
