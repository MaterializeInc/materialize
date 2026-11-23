// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { beforeEach, describe, expect, it } from "vitest";

import {
  forgetExternalRedirect,
  FRONTEGG_AFTER_AUTH_REDIRECT_URL,
  rememberExternalRedirect,
} from "./externalRedirect";

const ALLOWED = ["https://materialize.com"];

describe("external redirect storage", () => {
  beforeEach(() => {
    localStorage.removeItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL);
  });

  it("forgets a stored marketing redirect", () => {
    rememberExternalRedirect("https://materialize.com/pricing/");
    forgetExternalRedirect(ALLOWED);

    expect(localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL)).toBeNull();
  });

  it("keeps Frontegg's own internal redirects", () => {
    localStorage.setItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL, "/regions");
    forgetExternalRedirect(ALLOWED);

    expect(localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL)).toBe(
      "/regions",
    );
  });
});
