// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { documentTitle, parseConsoleAppearance } from "./appearance";

describe("documentTitle", () => {
  it("names the instance when it has a display name", () => {
    expect(documentTitle({ displayName: "prod" })).toEqual(
      "Materialize Console · prod",
    );
  });

  it("falls back to the plain title", () => {
    expect(documentTitle(undefined)).toEqual("Materialize Console");
    expect(documentTitle({})).toEqual("Materialize Console");
  });
});

describe("parseConsoleAppearance", () => {
  it("returns undefined when unconfigured", () => {
    expect(parseConsoleAppearance(undefined)).toBeUndefined();
    expect(parseConsoleAppearance(null)).toBeUndefined();
  });

  it("keeps a display name", () => {
    expect(parseConsoleAppearance({ displayName: "prod" })).toEqual({
      displayName: "prod",
    });
  });

  it("treats an empty display name as unset", () => {
    expect(parseConsoleAppearance({ displayName: "" })).toEqual({
      displayName: undefined,
    });
  });
});
