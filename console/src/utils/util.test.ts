// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { capitalizeSentence } from "~/util";

describe("capitalizeSentence", () => {
  it("should capitalize the first letter and add a period", () => {
    const input = "this is a test sentence";
    const expected = "This is a test sentence.";
    const result = capitalizeSentence(input);
    expect(result).toBe(expected);
  });

  it("should handle an empty string", () => {
    const input = "";
    const expected = ".";
    const result = capitalizeSentence(input);
    expect(result).toBe(expected);
  });

  it("should handle a single-character string", () => {
    const input = "a";
    const expected = "A.";
    const result = capitalizeSentence(input);
    expect(result).toBe(expected);
  });

  it("should handle a string with already capitalized first letter", () => {
    const input = "Already capitalized.";
    const expected = "Already capitalized.";
    const result = capitalizeSentence(input);
    expect(result).toBe(expected);
  });
});
