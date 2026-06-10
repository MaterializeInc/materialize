// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { alreadyExistsError } from "./parseErrors";

describe("alreadyExistsError", () => {
  it("server error with a quoted name and the text 'already exists'", () => {
    const serverError = "catalog item 'test_1' already exists";
    expect(alreadyExistsError(serverError)).toBe("test_1");
  });

  it("server error with no quotations", () => {
    const serverError = "catalog item test_1 already exists";
    expect(alreadyExistsError(serverError)).toBe(null);
  });

  it("server error with double quotations", () => {
    const serverError = 'catalog item "test_1" already exists';
    expect(alreadyExistsError(serverError)).toBe("test_1");
  });

  it("server error with quotations but doesn't end with 'already exists'", () => {
    const serverError = "system item 'test_1' cannot be modified";
    expect(alreadyExistsError(serverError)).toBe(null);
  });

  it("error message is undefined", () => {
    expect(alreadyExistsError(undefined)).toBe(null);
  });
});
