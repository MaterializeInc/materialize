// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getSchemaNameFromSearchPath } from "./useSchemas";

describe("getSchemaNameFromSearchPath", () => {
  it("should return undefined if the search path is undefined", () => {
    expect(getSchemaNameFromSearchPath(undefined, [])).toEqual(undefined);
  });

  it("should return the first valid schema in the search path", () => {
    const extantSchemas = [{ name: "benji" }, { name: "wenji" }];
    const searchPath = "wenji";

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual(
      "wenji",
    );
  });

  it("should return undefined if search path is not an existing schema", () => {
    const extantSchemas = [{ name: "benji" }, { name: "wenji" }];
    const searchPath = "genji";

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual(
      undefined,
    );
  });

  it("should return the first item in the search path if it exists", () => {
    const extantSchemas = [{ name: "benji" }, { name: "wenji" }];
    const searchPath = "genji,shinji,wenji";

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual(
      "wenji",
    );
  });

  it("should return undefined if extant schemas is empty", () => {
    const extantSchemas: { name: string }[] = [];
    const searchPath = "benji,wenji";

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual(
      undefined,
    );
  });

  it("should account for spaces in the search path", () => {
    const extantSchemas = [{ name: "wenji" }];
    const searchPath = "benji, wenji";

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual(
      "wenji",
    );
  });

  it("strips quotes", () => {
    const extantSchemas = [{ name: "καφέ$" }];
    const searchPath = '"καφέ$, foo"';

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual(
      "καφέ$",
    );
  });

  it("unescapes quotes", () => {
    const extantSchemas = [{ name: '"' }];
    // Any search path containing special characters is surrounded with double quotes,
    // and double quotes in the value will be escaped with an extra double quotes.
    const searchPath = '""""';

    expect(getSchemaNameFromSearchPath(searchPath, extantSchemas)).toEqual('"');
  });
});
