// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CSSObjectWithLabel } from "react-select";
import { describe, expect, it } from "vitest";

import { lightTheme } from "~/theme";

import { buildSearchableSelectFilterStyles } from "./utils";

const containerStyle = (frameless?: boolean) =>
  buildSearchableSelectFilterStyles({
    colors: lightTheme.colors,
    shadows: lightTheme.shadows,
    frameless,
  }).container?.({} as CSSObjectWithLabel, { isFocused: true } as never);

describe("buildSearchableSelectFilterStyles", () => {
  it("draws its own border and focus ring by default", () => {
    expect(containerStyle()).toMatchObject({ borderWidth: "1px" });
    expect(containerStyle()?.boxShadow).not.toBe("none");
  });

  it("draws neither when frameless, for a parent that frames it", () => {
    expect(containerStyle(true)).toMatchObject({
      borderWidth: 0,
      borderRadius: 0,
      boxShadow: "none",
    });
  });
});
