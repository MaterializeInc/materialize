// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { highlightTree } from "@lezer/highlight";

import mzDialect from "./mzDialect";
import { darkHighlightStyle, lightHighlightStyle } from "./theme";

// Lezer matches highlight tags by identity, so a second copy of
// @lezer/highlight in node_modules silently disables highlighting.
describe("highlight styles", () => {
  it.each([
    ["light", lightHighlightStyle],
    ["dark", darkHighlightStyle],
  ])("highlights SQL keywords in the %s theme", (_name, highlightStyle) => {
    const tree = mzDialect.language.parser.parse("SELECT 1");
    const highlighted: string[] = [];
    highlightTree(tree, highlightStyle, (from, to) => {
      highlighted.push("SELECT 1".slice(from, to));
    });
    expect(highlighted).toContain("SELECT");
  });
});
