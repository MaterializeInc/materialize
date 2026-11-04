// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { shouldRefit } from "./DataflowGraphView";

const base = { scope: "a", nodeSet: "n1,n2,n3", expansion: "" };

describe("shouldRefit", () => {
  it("refits on a scope change", () => {
    expect(shouldRefit(base, { ...base, scope: "b" })).toBe(true);
  });

  it("refits when a filter toggle changes the node set", () => {
    expect(shouldRefit(base, { ...base, nodeSet: "n1" })).toBe(true);
  });

  it("does not refit on a refresh that keeps the node set", () => {
    expect(shouldRefit(base, { ...base })).toBe(false);
  });

  it("does not refit when the node set changes because a region was expanded", () => {
    expect(
      shouldRefit(base, { ...base, nodeSet: "n1,n2,n3,n4", expansion: "n2" }),
    ).toBe(false);
  });
});
