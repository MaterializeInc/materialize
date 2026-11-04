// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { revealCenter, shouldRefit } from "./viewportFit";

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

describe("revealCenter", () => {
  const node = { x: 100, y: 100, width: 200, height: 72 };
  const canvas = { width: 1000, height: 600 };
  const identity = { x: 0, y: 0, zoom: 1 };

  it("does not pan a node that is fully inside the uncovered area", () => {
    expect(revealCenter(node, identity, canvas, 360)).toBeNull();
  });

  it("pans a node hidden under the overlay to the middle of the uncovered area", () => {
    const hidden = { ...node, x: 800 };
    // Uncovered width is 640, so its middle is at screen x 320. setCenter
    // places its point at 500, so the point sits 180 flow units right of the
    // node's center.
    expect(revealCenter(hidden, identity, canvas, 360)).toEqual({
      x: 900 + 180,
      y: 136,
    });
  });

  it("pans a node that is partly cut off at the overlay edge", () => {
    expect(
      revealCenter({ ...node, x: 500 }, identity, canvas, 360),
    ).not.toBeNull();
  });

  it("accounts for zoom and pan when testing visibility", () => {
    // At 0.5 zoom shifted left by 100 the node spans screen x -50..50.
    expect(
      revealCenter(node, { x: -100, y: 0, zoom: 0.5 }, canvas, 0),
    ).not.toBeNull();
  });
});
