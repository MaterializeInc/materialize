// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { buildXYGraphLayoutProps } from "./graph";

describe("buildXYGraphLayoutProps", () => {
  it("clamps inner width/height to 0 rather than going negative", () => {
    // ParentSize renders once with width/height 0 before its ResizeObserver
    // reports the real size. A negative value here would flow into an SVG
    // rect's width/height attribute (e.g. GraphEventOverlay), which throws.
    const { graphEventOverlayProps } = buildXYGraphLayoutProps({
      width: 0,
      height: 0,
      margin: { top: 10, right: 0, bottom: 36, left: 50 },
    });

    expect(graphEventOverlayProps.width).toBe(0);
    expect(graphEventOverlayProps.height).toBe(0);
  });

  it("computes the normal positive layout when width/height are non-zero", () => {
    const { graphEventOverlayProps } = buildXYGraphLayoutProps({
      width: 300,
      height: 200,
      margin: { top: 10, right: 0, bottom: 36, left: 50 },
    });

    expect(graphEventOverlayProps.width).toBe(250);
    expect(graphEventOverlayProps.height).toBe(154);
  });
});
