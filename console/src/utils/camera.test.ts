// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MAX_ZOOM, MIN_ZOOM, zoomCamera } from "./camera";

describe("zoomCamera", () => {
  it("zoom in correctly", () => {
    const result = zoomCamera(
      { x: 0, y: 0, z: 1, width: 800, height: 600 },
      { x: 100, y: 100 },
      0.2,
    );
    expect(result).toEqual({
      height: 600,
      width: 800,
      x: -75,
      y: -50,
      z: 0.8,
    });
  });

  it("zoom out correctly", () => {
    const result = zoomCamera(
      { x: 0, y: 0, z: 0.2, width: 800, height: 600 },
      { x: 100, y: 100 },
      -0.2,
    );
    expect(result).toEqual({
      height: 600,
      width: 800,
      x: 750,
      y: 500,
      z: 0.4,
    });
  });

  it("enforces MIN_ZOOM", () => {
    const result = zoomCamera(
      { x: 0, y: 0, z: MIN_ZOOM, width: 800, height: 600 },
      { x: 100, y: 100 },
      0.1,
    );
    expect(result).toEqual({
      x: 0,
      y: 0,
      z: MIN_ZOOM,
      width: 800,
      height: 600,
    });
  });

  it("enforces MAX_ZOOM", () => {
    const result = zoomCamera(
      { x: 0, y: 0, z: MAX_ZOOM, width: 800, height: 600 },
      { x: 100, y: 100 },
      -0.1,
    );
    expect(result).toEqual({
      x: 0,
      y: 0,
      z: MAX_ZOOM,
      width: 800,
      height: 600,
    });
  });
});
