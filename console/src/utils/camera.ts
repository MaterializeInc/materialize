// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Point } from "~/types/geometry";
import { clamp } from "~/util";

export interface Camera {
  height: number;
  width: number;
  x: number;
  y: number;
  z: number;
}

export interface Dimensions {
  height: number;
  width: number;
}

export const MIN_ZOOM = 0.1;
export const MAX_ZOOM = 1;

export const panCamera = (camera: Camera, dx: number, dy: number): Camera => {
  return {
    ...camera,
    x: camera.x - dx / camera.z,
    y: camera.y - dy / camera.z,
    z: camera.z,
  };
};

/**
 * Calculates a translation required to keep the point under the cursor in the same
 * relative position after zooming.
 */
const calculateZoomOffset = (
  cameraSize: number,
  cursorPosition: number,
  currentZoom: number,
  newZoom: number,
) => {
  // Difference in the number of canvas pixels that will show up
  const relativeDistance = cameraSize / currentZoom - cameraSize / newZoom;
  // Ratio of distance from camera center to camera edge
  const offsetRatio = (cursorPosition - cameraSize / 2) / cameraSize;
  // The amount to translate by, multiplied by -1 because we translate the canvas rather
  // than the camera
  return -1 * relativeDistance * offsetRatio;
};

export const zoomCamera = (
  camera: Camera,
  cursor: Point,
  deltaZoom: number,
): Camera => {
  const newZoom = clamp(camera.z - deltaZoom, MIN_ZOOM, MAX_ZOOM);
  if (newZoom === camera.z) return camera;

  const diffX = calculateZoomOffset(camera.width, cursor.x, camera.z, newZoom);
  const diffY = calculateZoomOffset(camera.height, cursor.y, camera.z, newZoom);

  return {
    ...camera,
    x: camera.x + diffX,
    y: camera.y + diffY,
    z: newZoom,
  };
};
