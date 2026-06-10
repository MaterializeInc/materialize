// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React, { useRef } from "react";
import useResizeObserver from "use-resize-observer";

import { clamp } from "~/util";
import {
  Camera,
  MAX_ZOOM,
  MIN_ZOOM,
  panCamera,
  zoomCamera,
} from "~/utils/camera";

const ZOOM_STEP = 0.1;

export function useCamera() {
  const cameraRef = useRef<HTMLElement | null>(null);

  useResizeObserver<HTMLElement>({
    ref: cameraRef,
    onResize: ({ height, width }) => {
      if (height && width) {
        setCamera((c) => ({
          ...c,
          height,
          width,
        }));
      }
    },
  });

  const [camera, setCamera] = React.useState({
    x: 0,
    y: 0,
    z: 1,
    height: 0,
    width: 0,
  });

  React.useEffect(() => {
    function handleWheel(event: WheelEvent) {
      event.preventDefault();
      if (!cameraRef.current) return;

      const rect = cameraRef.current.getBoundingClientRect();

      const { clientX, clientY, deltaX, deltaY, ctrlKey, metaKey } = event;

      if (metaKey || ctrlKey) {
        setCamera((c: Camera) =>
          zoomCamera(
            c,
            // subtract the offset of the header and sidebar
            // so we have graph viewport relative coordinates
            { x: clientX - rect.left, y: clientY - rect.y },
            deltaY / 100,
          ),
        );
      } else {
        setCamera((c: Camera) => panCamera(c, deltaX, deltaY));
      }
    }

    const cameraEl = cameraRef.current;

    if (!cameraEl) return;

    cameraEl.addEventListener("wheel", handleWheel, { passive: false });

    return () => cameraEl.removeEventListener("wheel", handleWheel);
  }, []);

  const cssTransform = React.useMemo(
    () => `scale(${camera.z}) translate(${camera.x}px, ${camera.y}px)`,
    [camera.x, camera.y, camera.z],
  );

  const panTo = React.useCallback((x: number, y: number) => {
    setCamera((c: Camera) => ({ ...c, x, y }));
  }, []);

  const panBy = React.useCallback((deltaX: number, deltaY: number) => {
    setCamera((c: Camera) => panCamera(c, deltaX, deltaY));
  }, []);

  const resetZoom = React.useCallback(() => {
    setCamera((prev: Camera) => ({ ...prev, z: 1 }));
  }, []);

  const zoomIn = React.useCallback(() => {
    setCamera((prev: Camera) => ({
      ...prev,
      z: clamp(prev.z + ZOOM_STEP, MIN_ZOOM, MAX_ZOOM),
    }));
  }, []);

  const zoomOut = React.useCallback(() => {
    setCamera((prev: Camera) => ({
      ...prev,
      z: clamp(prev.z - ZOOM_STEP, MIN_ZOOM, MAX_ZOOM),
    }));
  }, []);

  const formatZoomValue = React.useCallback((value: number) => {
    return `${Math.round(value * 100)}%`;
  }, []);

  return {
    camera,
    formatZoomValue,
    cameraRef,
    resetZoom,
    cssTransform,
    zoomIn,
    zoomOut,
    panBy,
    panTo,
  };
}
