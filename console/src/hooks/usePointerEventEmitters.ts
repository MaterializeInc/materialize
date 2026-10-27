// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { localPoint } from "@visx/event";
import { Point } from "framer-motion";
import { PointerEvent, useCallback, useContext, useState } from "react";

import { EventEmitterContext } from "~/components/EventEmitter";

// This file was copied from the visx XYchart:
// https://github.com/airbnb/visx/blob/dee0ac097cc3f53f4d81d1d99b736bf0e3c7ac1c/packages/visx-xychart/src/hooks/useEventEmitters.ts

type PointerEventEmitterParams = {
  /** Source of the events, e.g., the component name. */
  source: string;
  onBlur?: boolean;
  onFocus?: boolean;
  onPointerMove?: boolean;
  onPointerOut?: boolean;
  onPointerUp?: boolean;
  onPointerDown?: boolean;
};

/**
 * A hook that simplifies creation of handlers for emitting
 * pointermove, pointerout, and pointerup events to EventEmitterContext.
 */
export function usePointerEventEmitters({
  source,
  onPointerOut = true,
  onPointerMove = true,
}: PointerEventEmitterParams) {
  const emitter = useContext(EventEmitterContext);

  const emitPointerMove = useCallback(
    (event: PointerEvent) => {
      emitter?.emit("pointermove", {
        event,
        source,
      });
    },
    [emitter, source],
  );
  const emitPointerOut = useCallback(
    (event: PointerEvent) => {
      emitter?.emit("pointerout", {
        event,
        source,
      });
    },
    [emitter, source],
  );

  return {
    onPointerMove: onPointerMove ? emitPointerMove : undefined,
    onPointerOut: onPointerOut ? emitPointerOut : undefined,
  };
}

export function useCursorState() {
  const [cursor, setCursor] = useState<Point | null>(null);

  const handleCursorMove = useCallback((event: PointerEvent) => {
    const svgPoint = localPoint(event);
    if (!svgPoint) return;

    setCursor({ x: svgPoint.x, y: svgPoint.y });
  }, []);

  const hideCursor = useCallback(() => {
    setCursor(null);
  }, []);

  return { handleCursorMove, hideCursor, cursor };
}
