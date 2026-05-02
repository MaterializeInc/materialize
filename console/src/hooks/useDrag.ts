// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useCallbackRef } from "@chakra-ui/react";
import React from "react";

import { Point } from "~/types/geometry";

/** Max movement before we ignore click events and assume the intent is to drag */
const MAX_CLICK_MOVE_THRESHOLD = 10;

// User-select Hacks:
// Useful for preventing blue highlights all over everything when dragging.
// These functions are derived from react-draggable https://github.com/react-grid-layout/react-draggable/blob/master/lib/utils/domFns.js
export function addUserSelectStyles(doc?: Document) {
  if (!doc) return;
  let styleEl = doc.getElementById(
    "use-drag-style-el",
  ) as HTMLStyleElement | null;
  if (!styleEl) {
    styleEl = doc.createElement("style");
    styleEl.id = "use-drag-style-el";
    styleEl.innerHTML =
      ".use-drag-transparent-selection {user-select: none;-webkit-user-select: none;}\n";
    doc.getElementsByTagName("head")[0]?.appendChild(styleEl);
  }
  if (doc.body) {
    doc.body.classList.add("use-drag-transparent-selection");
  }
}

export function removeUserSelectStyles(doc?: Document) {
  if (!doc) return;
  if (doc.body) {
    doc.body.classList.remove("use-drag-transparent-selection");
  }
  const selection = (doc.defaultView || window).getSelection();
  if (selection && selection.type !== "Caret") {
    selection.removeAllRanges();
  }
}

function createDraggableEvent({
  curPoint,
  lastPoint,
}: {
  curPoint: Point;
  lastPoint: Point | null;
}) {
  if (!lastPoint) {
    return {
      curPoint,
      lastPoint,
      pointDelta: null,
    };
  }

  return {
    curPoint,
    lastPoint,
    pointDelta: {
      x: curPoint.x - lastPoint.x,
      y: curPoint.y - lastPoint.y,
    },
  };
}
export type DraggableEvent = ReturnType<typeof createDraggableEvent>;

export const useDrag = ({
  ref,
  onDrag,
  onStart,
  onStop,
}: {
  ref: React.RefObject<HTMLElement | null>;
  onDrag?: (event: PointerEvent, draggableEvent: DraggableEvent) => void;
  onStart?: (event: PointerEvent) => void;
  onStop?: (event: PointerEvent) => void;
}) => {
  const _onDrag = useCallbackRef(onDrag);
  const _onStart = useCallbackRef(onStart);
  const _onStop = useCallbackRef(onStop);

  // Tracks the point since the last pointermove event
  const lastPointSinceDragMoveRef = React.useRef<{
    x: number;
    y: number;
  } | null>(null);
  // Tracks the point since the last pointerdown event. Useful for short circuiting click events
  const lastPointSinceDragStartRef = React.useRef<{
    x: number;
    y: number;
  } | null>(null);

  const handlePointerMove = React.useCallback(
    (e: PointerEvent) => {
      if (lastPointSinceDragMoveRef.current && _onDrag) {
        _onDrag(
          e,
          createDraggableEvent({
            curPoint: { x: e.pageX, y: e.pageY },
            lastPoint: lastPointSinceDragMoveRef.current,
          }),
        );
        lastPointSinceDragMoveRef.current = { x: e.pageX, y: e.pageY };
      }
    },
    [_onDrag],
  );

  const handlePointerUp = React.useCallback(
    (e: PointerEvent) => {
      const element = ref.current;
      if (!element) return;

      element.ownerDocument.removeEventListener(
        "pointermove",
        handlePointerMove,
      );
      element.ownerDocument.removeEventListener("pointerup", handlePointerUp);

      _onStop?.(e);

      // Cleanup hack to prevent text selection while dragging
      removeUserSelectStyles(element.ownerDocument);
    },
    [ref, handlePointerMove, _onStop],
  );

  const handlePointerDown = React.useCallback(
    (e: PointerEvent) => {
      // only respond to left click
      if (e.button !== 0) return;
      const element = ref.current;
      if (!element) return;
      lastPointSinceDragMoveRef.current = { x: e.pageX, y: e.pageY };
      lastPointSinceDragStartRef.current = { x: e.pageX, y: e.pageY };

      element.ownerDocument.addEventListener("pointerup", handlePointerUp);
      element.ownerDocument.addEventListener("pointermove", handlePointerMove);

      _onStart?.(e);

      // Hack to prevent text selection while dragging
      addUserSelectStyles(element.ownerDocument);
    },
    [ref, handlePointerMove, handlePointerUp, _onStart],
  );

  const handleClick = React.useCallback((e: MouseEvent) => {
    if (!lastPointSinceDragStartRef.current) return;
    const x = e.pageX;
    const y = e.pageY;
    const deltaX = x - lastPointSinceDragStartRef.current.x;
    const deltaY = y - lastPointSinceDragStartRef.current.y;

    if (
      Math.abs(deltaX) > MAX_CLICK_MOVE_THRESHOLD &&
      Math.abs(deltaY) > MAX_CLICK_MOVE_THRESHOLD
    ) {
      e.stopPropagation();
    }
  }, []);

  React.useEffect(() => {
    const element = ref.current;
    if (element) {
      element.addEventListener("click", handleClick);
      element.addEventListener("pointerdown", handlePointerDown);

      return () => {
        element.removeEventListener("click", handleClick);
        element.removeEventListener("pointerdown", handlePointerDown);

        element.ownerDocument.removeEventListener(
          "pointermove",
          handlePointerMove,
        );
        element.ownerDocument.removeEventListener("pointerup", handlePointerUp);

        // Cleanup hack to prevent text selection while dragging
        removeUserSelectStyles(element.ownerDocument);
      };
    }
  }, [handlePointerDown, handlePointerUp, handlePointerMove, ref, handleClick]);
};
