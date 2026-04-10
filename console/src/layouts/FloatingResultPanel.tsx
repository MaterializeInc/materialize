// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, useTheme } from "@chakra-ui/react";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import React from "react";
import { useLocation } from "react-router-dom";

import ResultsPanel from "~/platform/worksheet/ResultsPanel";
import {
  resultsPanelHeightAtom,
  resultsPanelOpenAtom,
  resultsPanelPathAtom,
  stopSubscribeAtom,
  subscribeStateAtom,
  worksheetExecuteAtom,
  worksheetResultAtom,
} from "~/platform/worksheet/store";
import type { MaterializeTheme } from "~/theme";

const noop = () => {};

/** Default height of the results panel in pixels. */
const DEFAULT_HEIGHT = 300;

/** Minimum height the results panel can be resized to (pixels). */
const MIN_HEIGHT = 100;

/** Maximum height the results panel can be resized to (pixels). */
const MAX_HEIGHT = 600;

/**
 * Global result panel rendered in BaseLayout. Shows query results,
 * SUBSCRIBE streams, or SHOW CREATE output on any page. Resizable
 * by dragging the top edge.
 */
const FloatingResultPanel = () => {
  const location = useLocation();
  const result = useAtomValue(worksheetResultAtom);
  const subscribeState = useAtomValue(subscribeStateAtom);
  const stopSubscribe = useAtomValue(stopSubscribeAtom);
  const [panelOpen, setPanelOpen] = useAtom(resultsPanelOpenAtom);
  const executeFromWorksheet = useAtomValue(worksheetExecuteAtom);
  const setResultsPanelPath = useSetAtom(resultsPanelPathAtom);
  const setResultsPanelHeight = useSetAtom(resultsPanelHeightAtom);
  const { colors } = useTheme<MaterializeTheme>();

  const [height, setHeight] = React.useState(DEFAULT_HEIGHT);
  const isDragging = React.useRef(false);
  const dragCleanupRef = React.useRef<(() => void) | null>(null);

  // Clean up any lingering drag listeners if the component unmounts mid-drag.
  React.useEffect(() => {
    return () => dragCleanupRef.current?.();
  }, []);

  const handleMouseDown = React.useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      isDragging.current = true;
      const startY = e.clientY;
      const startHeight = height;

      const handleMouseMove = (moveEvent: MouseEvent) => {
        const delta = startY - moveEvent.clientY;
        const newHeight = Math.min(
          MAX_HEIGHT,
          Math.max(MIN_HEIGHT, startHeight + delta),
        );
        setHeight(newHeight);
      };

      const handleMouseUp = () => {
        isDragging.current = false;
        dragCleanupRef.current = null;
        document.removeEventListener("mousemove", handleMouseMove);
        document.removeEventListener("mouseup", handleMouseUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };

      dragCleanupRef.current = () => {
        document.removeEventListener("mousemove", handleMouseMove);
        document.removeEventListener("mouseup", handleMouseUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };

      document.addEventListener("mousemove", handleMouseMove);
      document.addEventListener("mouseup", handleMouseUp);
      document.body.style.cursor = "row-resize";
      document.body.style.userSelect = "none";
    },
    [height],
  );

  // Record the path when the panel becomes visible
  const wasVisible = React.useRef(false);
  React.useEffect(() => {
    if (panelOpen && !wasVisible.current) {
      setResultsPanelPath(location.pathname);
    }
    wasVisible.current = panelOpen;
  }, [panelOpen, location.pathname, setResultsPanelPath]);

  const hasContent =
    result !== null ||
    subscribeState.isStreaming ||
    subscribeState.columns.length > 0;

  const isVisible = panelOpen && hasContent;

  // Sync the panel height to the atom so the editor can add bottom padding
  React.useEffect(() => {
    setResultsPanelHeight(isVisible ? height : 0);
    return () => setResultsPanelHeight(0);
  }, [isVisible, height, setResultsPanelHeight]);

  if (!isVisible) {
    return null;
  }

  const handleDismiss = () => {
    setPanelOpen(false);
  };

  const handleStopSubscribe = () => {
    stopSubscribe?.();
  };

  return (
    <Flex
      direction="column"
      height={`${height}px`}
      bg={colors.background.primary}
      borderTopWidth="1px"
      borderColor={colors.border.primary}
      position="absolute"
      bottom="0"
      left="0"
      right="0"
      zIndex={10}
      boxShadow="0 -2px 8px rgba(0,0,0,0.1)"
    >
      {/* Drag handle */}
      <Box
        position="absolute"
        top="0"
        left="0"
        right="0"
        height="4px"
        cursor="row-resize"
        onMouseDown={handleMouseDown}
        _hover={{ bg: colors.accent.purple }}
        transition="background 0.15s"
        zIndex={1}
      />
      <Box flex="1" overflow="auto">
        <ResultsPanel
          subscribeState={subscribeState}
          onStopSubscribe={handleStopSubscribe}
          onDismiss={handleDismiss}
          onExecute={executeFromWorksheet ?? noop}
        />
      </Box>
    </Flex>
  );
};

export default FloatingResultPanel;
