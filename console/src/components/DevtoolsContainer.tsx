// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Center,
  HStack,
  Switch,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import React, { Suspense, useRef, useState } from "react";

import { useDrag } from "~/hooks/useDrag";
import { getStore } from "~/jotai";
import { DEVTOOL_BUTTONS_Z_INDEX } from "~/layouts/zIndex";
import { MaterializeTheme } from "~/theme";

import { MaterializeLogo } from "./MaterializeLogo";

const SwitchStackModal = React.lazy(() => import("./SwitchStackModal"));

const LazyJotaiDevtools = React.lazy(() =>
  import("jotai-devtools").then(({ DevTools }) => ({
    default: () => <DevTools position="bottom-right" store={getStore()} />,
  })),
);

const DEVTOOLS_POSITION_KEY = "devtools-position";
const BUTTON_SIZE = 36;

const clamp = (value: number, min: number, max: number) =>
  Math.max(min, Math.min(max, value));

const getStoredPosition = (): { x: number; y: number } => {
  try {
    const stored = localStorage.getItem(DEVTOOLS_POSITION_KEY);
    if (stored) return JSON.parse(stored);
  } catch {
    // ignore
  }
  return { x: 20, y: window.innerHeight - 140 };
};

export const DevtoolsContainer = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const [isExpanded, setIsExpanded] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [showRQDevtools, setShowRQDevtools] = useState(false);
  const [showJotaiDevtools, setShowJotaiDevtools] = useState(false);
  const [position, setPosition] = useState(getStoredPosition);

  const buttonRef = useRef<HTMLDivElement>(null);

  useDrag({
    ref: buttonRef,
    onStart: () => setIsDragging(true),
    onDrag: (_, { pointDelta }) => {
      if (!pointDelta) return;
      setPosition((prev) => ({
        x: clamp(prev.x + pointDelta.x, 0, window.innerWidth - BUTTON_SIZE),
        y: clamp(prev.y + pointDelta.y, 0, window.innerHeight - BUTTON_SIZE),
      }));
    },
    onStop: () => {
      setIsDragging(false);
      setPosition((pos) => {
        localStorage.setItem(DEVTOOLS_POSITION_KEY, JSON.stringify(pos));
        return pos;
      });
    },
  });

  return (
    <>
      {(showRQDevtools || showJotaiDevtools) && (
        <VStack
          position="fixed"
          bottom="20"
          right="4"
          zIndex={DEVTOOL_BUTTONS_Z_INDEX + 2}
          spacing="2"
          align="flex-end"
          sx={{
            ".jotai-devtools-trigger-button": {
              position: "relative !important",
              bottom: "unset !important",
              right: "unset !important",
              zIndex: "unset !important",
            },
          }}
        >
          {showRQDevtools && <ReactQueryDevtools buttonPosition="relative" />}
          {showJotaiDevtools && (
            <Suspense fallback={null}>
              <LazyJotaiDevtools />
            </Suspense>
          )}
        </VStack>
      )}

      <Box
        position="fixed"
        left={`${position.x}px`}
        top={`${position.y}px`}
        zIndex={DEVTOOL_BUTTONS_Z_INDEX}
        onMouseEnter={() => {
          if (!isDragging) setIsExpanded(true);
        }}
        onMouseLeave={() => setIsExpanded(false)}
      >
        <Center
          ref={buttonRef}
          width={`${BUTTON_SIZE}px`}
          height={`${BUTTON_SIZE}px`}
          borderRadius="full"
          bg={colors.background.primary}
          border="1px"
          borderColor={colors.border.secondary}
          boxShadow="md"
          cursor="grab"
          opacity={0.7}
          _hover={{ opacity: 1 }}
          _active={{ cursor: "grabbing" }}
          transition="opacity 0.2s"
        >
          <MaterializeLogo markOnly height="4" width="4" />
        </Center>

        {isExpanded && !isDragging && (
          <VStack
            position="absolute"
            bottom="100%"
            left="0"
            mb="1"
            p="3"
            bg={colors.background.primary}
            border="1px"
            borderColor={colors.border.primary}
            borderRadius="md"
            boxShadow="lg"
            spacing="3"
            align="stretch"
            minWidth="180px"
          >
            <Text
              fontSize="xs"
              fontWeight="600"
              color={colors.foreground.secondary}
            >
              Dev Tools
            </Text>

            <HStack justify="space-between">
              <Text fontSize="xs">React Query</Text>
              <Switch
                size="sm"
                isChecked={showRQDevtools}
                onChange={() => setShowRQDevtools((v) => !v)}
              />
            </HStack>

            <HStack justify="space-between">
              <Text fontSize="xs">Jotai</Text>
              <Switch
                size="sm"
                isChecked={showJotaiDevtools}
                onChange={() => setShowJotaiDevtools((v) => !v)}
              />
            </HStack>

            <Box borderTop="1px" borderColor={colors.border.primary} pt="2">
              <Suspense fallback={null}>
                <SwitchStackModal />
              </Suspense>
            </Box>
          </VStack>
        )}
      </Box>
    </>
  );
};
