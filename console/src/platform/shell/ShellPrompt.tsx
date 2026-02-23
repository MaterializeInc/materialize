// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SearchIcon } from "@chakra-ui/icons";
import {
  chakra,
  Code,
  HStack,
  Spinner,
  StackProps,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import React, { forwardRef, memo, useCallback, useEffect, useRef } from "react";

import { useSegment } from "~/analytics/segment";
import CommandBlock, {
  EditorCommand,
  EditorEvent,
} from "~/components/CommandBlock";
import { DraggableEvent, useDrag } from "~/hooks/useDrag";
import { MaterializeTheme } from "~/theme";
import { controlOrCommand } from "~/util";
import { FocusableElement } from "~/utils/focusableElement";

import CommandChevron from "./CommandChevron";
import { isCommandProcessing as webSocketCommandProcessing } from "./machines/webSocketFsm";
import { setPromptValue } from "./store/prompt";
import { shellStateAtom } from "./store/shell";

type ShellPromptProps = StackProps & {
  onCommandBlockKeyDown: (event: EditorEvent) => boolean;
  onCommandBlockCommand: (command: EditorCommand) => boolean;
  onHistoryOpen: () => void;
};

const ShellPromptHandle = ({
  onDrag,
}: {
  onDrag?: (event: PointerEvent, draggableEvent: DraggableEvent) => void;
}) => {
  const handleRef = useRef<HTMLButtonElement | null>(null);
  useDrag({
    ref: handleRef,
    onDrag: (...rest) => {
      onDrag?.(...rest);
    },
    onStart: () => {
      document.body.style.setProperty("cursor", "ns-resize");
    },
    onStop: () => {
      document.body.style.removeProperty("cursor");
    },
  });

  useEffect(() => {
    return () => {
      // In case this component mounts before onStop is called
      document.body.style.removeProperty("cursor");
    };
  }, []);
  return (
    <chakra.button
      height="3"
      width="100%"
      position="absolute"
      backgroundColor="transparent"
      zIndex={1}
      top="-3"
      ref={handleRef}
      cursor="ns-resize"
    />
  );
};

const CODE_PROPS = {
  width: "100%",
  height: "100%",
  overflow: "auto",
};

const ShellPrompt = forwardRef<FocusableElement, ShellPromptProps>(
  (
    { onCommandBlockCommand, onCommandBlockKeyDown, onHistoryOpen, ...rest },
    promptRef,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();
    const setPrompt = useAtomCallback(setPromptValue);

    const containerRef = useRef<HTMLDivElement | null>(null);

    const [{ webSocketState }] = useAtom(shellStateAtom);

    const onPromptResize = useCallback(
      (_: PointerEvent, draggableEvent: DraggableEvent) => {
        if (!containerRef.current) {
          return;
        }
        const curHeight = containerRef.current.getBoundingClientRect().height;

        const newPromptHeight = curHeight - (draggableEvent.pointDelta?.y ?? 0);
        containerRef.current.style.height = `${newPromptHeight}px`;
      },
      [],
    );

    const handleChange = useCallback(
      (newPrompt: string) => {
        setPrompt(newPrompt);
      },
      [setPrompt],
    );

    const isCommandProcessing =
      webSocketState && webSocketCommandProcessing(webSocketState);

    return (
      <HStack
        borderTop="1px"
        borderColor={colors.border.secondary}
        ref={containerRef}
        position="relative"
        {...rest}
      >
        <ShellPromptHandle onDrag={onPromptResize} />
        <HStack alignItems="flex-start" p="3" width="100%" height="100%">
          {isCommandProcessing ? (
            <HStack color={colors.foreground.tertiary} cursor="not-allowed">
              <Spinner size="sm" thickness="1.5px" speed="0.65s" />
              <Code>Command is processing</Code>
            </HStack>
          ) : (
            <>
              <VStack height="100%" justifyContent="space-between">
                <CommandChevron />
                <HistorySearchActionButton onHistoryOpen={onHistoryOpen} />
              </VStack>
              <CommandBlock
                onChange={handleChange}
                onCommand={onCommandBlockCommand}
                onKeyDown={onCommandBlockKeyDown}
                autoFocus={true}
                ref={promptRef}
                placeholder="-- write your query here"
                containerProps={CODE_PROPS}
              />
            </>
          )}
        </HStack>
      </HStack>
    );
  },
);

const HistorySearchActionButton = memo(
  ({ onHistoryOpen }: { onHistoryOpen: () => void }) => {
    const { colors } = useTheme<MaterializeTheme>();
    const kbdModifier = controlOrCommand();
    const { track } = useSegment();

    return (
      <Tooltip label={`Search your query history (${kbdModifier} + g)`}>
        <SearchIcon
          onClick={() => {
            track("Shell History Search Opened", {
              method: "button",
            });
            onHistoryOpen();
          }}
          cursor="pointer"
          color={colors.foreground.secondary}
        />
      </Tooltip>
    );
  },
);

export default ShellPrompt;
