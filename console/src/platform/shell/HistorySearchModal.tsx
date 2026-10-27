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
  Button,
  Grid,
  GridItem,
  Input,
  Kbd,
  List,
  ListItem,
  ModalBody,
  ModalContent,
  ModalOverlay,
  Show,
  Text,
  useColorMode,
  useTheme,
} from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { useAtom } from "jotai";
import React, {
  ElementRef,
  forwardRef,
  useEffect,
  useRef,
  useState,
} from "react";
import scrollIntoView from "scroll-into-view-if-needed";

import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import Highlight from "~/components/Highlight";
import { Modal } from "~/components/Modal";
import { NAV_MIN_HEIGHT_PX, NAV_MIN_WIDTH_PX } from "~/layouts/constants";
import { MaterializeTheme } from "~/theme";
import { FocusableElement } from "~/utils/focusableElement";

import { NAVBAR_HEIGHT_PX as SHELL_NAVBAR_HEIGHT_PX } from "./constants";
import { CommandListEntry, filteredShellHistory } from "./store/history";

const HistoryList = ({
  commands,
  selectedItem,
  onHover,
  onSelect,
  searchQuery,
  scrollOnChange,
}: {
  commands: CommandListEntry[];
  scrollOnChange: boolean;
  selectedItem: number;
  onHover: (ix: number) => void;
  onSelect: (command: CommandListEntry) => void;
  searchQuery: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { colorMode } = useColorMode();
  const highlightBgColor =
    colorMode === "light" ? colors.purple[200] : colors.purple[700];
  const highlightFgColor =
    colorMode === "light" ? "unset" : colors.foreground.primary;

  const activeColor =
    colorMode === "light" ? colors.gray[200] : colors.gray[800];

  const buttonRefs = useRef(new Map<string, HTMLElement>());

  useEffect(() => {
    const id = commands[selectedItem]?.id;
    if (!id) {
      Sentry.captureException(new Error("No historical command found"), {
        extra: {
          numCommands: commands.length,
          index: selectedItem,
        },
      });
    }
    const ref = buttonRefs.current.get(id);
    if (!ref) {
      Sentry.captureException(new Error("No historical button ref found"), {
        extra: {
          numCommands: commands.length,
          index: selectedItem,
          id,
        },
      });
      return;
    }
    if (scrollOnChange) {
      scrollIntoView(ref, {
        block: "nearest",
        behavior: "smooth",
        scrollMode: "if-needed",
      });
    }
  }, [selectedItem, scrollOnChange, commands]);
  return (
    <List
      width="100%"
      height="100%"
      overflow="auto"
      data-testid="history-list"
      spacing="1"
    >
      {commands.map((command, ix) => {
        const selected = selectedItem === ix;
        return (
          <ListItem key={`${command.id}.${ix}`}>
            <Button
              role="option"
              aria-selected={selected ? true : undefined}
              backgroundColor={selected ? activeColor : "unset"}
              onClick={() => onSelect(command)}
              onMouseOver={() => onHover(ix)}
              variant="ghost"
              width="100%"
              textStyle="monospace"
              justifyContent="flex-start"
              transition="all"
              title={command.command}
              ref={(element) => {
                if (element !== null)
                  buttonRefs.current.set(command.id, element);
              }}
            >
              <Text overflow="hidden" textOverflow="ellipsis">
                <Highlight
                  query={searchQuery}
                  styles={{ bg: highlightBgColor, color: highlightFgColor }}
                  caseSensitive
                >
                  {command.label}
                </Highlight>
              </Text>
            </Button>
          </ListItem>
        );
      })}
    </List>
  );
};

export const HistorySearch = forwardRef(
  (
    { onSelect }: { onSelect: (command: string) => void },
    ref: React.Ref<ElementRef<"input">>,
  ) => {
    const [searchQuery, setSearchQuery] = useState("");
    const [commands] = useAtom(filteredShellHistory(searchQuery));
    const [selectedCommandIdx, setSelectedCommandIdx] = useState(0);
    const [isKeyboardSelecting, setIsKeyboardSelecting] = useState(false);

    const selectAndClose = ({ command }: CommandListEntry) => {
      onSelect(command);
    };

    const setKeyboardSelectedCommandIdx = (
      state: Parameters<typeof setSelectedCommandIdx>[0],
    ) => {
      setIsKeyboardSelecting(true);
      setSelectedCommandIdx(state);
    };

    return (
      <Grid
        templateAreas={`"header header"
          "list preview"
          "footer footer"`}
        gridTemplateColumns={{ base: "1fr", sm: "repeat(2, minmax(0, 1fr))" }}
        gridTemplateRows="auto 400px auto"
        gap={4}
        minHeight="400px"
      >
        <GridItem area="header">
          <Input
            data-testid="filter-input"
            ref={ref}
            value={searchQuery}
            onChange={(event) => {
              setKeyboardSelectedCommandIdx(0);
              setSearchQuery(event.target.value);
            }}
            variant="unstyled"
            border="none"
            size="lg"
            textStyle="monospace"
            fontSize="large"
            placeholder="Type to search your history"
            onKeyDown={(event) => {
              if (event.metaKey || event.shiftKey || event.ctrlKey) {
                return false;
              }
              switch (event.key) {
                case "ArrowUp":
                  setKeyboardSelectedCommandIdx((ix) => Math.max(0, ix - 1));
                  break;
                case "ArrowDown":
                  setKeyboardSelectedCommandIdx((ix) =>
                    Math.min(commands.length - 1, ix + 1),
                  );
                  break;
                case "Enter":
                  selectAndClose(commands[selectedCommandIdx]);
                  break;
                default:
                  return false;
              }
              event.preventDefault();
              event.stopPropagation();
              return true;
            }}
          />
        </GridItem>
        <GridItem area="list">
          {commands.length !== 0 ? (
            <HistoryList
              commands={commands}
              searchQuery={searchQuery}
              selectedItem={selectedCommandIdx}
              onHover={(ix) => {
                setIsKeyboardSelecting(false);
                setSelectedCommandIdx(ix);
              }}
              onSelect={selectAndClose}
              scrollOnChange={isKeyboardSelecting}
            />
          ) : (
            <Box
              textStyle="text-ui-reg"
              fontStyle="italic"
              data-testid="empty-message"
            >
              {searchQuery === ""
                ? "No queries in your history. Write some SQL!"
                : "No matching queries"}
            </Box>
          )}
        </GridItem>
        <Show above="sm">
          <GridItem area="preview">
            <Box p={2} overflow="auto" height="100%">
              <ReadOnlyCommandBlock
                value={commands[selectedCommandIdx]?.command}
                highlight
                highlightTerm={searchQuery}
              />
            </Box>
          </GridItem>
        </Show>
        <GridItem area="footer">
          <Box px={2} textAlign="center" position="relative">
            <Kbd>&uarr;</Kbd>
            <Kbd>&darr;</Kbd> to choose, and <Kbd>Enter</Kbd> to select.
          </Box>
        </GridItem>
      </Grid>
    );
  },
);

const HistorySearchModal = ({
  isOpen,
  onClose,
  onSelect,
  finalFocusRef,
}: {
  isOpen: boolean;
  onClose: () => void;
  onSelect: (command: string) => void;
  finalFocusRef: React.RefObject<FocusableElement>;
}) => {
  const searchQueryRef = useRef<ElementRef<"input">>(null);
  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      initialFocusRef={searchQueryRef}
      finalFocusRef={finalFocusRef}
      size="3xl"
      motionPreset="slideInTop"
    >
      <ModalOverlay />
      <ModalContent
        marginLeft={{ base: "unset", lg: `${NAV_MIN_WIDTH_PX}px` }}
        marginTop={{
          base: `${SHELL_NAVBAR_HEIGHT_PX + NAV_MIN_HEIGHT_PX + 16}px`,
          lg: `${SHELL_NAVBAR_HEIGHT_PX + 16}px`,
        }}
      >
        <ModalBody
          id="shellHistory"
          onClick={() =>
            !!searchQueryRef.current && searchQueryRef.current.focus()
          }
        >
          {/* Force an unmount of the element to ensure no computation happens when not displayed */}
          {isOpen && <HistorySearch ref={searchQueryRef} onSelect={onSelect} />}
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default HistorySearchModal;
