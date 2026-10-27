// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import "./crt.css";

import { Grid, GridItem, useDisclosure, VStack } from "@chakra-ui/react";
import { TransactionSpec } from "@codemirror/state";
import { useAtom } from "jotai";
import { RESET, useAtomCallback } from "jotai/utils";
import React, { useCallback, useEffect, useMemo, useRef } from "react";

import { useSegment } from "~/analytics/segment";
import { EditorCommand, EditorEvent } from "~/components/CommandBlock";
import {
  useGetView,
  useViewDispatch,
} from "~/components/CommandBlock/provider";
import { useCancelQuery } from "~/queries/cancelQuery";
import { exhaustiveGuard } from "~/util";
import { FocusableElement } from "~/utils/focusableElement";

import { TUTORIAL_WIDTH } from "./constants";
import { Audio } from "./EasterEgg";
import { clearListItemHeights } from "./heightByListItem";
import HistorySearchModal from "./HistorySearchModal";
import PlanInsightsDrawer from "./plan-insights/PlanInsightsDrawer";
import RunCommandButton from "./RunCommandButton";
import ShellHeader from "./ShellHeader";
import ShellPrompt from "./ShellPrompt";
import ShellVirtualizedList from "./ShellVirtualizedList";
import splitQueryString from "./splitQueryString";
import {
  nextPrompt as nextPromptJotaiCallback,
  previousPrompt as previousPromptJotaiCallback,
  saveClearPrompt,
  setPromptValue,
} from "./store/prompt";
import {
  createDefaultLocalCommandOutput,
  historyIdsAtom,
  historyItemAtom,
  shellStateAtom,
} from "./store/shell";
import Tutorial from "./Tutorial";
import { useShellVirtualizedList } from "./useShellVirtualizedList";
import useShellWebsocket, { shellWsEmitterEvents } from "./useShellWebsocket";

function shouldNavigatePrevious(event: EditorEvent) {
  return event.cursor.line === 1;
}

function shouldNavigateNext(event: EditorEvent) {
  return event.cursor.line === event.state.lineCount;
}

const Shell = () => {
  const [shellState, setShellState] = useAtom(shellStateAtom);

  const {
    send,
    commitToHistory,
    isSocketError,
    isSocketAvailable,
    cacheCommand,
    on,
    off,
  } = useShellWebsocket();

  const { scrollToBottom, setShouldAutoScroll } = useShellVirtualizedList();

  const { mutate: cancelQueryMutation } = useCancelQuery();

  const getView = useGetView();
  const viewDispatch = useViewDispatch();

  const { track } = useSegment();

  const cancelQuery = () => {
    const { connectionId } = shellState;
    if (!connectionId) {
      return;
    }
    cancelQueryMutation({ connectionId });
  };

  const clearHistory = useAtomCallback(
    React.useCallback(
      (_get, set) =>
        set(historyIdsAtom, (currentHistoryIds) => {
          currentHistoryIds.forEach((historyId) =>
            set(historyItemAtom(historyId), RESET),
          );
          clearListItemHeights();
          return [];
        }),
      [],
    ),
  );

  const previousPrompt = useAtomCallback(previousPromptJotaiCallback);
  const nextPrompt = useAtomCallback(nextPromptJotaiCallback);
  const setPrompt = useAtomCallback(setPromptValue);
  const dispatchReplacePrompt = useCallback(
    (value: string) => {
      const view = getView();
      if (!view) return;
      const updateEvent: TransactionSpec = {
        changes: { from: 0, to: view.state.doc.length, insert: value },
        selection: { anchor: value.length },
      };
      viewDispatch(updateEvent);
    },
    [getView, viewDispatch],
  );

  // Scroll to the bottom whenever we update the Shell's history list.
  useEffect(() => {
    on(shellWsEmitterEvents.UPDATE_HISTORY, () => {
      scrollToBottom();
    });

    return () => {
      off(shellWsEmitterEvents.UPDATE_HISTORY);
    };
  }, [on, off, scrollToBottom]);

  const showHelp = useCallback(
    (commands: Map<string, SlashCommandEntry>) => {
      const helpItems: Array<[string, string]> = [];
      for (const [command, { display, description }] of commands) {
        if (!display) continue;
        helpItems.push([`\\${command}`, description]);
      }
      commitToHistory(
        createDefaultLocalCommandOutput({
          command: "\\help",
          commandResults: helpItems,
        }),
      );
    },
    [commitToHistory],
  );

  const clearPrompt = useAtomCallback(saveClearPrompt);
  const socketReadyForQuery = shellState.webSocketState === "readyForQuery";

  const runCommand = useCallback(
    async (command: string, options?: { trackingMethod: string }) => {
      if (
        !socketReadyForQuery ||
        !isSocketAvailable ||
        isSocketError ||
        command.length === 0
      ) {
        return;
      }
      setShouldAutoScroll(true);

      const queries = splitQueryString(command);

      send({ queries, originalCommand: command });
      if (options?.trackingMethod) {
        track("Shell Command Executed", { method: options.trackingMethod });
      }
    },
    [
      socketReadyForQuery,
      isSocketAvailable,
      isSocketError,
      send,
      setShouldAutoScroll,
      track,
    ],
  );

  type SlashCommandEntry = {
    callback: () => void;
    description: string;
    display: boolean;
  };

  const slashCommands: Map<string, SlashCommandEntry> = useMemo(
    () =>
      new Map([
        [
          "hacktheplanet",
          {
            callback: () =>
              setShellState((currentState) => ({
                ...currentState,
                crtEnabled: !currentState.crtEnabled,
              })),
            display: false,
            description: "Hack the planet!",
          },
        ],
        [
          "help",
          {
            callback: () => showHelp(slashCommands),
            display: true,
            description: "Show this help message",
          },
        ],
        [
          "clear",
          {
            callback: clearHistory,
            display: true,
            description: "Clear your displayed history",
          },
        ],
      ]),
    [clearHistory, setShellState, showHelp],
  );

  const {
    isOpen: isHistoryOpen,
    onOpen: onHistoryOpen,
    onClose: onHistoryClose,
  } = useDisclosure();

  const handlePromptInput = useCallback(
    (e: EditorEvent) => {
      if (e.key === "ArrowUp" && shouldNavigatePrevious(e)) {
        previousPrompt(dispatchReplacePrompt);
        return true;
      }

      if (e.key === "ArrowDown" && shouldNavigateNext(e)) {
        nextPrompt(dispatchReplacePrompt);
        return true;
      }

      if (e.key === "Enter") {
        const text = e.state.text.trim();

        if (text && text.at(-1) === ";") {
          runCommand(text, { trackingMethod: "keyboard" });
          clearPrompt(dispatchReplacePrompt);
          return true;
        }

        if (text.startsWith("\\")) {
          setShouldAutoScroll(true);

          const handler = slashCommands.get(text.substring(1));
          if (handler) {
            handler.callback();
            cacheCommand(text);
            clearPrompt(dispatchReplacePrompt);
            return true;
          }
        }
      }
      return false;
    },
    [
      cacheCommand,
      clearPrompt,
      dispatchReplacePrompt,
      previousPrompt,
      nextPrompt,
      runCommand,
      setShouldAutoScroll,
      slashCommands,
    ],
  );

  const handlePromptCommand = useCallback(
    (editorCommand: EditorCommand) => {
      switch (editorCommand.command) {
        case "find":
          track("Shell History Search Opened", {
            method: "keyboard",
          });
          onHistoryOpen();
          return true;
        case "submit":
          runCommand(editorCommand.state.text, { trackingMethod: "shortcut" });
          clearPrompt(dispatchReplacePrompt);
          return true;
        default:
          exhaustiveGuard(editorCommand);
      }
    },
    [onHistoryOpen, track, runCommand, dispatchReplacePrompt, clearPrompt],
  );

  const shellPromptRef = useRef<FocusableElement>(null);

  const focusShell = useCallback(() => shellPromptRef.current?.focus(), []);

  return (
    <Grid
      templateAreas={`
      "header header"
      "shell tutorial"
      `}
      gridTemplateRows="auto minmax(0,1fr)"
      gridTemplateColumns={`minmax(0,1fr) ${shellState.tutorialVisible ? TUTORIAL_WIDTH : 0}`}
      width="100%"
      data-testid="shell"
    >
      <ShellHeader runCommand={runCommand} />
      <GridItem area="shell" position="relative">
        <VStack height="100%" width="100%" spacing={0}>
          <ShellVirtualizedList
            width="100%"
            flexGrow="1"
            flexShrink="0"
            minHeight="0"
          />
          <ShellPrompt
            flexGrow="0"
            flexShrink="0"
            minHeight="32"
            maxHeight="720"
            width="100%"
            onCommandBlockKeyDown={handlePromptInput}
            onCommandBlockCommand={handlePromptCommand}
            ref={shellPromptRef}
            onClick={focusShell}
            onHistoryOpen={onHistoryOpen}
          />
        </VStack>
        <RunCommandButton
          runCommand={(command) =>
            runCommand(command, { trackingMethod: "button" })
          }
          cancelQuery={cancelQuery}
          position="absolute"
          bottom="3"
          right="3"
        />
      </GridItem>
      {shellState.tutorialVisible && (
        <Tutorial
          runCommand={(command) =>
            runCommand(command, { trackingMethod: "tutorial" })
          }
        />
      )}
      <HistorySearchModal
        isOpen={isHistoryOpen}
        onClose={onHistoryClose}
        onSelect={(command) => {
          onHistoryClose();
          setPrompt(command, dispatchReplacePrompt);
        }}
        finalFocusRef={shellPromptRef}
      />
      {shellState.crtEnabled && <Audio />}
      <PlanInsightsDrawer />
    </Grid>
  );
};

export default Shell;
