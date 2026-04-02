// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import "./crt.css";
import "./aprilFools.css";

import { Grid, GridItem, useDisclosure, VStack } from "@chakra-ui/react";
import { TransactionSpec } from "@codemirror/state";
import { useAtom } from "jotai";
import { RESET, useAtomCallback } from "jotai/utils";
import React, { useCallback, useEffect, useMemo, useRef } from "react";

import { useSegment } from "~/analytics/segment";
import { useToast } from "~/hooks/useToast";
import { startCursorTrail } from "./aprilFoolsEffects";
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

// April Fools: escalating DROP toast chain
const DROP_CHAIN = [
  "Whoa, who hurt you?",
  "Okay but are you REALLY sure?",
  "This is your third warning. We're getting concerned.",
  "Fine. But we're telling your DBA.",
  "Your DBA has been notified. They are not happy.",
  "The database is crying now. Are you proud of yourself?",
  "...you're still here? Impressive commitment to destruction.",
];

function dropToastChain(
  toast: (opts: {
    description: string;
    status: string;
    isClosable: boolean;
    duration?: number;
  }) => void,
  step = 0,
) {
  if (step >= DROP_CHAIN.length) return;
  setTimeout(
    () => {
      toast({
        description: `⚠️ ${DROP_CHAIN[step]}`,
        status: "error",
        isClosable: true,
        duration: 4000,
      });
      dropToastChain(toast, step + 1);
    },
    step === 0 ? 0 : 2000,
  );
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
  const sentientToast = useToast({ duration: 5000 });

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

      // April Fools: Sentient SQL Linter
      const upper = command.toUpperCase();
      const linterQuips: string[] = [];
      if (/SELECT\s+\*/.test(upper))
        linterQuips.push(
          "SELECT *? This feels like a lack of commitment to specific columns.",
        );
      if (/\bJOIN\b/.test(upper))
        linterQuips.push(
          "Are these two tables even compatible? Have they been to counseling?",
        );
      if (/\bDROP\b/.test(upper)) dropToastChain(sentientToast);
      if (/\bLIMIT\b/.test(upper))
        linterQuips.push("Why restrict yourself? Dream bigger.");
      if (/\bDELETE\b/.test(upper))
        linterQuips.push(
          "Deleting data? In this economy?",
        );
      if (/\bTRUNCATE\b/.test(upper))
        linterQuips.push(
          "TRUNCATE? That's not assertive, that's unhinged.",
        );
      if (/\bWHERE\s+1\s*=\s*1\b/.test(upper))
        linterQuips.push(
          "WHERE 1=1... profound. Have you considered a career in philosophy?",
        );
      if (/\bGROUP\s+BY\b/.test(upper))
        linterQuips.push(
          "GROUP BY? Some of us prefer to be individuals.",
        );
      if (/\bORDER\s+BY\b/.test(upper))
        linterQuips.push("Imposing order on chaos. Bold move.");
      if (/\bSUBSCRIBE\b/.test(upper))
        linterQuips.push(
          "SUBSCRIBE? Don't forget to like and hit that bell icon.",
        );
      if (/\bCREATE\s+MATERIALIZED\s+VIEW\b/.test(upper))
        linterQuips.push(
          "Creating a materialized view? That's a big commitment. Are you ready for this?",
        );
      for (const quip of linterQuips) {
        sentientToast({
          description: `🤔 ${quip}`,
          status: "success",
          isClosable: true,
        });
      }

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

  // April Fools: global CSS animations + cursor trail
  useEffect(() => {
    document.body.classList.add("april-fools-active", "april-fools-gravity");
    const cleanupTrail = startCursorTrail();
    return () => {
      document.body.classList.remove(
        "april-fools-active",
        "april-fools-gravity",
      );
      cleanupTrail();
    };
  }, []);

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
