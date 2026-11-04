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
  HStack,
  IconButton,
  Text,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import { Extension } from "@codemirror/state";
import React, { useEffect, useRef, useState } from "react";

import { PlayIcon, TableIcon } from "~/icons";
import HistorySearchModal from "~/platform/shell/HistorySearchModal";
import { MonitoringIcon } from "~/svg/nav/MonitoringIcon";
import { MaterializeTheme } from "~/theme";

import {
  CellDirection,
  CellEditor,
  HistoryDirection,
  LineNumbering,
} from "./CellEditor";
import { CellFrame } from "./CellFrame";
import { lineCount } from "./cellLabels";
import { LiveIcon } from "./LiveIcon";
import {
  isQueryStatement,
  runsLive,
  unwrapSubscribe,
  wrapInSubscribe,
} from "./subscribeStatement";

export interface NewCellProps {
  /** Adds the SQL as a new cell and runs it, shown as a chart if asked. */
  onSubmit: (source: string, asChart: boolean) => void;
  /** Queries run before, newest first. */
  history: readonly string[];
  language: Extension;
  /** Continues the worksheet's line numbers. */
  lineNumbers: LineNumbering;
  onTab: (direction: CellDirection) => void;
}

const isMac =
  typeof navigator !== "undefined" &&
  /Mac|iPhone|iPad/.test(navigator.platform);
const MOD = isMac ? "⌘" : "Ctrl-";

/**
 * The empty cell at the end of a worksheet. Running it turns what was typed
 * into a cell, and it empties for the next one.
 */
export const NewCell = ({
  onSubmit,
  history,
  language,
  lineNumbers,
  onTab,
}: NewCellProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [draft, setDraft] = useState("");
  const [asChart, setAsChart] = useState(false);
  const historySearch = useDisclosure();

  // Shell-style recall: -1 is the draft being written, 0 the newest query.
  const historyIndexRef = useRef(-1);
  const unsentDraftRef = useRef("");
  const recalledRef = useRef<string>();

  const showRecalled = (value: string) => {
    recalledRef.current = value;
    setDraft(value);
  };

  const recall = (direction: HistoryDirection) => {
    const index = historyIndexRef.current;
    const next = direction === "previous" ? index + 1 : index - 1;
    if (next < -1 || next >= history.length) return false;
    if (index === -1) unsentDraftRef.current = draft;
    historyIndexRef.current = next;
    showRecalled(next === -1 ? unsentDraftRef.current : history[next]);
    return true;
  };

  const onEdit = (value: string) => {
    // Applying a recalled query reports back as a change; anything else is
    // the user editing, which leaves history navigation.
    if (value !== recalledRef.current) {
      historyIndexRef.current = -1;
      recalledRef.current = undefined;
    }
    setDraft(value);
  };

  const draftIsLive = runsLive(draft);
  const isQuery = isQueryStatement(draft) || draftIsLive;

  const submit = () => {
    if (!draft.trim()) return;
    onSubmit(draft, asChart && isQuery);
    historyIndexRef.current = -1;
    recalledRef.current = undefined;
    setDraft("");
  };

  const { onOpen: openHistorySearch } = historySearch;
  useEffect(() => {
    // NOTE: ⌘G is the browser's "find next", so it is taken over only while
    // a worksheet is open.
    const onKeyDown = (event: KeyboardEvent) => {
      if (
        (event.metaKey || event.ctrlKey) &&
        !event.shiftKey &&
        !event.altKey &&
        event.key.toLowerCase() === "g"
      ) {
        event.preventDefault();
        openHistorySearch();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [openHistorySearch]);

  return (
    <Box pt="2" pb="6" data-cell="new">
      <CellFrame
        barColor={colors.border.secondary}
        gutter={
          // Where the cells above have their collapse button.
          <Box width="4" />
        }
        actions={
          <IconButton
            aria-label="Add and run"
            title="Add as a cell and run it (Shift-Enter)"
            icon={<PlayIcon />}
            size="xs"
            variant="inline"
            minW="5"
            height="6"
            color={colors.accent.purple}
            isDisabled={!draft.trim()}
            onClick={submit}
          />
        }
        editor={
          <CellEditor
            value={draft}
            language={language}
            lineNumbers={{
              first: lineNumbers.first,
              digits: Math.max(
                lineNumbers.digits,
                String(lineNumbers.first + lineCount(draft) - 1).length,
              ),
            }}
            isDefaultInsertTarget
            placeholder="Write SQL and press Shift-Enter to add it as a cell"
            onChange={onEdit}
            onTab={onTab}
            onHistory={recall}
            onRun={submit}
          />
        }
        below={
          <HStack spacing="1" height="7" ml="-1">
            <IconButton
              aria-label={draftIsLive ? "Stop live updates" : "Make live"}
              aria-pressed={draftIsLive}
              title={
                draftIsLive
                  ? "Live. Click to remove the SUBSCRIBE."
                  : "Make live: wraps the query in SUBSCRIBE so results keep updating"
              }
              icon={<LiveIcon boxSize="3.5" />}
              size="xs"
              variant="inline"
              minW="5"
              color={
                draftIsLive ? colors.accent.green : colors.foreground.tertiary
              }
              isDisabled={!isQuery}
              onClick={() => {
                const next = draftIsLive
                  ? unwrapSubscribe(draft)
                  : wrapInSubscribe(draft);
                if (next !== undefined) setDraft(next);
              }}
            />
            <IconButton
              aria-label={asChart ? "Add as a table" : "Add as a chart"}
              aria-pressed={asChart}
              title={
                asChart
                  ? "Will show results as a chart. Click for a table."
                  : "Will show results as a table. Click for a chart."
              }
              icon={
                asChart ? (
                  <MonitoringIcon color="currentColor" />
                ) : (
                  <TableIcon />
                )
              }
              size="xs"
              variant="inline"
              minW="5"
              color={
                asChart ? colors.accent.purple : colors.foreground.tertiary
              }
              onClick={() => setAsChart((v) => !v)}
            />
            <Text
              textStyle="text-small"
              color={colors.foreground.tertiary}
              flex="1"
              noOfLines={1}
            >
              ↑ recalls earlier queries
            </Text>
            <Button
              size="xs"
              variant="inline"
              color={colors.foreground.secondary}
              fontWeight="400"
              title={`Search earlier queries (${MOD}G)`}
              isDisabled={history.length === 0}
              onClick={historySearch.onOpen}
            >
              History {MOD}G
            </Button>
          </HStack>
        }
      />
      <HistorySearchModal
        isOpen={historySearch.isOpen}
        onClose={historySearch.onClose}
        history={history}
        onSelect={(command) => {
          historySearch.onClose();
          historyIndexRef.current = -1;
          recalledRef.current = undefined;
          setDraft(command);
        }}
      />
    </Box>
  );
};
