// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon, DownloadIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  HStack,
  IconButton,
  Text,
  useTheme,
} from "@chakra-ui/react";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import React, { useCallback, useMemo, useState } from "react";

import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { CopyButton } from "~/components/copyableComponents";
import formatRows from "~/components/formatRows";
import SqlSelectTable, { TablePagination } from "~/components/SqlSelectTable";
import type { MaterializeTheme } from "~/theme";

import { PAGE_SIZE } from "./constants";
import type { SubscribeState } from "./store";
import {
  type QueryResult,
  worksheetExecutionAtom,
  worksheetResultAtom,
  worksheetStashedSqlResultAtom,
} from "./store";
import SubscribeView from "./SubscribeView";

/**
 * Props for the results panel. Shows either a SUBSCRIBE view (when streaming
 * or when subscribe data exists) or a table of row-returning query results.
 */
export interface ResultsPanelProps {
  /** Current state of a SUBSCRIBE query, if one is active or has completed. */
  subscribeState: SubscribeState;
  /** Cancels an active SUBSCRIBE stream. */
  onStopSubscribe: () => void;
  /** Closes the results panel entirely. */
  onDismiss: () => void;
  /** Executes a SQL statement through the worksheet WebSocket. */
  onExecute: (sql: string, kind: string, offset: number) => void;
}

/**
 * Bottom panel that displays query results, SUBSCRIBE streams, or SHOW CREATE
 * output. Automatically switches between SubscribeView, SqlView, and
 * ResultTable based on execution state and result display mode.
 */
const ResultsPanel = ({
  subscribeState,
  onStopSubscribe,
  onDismiss,
  onExecute,
}: ResultsPanelProps) => {
  const result = useAtomValue(worksheetResultAtom);
  const execution = useAtomValue(worksheetExecutionAtom);

  const hasSubscribeData = subscribeState.columns.length > 0;

  if (
    execution.status === "streaming" ||
    subscribeState.isStreaming ||
    hasSubscribeData
  ) {
    return (
      <SubscribeView
        subscribeState={subscribeState}
        onStop={onStopSubscribe}
        onDismiss={onDismiss}
      />
    );
  }

  if (!result) {
    return null;
  }

  if (result.displayMode === "sql") {
    return <SqlView result={result} onDismiss={onDismiss} onExecute={onExecute} />;
  }

  if (result.displayMode === "text") {
    return <TextView result={result} onDismiss={onDismiss} />;
  }

  return <ResultTable result={result} onDismiss={onDismiss} />;
};

/** Maps SHOW CREATE kinds to their EXPLAIN prefix for objects that support it. */
const EXPLAINABLE_KINDS: Record<string, string> = {
  show_create_materialized_view: "EXPLAIN MATERIALIZED VIEW",
  show_create_index: "EXPLAIN INDEX",
};

/** Renders a SHOW CREATE result with an optional Explain button for MVs and indexes. */
const SqlView = ({
  result,
  onDismiss,
  onExecute,
}: {
  result: QueryResult;
  onDismiss: () => void;
  onExecute: (sql: string, kind: string, offset: number) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const setStashedSqlResult = useSetAtom(worksheetStashedSqlResultAtom);
  const sqlText = String(result.rows[0]?.[0] ?? "");

  const explainPrefix = result.kind ? EXPLAINABLE_KINDS[result.kind] : null;
  const explainQuery = useMemo(
    () =>
      explainPrefix && result.objectName
        ? `${explainPrefix} ${result.objectName}`
        : null,
    [explainPrefix, result.objectName],
  );

  const handleExplainClick = useCallback(() => {
    if (explainQuery) {
      setStashedSqlResult(result);
      onExecute(explainQuery, "explain_plan", 0);
    }
  }, [explainQuery, onExecute, result, setStashedSqlResult]);

  const handleDownload = useCallback(() => {
    const blob = new Blob([sqlText], { type: "text/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "show_create.sql";
    a.click();
    URL.revokeObjectURL(url);
  }, [sqlText]);

  return (
    <Box height="100%" overflow="auto">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        justifyContent="space-between"
      >
        <HStack spacing="2">
          {explainQuery && (
            <Button size="xs" variant="ghost" onClick={handleExplainClick}>
              Explain
            </Button>
          )}
          <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
            {result.commandComplete}
          </Text>
        </HStack>
        <HStack spacing="1">
          <CopyButton contents={sqlText} size="xs" />
          <IconButton
            icon={<DownloadIcon boxSize="3" />}
            aria-label="Download SQL"
            onClick={handleDownload}
            variant="ghost"
            size="xs"
          />
          <IconButton
            icon={<CloseIcon boxSize="2.5" />}
            aria-label="Dismiss results"
            onClick={onDismiss}
            variant="ghost"
            size="xs"
          />
        </HStack>
      </HStack>
      <Box p="2">
        <ReadOnlyCommandBlock value={sqlText} lineNumbers />
      </Box>
    </Box>
  );
};

/** Renders an EXPLAIN result as plain monospace text with copy/download actions. */
const TextView = ({
  result,
  onDismiss,
}: {
  result: QueryResult;
  onDismiss: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const text = String(result.rows[0]?.[0] ?? "");
  const [stashedSqlResult, setStashedSqlResult] = useAtom(
    worksheetStashedSqlResultAtom,
  );
  const setResult = useSetAtom(worksheetResultAtom);

  const handleBackToSql = useCallback(() => {
    if (stashedSqlResult) {
      setResult(stashedSqlResult);
      setStashedSqlResult(null);
    }
  }, [stashedSqlResult, setResult, setStashedSqlResult]);

  const handleDownload = useCallback(() => {
    const blob = new Blob([text], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "explain.txt";
    a.click();
    URL.revokeObjectURL(url);
  }, [text]);

  return (
    <Box height="100%" overflow="auto">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        justifyContent="space-between"
      >
        <HStack spacing="2">
          {stashedSqlResult && (
            <Button size="xs" variant="ghost" onClick={handleBackToSql}>
              SQL
            </Button>
          )}
          <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
            {result.commandComplete}
          </Text>
        </HStack>
        <HStack spacing="1">
          <CopyButton contents={text} size="xs" />
          <IconButton
            icon={<DownloadIcon boxSize="3" />}
            aria-label="Download text"
            onClick={handleDownload}
            variant="ghost"
            size="xs"
          />
          <IconButton
            icon={<CloseIcon boxSize="2.5" />}
            aria-label="Dismiss results"
            onClick={onDismiss}
            variant="ghost"
            size="xs"
          />
        </HStack>
      </HStack>
      <Box
        p="4"
        fontFamily="mono"
        fontSize="sm"
        whiteSpace="pre"
        overflowX="auto"
      >
        {text}
      </Box>
    </Box>
  );
};

/** Renders row-returning query results (SELECT, SHOW, EXPLAIN) in a paginated table. */
const ResultTable = ({
  result,
  onDismiss,
}: {
  result: QueryResult;
  onDismiss: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [currentPage, setCurrentPage] = useState(0);

  React.useEffect(() => {
    setCurrentPage(0);
  }, [result]);

  const colNames = result.columns.map((c) => c.name);
  const formattedRows = useMemo(
    () => formatRows(result.columns, result.rows),
    [result.columns, result.rows],
  );

  const totalPages = Math.ceil(formattedRows.length / PAGE_SIZE);
  const startIndex = currentPage * PAGE_SIZE;
  const endIndex = Math.min(startIndex + PAGE_SIZE, formattedRows.length);
  const paginatedRows = formattedRows.slice(startIndex, endIndex);

  return (
    <Box height="100%" overflow="auto" p="2">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        justifyContent="space-between"
      >
        <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
          Results — {result.rows.length} row
          {result.rows.length !== 1 ? "s" : ""} in {result.durationMs}ms
        </Text>
        <HStack>
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            {result.commandComplete}
          </Text>
          <IconButton
            icon={<CloseIcon boxSize="2.5" />}
            aria-label="Dismiss results"
            onClick={onDismiss}
            variant="ghost"
            size="xs"
          />
        </HStack>
      </HStack>
      <SqlSelectTable
        colNames={colNames}
        cols={result.columns}
        paginatedRows={paginatedRows}
        rows={result.rows}
      />
      {totalPages > 1 && (
        <TablePagination
          totalPages={totalPages}
          totalNumRows={formattedRows.length}
          currentPage={currentPage}
          startIndex={startIndex}
          endIndex={endIndex}
          pageSize={PAGE_SIZE}
          prevEnabled={currentPage > 0}
          nextEnabled={currentPage < totalPages - 1}
          isFollowing={null}
          onToggleFollow={() => {}}
          onPrevPage={() => setCurrentPage((p) => Math.max(0, p - 1))}
          onNextPage={() =>
            setCurrentPage((p) => Math.min(totalPages - 1, p + 1))
          }
          px="4"
          py="2"
        />
      )}
    </Box>
  );
};

export default ResultsPanel;
