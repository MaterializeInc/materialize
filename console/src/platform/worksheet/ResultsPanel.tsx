// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ChevronDownIcon, CloseIcon, DownloadIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  HStack,
  IconButton,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  Spinner,
  Text,
  Tooltip,
  useTheme,
} from "@chakra-ui/react";
import { useAtom, useAtomValue, useSetAtom } from "jotai";
import React, { useCallback, useMemo, useState } from "react";

import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { useAllClusters } from "~/store/allClusters";
import { CopyButton } from "~/components/copyableComponents";
import formatRows from "~/components/formatRows";
import SqlSelectTable, { TablePagination } from "~/components/SqlSelectTable";
import type { MaterializeTheme } from "~/theme";

import { PAGE_SIZE } from "./constants";
import type { SubscribeState } from "./store";
import {
  type QueryResult,
  worksheetExecutionAtom,
  worksheetNoticeAtom,
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
  onExecute: (sql: string, kind: string, offset: number, options?: { cluster?: string; replica?: string }) => void;
  /** Cancels the currently running query. */
  onCancel: () => void;
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
  onCancel,
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

  if (execution.status === "running") {
    return <RunningView onCancel={onCancel} onDismiss={onDismiss} />;
  }

  if (!result) {
    return null;
  }

  if (result.displayMode === "sql") {
    return <SqlView result={result} onDismiss={onDismiss} onExecute={onExecute} />;
  }

  if (result.displayMode === "text") {
    return <TextView result={result} onDismiss={onDismiss} onExecute={onExecute} />;
  }

  return <ResultTable result={result} onDismiss={onDismiss} onExecute={onExecute} />;
};

/** Shown while a query is executing, with a Cancel button. */
const RunningView = ({
  onCancel,
  onDismiss,
}: {
  onCancel: () => void;
  onDismiss: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const notice = useAtomValue(worksheetNoticeAtom);
  return (
    <Box height="100%">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        justifyContent="space-between"
      >
        <HStack spacing="2">
          <Spinner size="xs" />
          <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
            Running...
          </Text>
        </HStack>
        <HStack spacing="1">
          <Button size="xs" variant="outline" onClick={onCancel}>
            Cancel
          </Button>
          <IconButton
            icon={<CloseIcon boxSize="2.5" />}
            aria-label="Dismiss results"
            onClick={onDismiss}
            variant="ghost"
            size="xs"
          />
        </HStack>
      </HStack>
      {notice && (
        <Box px="4" py="2">
          <Text fontSize="sm" color={colors.foreground.secondary}>
            NOTICE: {notice}
          </Text>
        </Box>
      )}
    </Box>
  );
};

/** Maps SHOW CREATE kinds to their EXPLAIN prefix for objects that support it. */
const EXPLAINABLE_KINDS: Record<string, string> = {
  show_create_materialized_view: "EXPLAIN MATERIALIZED VIEW",
  show_create_index: "EXPLAIN INDEX",
};

/** Maps SHOW CREATE kinds to their EXPLAIN ANALYZE prefix. */
const ANALYZABLE_KINDS: Record<string, string> = {
  show_create_materialized_view: "EXPLAIN ANALYZE CPU, MEMORY FOR MATERIALIZED VIEW",
  show_create_index: "EXPLAIN ANALYZE CPU, MEMORY FOR INDEX",
};


/** Shared SQL/Explain/Analyze toggle for SHOW CREATE results. */
const SqlExplainToggle = ({
  activeTab,
  stashedResult,
  canAnalyze,
  onSqlClick,
  onExplainClick,
  onAnalyzeClick,
}: {
  activeTab: "sql" | "explain" | "analyze";
  stashedResult: QueryResult | null;
  canAnalyze: boolean;
  onSqlClick: () => void;
  onExplainClick: () => void;
  onAnalyzeClick: (replica?: string) => void;
}) => {
  const source = stashedResult ?? undefined;
  const explainPrefix = source?.kind ? EXPLAINABLE_KINDS[source.kind] : null;
  const analyzePrefix = source?.kind ? ANALYZABLE_KINDS[source.kind] : null;
  const { getClusterById } = useAllClusters();
  const replicas = source?.clusterId
    ? (getClusterById(source.clusterId)?.replicas ?? [])
    : [];

  if (!explainPrefix) return null;

  const analyzeButton = (
    <Tooltip
      label={canAnalyze ? undefined : "Requires superuser or USAGE privilege on the schema"}
      placement="top"
      hasArrow
      isDisabled={canAnalyze}
    >
      <Button
        size="xs"
        variant={activeTab === "analyze" ? "solid" : "ghost"}
        onClick={() => onAnalyzeClick(replicas.length > 1 ? replicas[0].name : undefined)}
        borderLeftRadius={0}
        borderRightRadius={replicas.length > 1 ? 0 : undefined}
        isDisabled={!canAnalyze}
      >
        Analyze
      </Button>
    </Tooltip>
  );

  return (
    <HStack spacing="0">
      <Button
        size="xs"
        variant={activeTab === "sql" ? "solid" : "ghost"}
        onClick={onSqlClick}
        borderRightRadius={0}
      >
        SQL
      </Button>
      <Button
        size="xs"
        variant={activeTab === "explain" ? "solid" : "ghost"}
        onClick={onExplainClick}
        borderRadius={analyzePrefix ? 0 : undefined}
        borderLeftRadius={0}
      >
        Explain
      </Button>
      {analyzePrefix && analyzeButton}
      {analyzePrefix && replicas.length > 1 && canAnalyze && (
        <Menu>
          <MenuButton
            as={Button}
            size="xs"
            variant={activeTab === "analyze" ? "solid" : "ghost"}
            borderLeftRadius={0}
            px="1"
          >
            <ChevronDownIcon />
          </MenuButton>
          <MenuList minW="auto">
            {replicas.map((r) => (
              <MenuItem
                key={r.id}
                fontSize="sm"
                fontWeight={r.name === source?.selectedReplica ? 600 : 400}
                onClick={() => onAnalyzeClick(r.name)}
              >
                {r.name === source?.selectedReplica ? `✓ ${r.name}` : r.name}
              </MenuItem>
            ))}
          </MenuList>
        </Menu>
      )}
    </HStack>
  );
};

/** Renders a SHOW CREATE result with SQL/Explain/Analyze toggle. */
const SqlView = ({
  result,
  onDismiss,
  onExecute,
}: {
  result: QueryResult;
  onDismiss: () => void;
  onExecute: (sql: string, kind: string, offset: number, options?: { cluster?: string; replica?: string }) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const setStashedSqlResult = useSetAtom(worksheetStashedSqlResultAtom);
  const sqlText = String(result.rows[0]?.[0] ?? "");

  const explainPrefix = result.kind ? EXPLAINABLE_KINDS[result.kind] : null;
  const analyzePrefix = result.kind ? ANALYZABLE_KINDS[result.kind] : null;
  const canAnalyze = true;

  const handleExplainClick = useCallback(() => {
    if (explainPrefix && result.objectName) {
      setStashedSqlResult(result);
      onExecute(`${explainPrefix} ${result.objectName}`, "explain_plan", 0);
    }
  }, [explainPrefix, result, onExecute, setStashedSqlResult]);

  const handleAnalyzeClick = useCallback(
    (replica?: string) => {
      if (analyzePrefix && result.objectName) {
        setStashedSqlResult({ ...result, selectedReplica: replica });
        onExecute(
          `${analyzePrefix} ${result.objectName}`,
          "explain_analyze_object",
          0,
          {
            ...(result.clusterName ? { cluster: result.clusterName } : {}),
            ...(replica ? { replica } : {}),
          },
        );
      }
    },
    [analyzePrefix, result, onExecute, setStashedSqlResult],
  );

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
          <SqlExplainToggle
            activeTab="sql"
            stashedResult={result}
            canAnalyze={canAnalyze}
            onSqlClick={() => {}}
            onExplainClick={handleExplainClick}
            onAnalyzeClick={handleAnalyzeClick}
          />
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
  onExecute,
}: {
  result: QueryResult;
  onDismiss: () => void;
  onExecute: (sql: string, kind: string, offset: number, options?: { cluster?: string; replica?: string }) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const text = String(result.rows[0]?.[0] ?? "");
  const [stashedSqlResult, setStashedSqlResult] = useAtom(
    worksheetStashedSqlResultAtom,
  );
  const setResult = useSetAtom(worksheetResultAtom);
  const canAnalyze = true;

  const handleBackToSql = useCallback(() => {
    if (stashedSqlResult) {
      setResult(stashedSqlResult);
      setStashedSqlResult(null);
    }
  }, [stashedSqlResult, setResult, setStashedSqlResult]);

  const handleExplainClick = useCallback(() => {
    const source = stashedSqlResult;
    if (!source) return;
    const prefix = source.kind ? EXPLAINABLE_KINDS[source.kind] : null;
    if (prefix && source.objectName) {
      onExecute(`${prefix} ${source.objectName}`, "explain_plan", 0);
    }
  }, [stashedSqlResult, onExecute]);

  const handleAnalyzeClick = useCallback(
    (replica?: string) => {
      const source = stashedSqlResult;
      if (!source) return;
      const prefix = source.kind ? ANALYZABLE_KINDS[source.kind] : null;
      if (prefix && source.objectName) {
        setStashedSqlResult({ ...source, selectedReplica: replica });
        onExecute(
          `${prefix} ${source.objectName}`,
          "explain_analyze_object",
          0,
          {
            ...(source.clusterName ? { cluster: source.clusterName } : {}),
            ...(replica ? { replica } : {}),
          },
        );
      }
    },
    [stashedSqlResult, onExecute, setStashedSqlResult],
  );

  const handleDownload = useCallback(() => {
    const blob = new Blob([text], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "explain.txt";
    a.click();
    URL.revokeObjectURL(url);
  }, [text]);

  const activeTab = result.kind === "explain_analyze_object" ? "analyze" as const : "explain" as const;

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
            <SqlExplainToggle
              activeTab={activeTab}
              stashedResult={stashedSqlResult}
              canAnalyze={canAnalyze}
              onSqlClick={handleBackToSql}
              onExplainClick={handleExplainClick}
              onAnalyzeClick={handleAnalyzeClick}
            />
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
        whiteSpace="pre-wrap"
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
  onExecute,
}: {
  result: QueryResult;
  onDismiss: () => void;
  onExecute: (sql: string, kind: string, offset: number, options?: { cluster?: string; replica?: string }) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [currentPage, setCurrentPage] = useState(0);
  const [stashedSqlResult, setStashedSqlResult] = useAtom(
    worksheetStashedSqlResultAtom,
  );
  const setResult = useSetAtom(worksheetResultAtom);
  const canAnalyze = true;

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
        <HStack spacing="2">
          {stashedSqlResult && (
            <SqlExplainToggle
              activeTab="analyze"
              stashedResult={stashedSqlResult}
              canAnalyze={canAnalyze}
              onSqlClick={() => {
                setResult(stashedSqlResult);
                setStashedSqlResult(null);
              }}
              onExplainClick={() => {
                const source = stashedSqlResult;
                const prefix = source.kind
                  ? EXPLAINABLE_KINDS[source.kind]
                  : null;
                if (prefix && source.objectName) {
                  onExecute(
                    `${prefix} ${source.objectName}`,
                    "explain_plan",
                    0,
                  );
                }
              }}
              onAnalyzeClick={(replica?: string) => {
                const source = stashedSqlResult;
                if (!source) return;
                const prefix = source.kind
                  ? ANALYZABLE_KINDS[source.kind]
                  : null;
                if (prefix && source.objectName) {
                  setStashedSqlResult({ ...source, selectedReplica: replica });
                  onExecute(
                    `${prefix} ${source.objectName}`,
                    "explain_analyze_object",
                    0,
                    {
                      ...(source.clusterName
                        ? { cluster: source.clusterName }
                        : {}),
                      ...(replica ? { replica } : {}),
                    },
                  );
                }
              }}
            />
          )}
          <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
            {stashedSqlResult
              ? result.commandComplete
              : `Results — ${result.rows.length} row${result.rows.length !== 1 ? "s" : ""} in ${result.durationMs}ms`}
          </Text>
        </HStack>
        <HStack>
          {!stashedSqlResult && (
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              {result.commandComplete}
            </Text>
          )}
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
