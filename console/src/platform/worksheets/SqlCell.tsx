// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { RepeatIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  Code,
  Flex,
  HStack,
  IconButton,
  Input,
  Popover,
  PopoverBody,
  PopoverContent,
  PopoverTrigger,
  Portal,
  Spinner,
  Text,
  Tooltip,
  useDisclosure,
  useLatestRef,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { Extension } from "@codemirror/state";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { SessionVariables } from "~/api/materialize/types";
import {
  ChevronDownIcon,
  ChevronRightIcon,
  CloseIcon,
  EyeClosedIcon,
  EyeOpenIcon,
  PauseIcon,
  PlayIcon,
  TableIcon,
} from "~/icons";
import {
  describeExecutionTime,
  EXECUTION_TIME_MIN_VERSION,
  EXECUTION_TIME_SESSION_VARIABLE,
  formatExecutionTime,
} from "~/platform/shell/executionTime";
import { formatCommandDuration } from "~/platform/shell/timings";
import {
  useCurrentEnvironmentHttpAddress,
  useEnvironmentGate,
} from "~/store/environments";
import { MonitoringIcon } from "~/svg/nav/MonitoringIcon";
import { MaterializeTheme } from "~/theme";

import { CellDirection, CellEditor, LineNumbering } from "./CellEditor";
import { CellFrame } from "./CellFrame";
import { cellTitle, lineCount } from "./cellLabels";
import {
  CellRunner,
  CellRunState,
  MAX_DISPLAY_ROWS,
  RunMode,
} from "./CellRunner";
import { ChartView } from "./ChartView";
import { ExpandResult } from "./expandCellReferences";
import { LiveIcon } from "./LiveIcon";
import { PinIcon } from "./PinIcon";
import { ResultsTable } from "./ResultsTable";
import { hasCode } from "./sqlTokens";
import { newChartSpec, WorksheetCell } from "./store";
import {
  isQueryStatement,
  toCellQuery,
  unwrapSubscribe,
  wrapInSubscribe,
} from "./subscribeStatement";
import { toWorksheetRows, WorksheetRuntime } from "./worksheetRuntime";

const WORKSHEET_APPLICATION_NAME = "web_console_worksheet";

export interface ActiveRun {
  mode: RunMode;
  /** Bumped on every Run, so re-running unchanged SQL still restarts. */
  token: number;
}

export interface SqlCellProps {
  /** Shown in the toolbar to reorder the cell. */
  dragHandle?: React.ReactNode;
  onRemove: () => void;
  cell: WorksheetCell;
  runtime: WorksheetRuntime;
  /** Why the cell's name cannot be referenced, if it cannot. */
  nameError: string | undefined;
  /** The SQL the latest run executes. Undefined until the cell first runs. */
  expanded: ExpandResult | undefined;
  active: ActiveRun | undefined;
  cluster: string;
  onChange: (update: Partial<WorksheetCell>) => void;
  /** Returns why the name was refused, or undefined once it is applied. */
  onRename: (name: string) => string | undefined;
  onRun: () => void;
  /** Runs the cell again from the dashboard, outside the query history. */
  onRefresh: () => void;
  onStop: () => void;
  language: Extension;
  /**
   * Called once per run when it finishes, fails, or, for a live run, has
   * delivered its first snapshot.
   */
  onSettled?: () => void;
  /** Shows only a title, the status and the results, as on a dashboard. */
  presenting?: boolean;
  lineNumbers: LineNumbering;
  onTab: (direction: CellDirection) => void;
  /**
   * Where in the worksheet's results pane the cell renders its results and
   * their controls, if it is shown or pinned there.
   */
  resultsPane: HTMLElement | null;
  selected: boolean;
  /** Switches the cell between pinned in the pane and shown from its tab. */
  onTogglePin: () => void;
}

export const SqlCell = ({
  cell,
  runtime,
  nameError,
  expanded,
  active,
  cluster,
  onChange,
  onRename,
  onRun,
  onRefresh,
  onStop,
  language,
  onSettled,
  presenting = false,
  onRemove,
  dragHandle,
  lineNumbers,
  resultsPane,
  selected,
  onTab,
  onTogglePin,
}: SqlCellProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const httpAddress = useCurrentEnvironmentHttpAddress();
  // Older environments answer the unknown setting with a notice.
  const executionTimeSupported =
    useEnvironmentGate(EXECUTION_TIME_MIN_VERSION) === true;
  const [runner] = useState(() => new CellRunner());
  const [showExpanded, setShowExpanded] = useState(false);
  const state = React.useSyncExternalStore(
    runner.subscribe,
    runner.getSnapshot,
  );

  useEffect(() => () => runner.stop(), [runner]);

  // Publish the latest rows under this cell's name, so chart cells that plot
  // it re-render on every update, live or not.
  const runtimeRows = useMemo(
    () => toWorksheetRows(state.columns, state.rows),
    [state.columns, state.rows],
  );
  const hasColumns = state.columns.length > 0;
  const publishedName = nameError ? undefined : cell.name;
  useEffect(() => {
    if (hasColumns) {
      runtime.publish(cell.id, publishedName, {
        rows: runtimeRows,
        totalRows: state.totalRows,
      });
    } else {
      runtime.unpublish(cell.id);
    }
  }, [
    runtime,
    cell.id,
    publishedName,
    runtimeRows,
    state.totalRows,
    hasColumns,
  ]);
  useEffect(() => () => runtime.unpublish(cell.id), [runtime, cell.id]);

  // A one-shot run starts only for a new Run, so editing an upstream cell or
  // the cluster never re-executes DML or DDL behind the user's back. A live
  // run also restarts when its SQL or cluster changes, which is how a re-run
  // upstream cell propagates to live dependents.
  const expandedSql = expanded?.sql;
  const startedTokenRef = useRef<number>();
  useEffect(() => {
    if (!active || !expandedSql || !httpAddress) return;
    if (active.mode === "once" && startedTokenRef.current === active.token) {
      return;
    }
    startedTokenRef.current = active.token;
    const sessionVariables: SessionVariables = {
      application_name: WORKSHEET_APPLICATION_NAME,
      ...(cluster ? { cluster } : {}),
      ...(executionTimeSupported
        ? { [EXECUTION_TIME_SESSION_VARIABLE]: "on" }
        : {}),
    };
    runner.start({
      httpAddress,
      sql: expandedSql,
      mode: active.mode,
      sessionVariables,
    });
    if (active.mode === "live") return () => runner.stop();
  }, [
    runner,
    active,
    expandedSql,
    httpAddress,
    cluster,
    executionTimeSupported,
  ]);

  const isRunning = state.status === "connecting" || state.status === "running";

  // A run only counts as settled after it has been seen in progress: the
  // render that starts it still holds the previous run's finished state.
  const onSettledRef = useLatestRef(onSettled);
  const seenRunningTokenRef = useRef<number>();
  const settledTokenRef = useRef<number>();
  useEffect(() => {
    if (!active || settledTokenRef.current === active.token) return;
    if (isRunning) seenRunningTokenRef.current = active.token;
    const seenRunning = seenRunningTokenRef.current === active.token;
    const settled =
      expanded?.error !== undefined ||
      (seenRunning &&
        (state.status === "done" ||
          state.status === "error" ||
          (active.mode === "live" && state.frontier !== undefined)));
    if (settled) {
      settledTokenRef.current = active.token;
      onSettledRef.current?.();
    }
  }, [
    active,
    isRunning,
    state.status,
    state.frontier,
    expanded?.error,
    onSettledRef,
  ]);
  const hiddenLines = lineCount(cell.source) - 1;
  const isSubscribeStatement = toCellQuery(cell.source).isSubscribe;
  const isQuery = !isSubscribeStatement && isQueryStatement(cell.source);
  const isLive = isSubscribeStatement;
  const unwrapped = isLive ? unwrapSubscribe(cell.source) : undefined;
  const isChart = cell.view === "chart";
  const chartSpec = cell.chart ?? newChartSpec();
  const toggleView = () =>
    onChange({ view: isChart ? "table" : "chart", chart: chartSpec });
  // A cell shows its results one way: as a table or as a chart.
  const results = isChart ? (
    <Box>
      <ChartView
        chart={chartSpec}
        rows={hasColumns ? runtimeRows : undefined}
        totalRows={state.totalRows}
        onChange={(update) => onChange({ chart: { ...chartSpec, ...update } })}
        showControls={!presenting}
        unavailableMessage={
          state.status === "idle" ? "Run the cell to see its chart" : undefined
        }
      />
    </Box>
  ) : hasColumns ? (
    <ResultsTable
      columns={state.columns}
      rows={state.rows}
      fill={!presenting}
    />
  ) : undefined;
  const hasReferences = (expanded?.dependencies.length ?? 0) > 0;
  const runError = expanded?.error
    ? { message: expanded.error }
    : state.status === "error"
      ? state.error
      : undefined;

  const errorBlock = runError && (
    <Box px="2" py="1" fontSize="xs" fontFamily="mono">
      <Text color={colors.accent.red}>{runError.message}</Text>
      {runError.detail && (
        <Text color={colors.foreground.secondary}>{runError.detail}</Text>
      )}
      {runError.hint && (
        <Text color={colors.foreground.secondary}>Hint: {runError.hint}</Text>
      )}
    </Box>
  );

  if (presenting) {
    const title = cellTitle(cell);
    return (
      <Box borderBottomWidth="1px" borderColor={colors.border.primary} minW="0">
        <HStack
          role="group"
          px="2"
          py="1.5"
          spacing="3"
          justifyContent="space-between"
        >
          <Text
            textStyle="text-ui-med"
            fontWeight="600"
            noOfLines={1}
            title={title.detail}
          >
            {title.title}
          </Text>
          <HStack spacing="1">
            {/* Live cells keep themselves current. */}
            {!isLive && !isRunning && (
              <IconButton
                aria-label="Refresh this card"
                title="Run this cell again"
                icon={<RepeatIcon />}
                size="xs"
                variant="inline"
                minW="5"
                color={colors.foreground.secondary}
                opacity="0"
                _groupHover={{ opacity: 1 }}
                _focusVisible={{ opacity: 1 }}
                onClick={onRefresh}
              />
            )}
            <CellStatus state={state} />
          </HStack>
        </HStack>
        {errorBlock}
        {results ??
          (!runError && state.status === "idle" && (
            <Text
              px="2"
              pb="2"
              textStyle="text-small"
              color={colors.foreground.tertiary}
            >
              Not run yet
            </Text>
          ))}
      </Box>
    );
  }

  const liveToggle = (isQuery || isSubscribeStatement) && (
    <IconButton
      aria-label={isLive ? "Stop live updates" : "Make live"}
      aria-pressed={isLive}
      title={
        isLive
          ? unwrapped === undefined
            ? "Live. Its SUBSCRIBE options can't be removed automatically."
            : "Live. Click to remove the SUBSCRIBE and run once."
          : "Make live: wraps the query in SUBSCRIBE so results keep updating"
      }
      icon={<LiveIcon boxSize="3.5" />}
      size="xs"
      variant="inline"
      minW="5"
      color={isLive ? colors.accent.green : colors.foreground.tertiary}
      isDisabled={isLive && unwrapped === undefined}
      onClick={() =>
        onChange({
          source: isLive
            ? (unwrapped ?? cell.source)
            : wrapInSubscribe(cell.source),
        })
      }
    />
  );

  const runButton = isRunning ? (
    <IconButton
      aria-label="Stop"
      title="Stop"
      icon={<PauseIcon />}
      size="xs"
      variant="inline"
      minW="5"
      height="6"
      onClick={() => {
        // A one-shot run has no effect cleanup to stop it.
        runner.stop();
        onStop();
      }}
    />
  ) : (
    <IconButton
      aria-label="Run"
      title="Run (Shift-Enter)"
      icon={<PlayIcon />}
      size="xs"
      variant="inline"
      minW="5"
      height="6"
      color={colors.accent.purple}
      isDisabled={!hasCode(cell.source)}
      onClick={onRun}
    />
  );

  const viewToggle = (isQuery || isLive) && (
    <IconButton
      aria-label={isChart ? "Show as table" : "Show as chart"}
      title={isChart ? "Show as table" : "Show as chart"}
      icon={isChart ? <TableIcon /> : <MonitoringIcon color="currentColor" />}
      size="xs"
      variant="inline"
      minW="5"
      onClick={toggleView}
    />
  );
  const dashboardToggle = (
    <IconButton
      aria-label={
        cell.resultsHidden ? "Show on the dashboard" : "Hide from the dashboard"
      }
      aria-pressed={!cell.resultsHidden}
      title={
        cell.resultsHidden
          ? "Hidden from the dashboard. Click to show it there."
          : "Shown on the dashboard. Click to hide it there."
      }
      icon={cell.resultsHidden ? <EyeClosedIcon /> : <EyeOpenIcon />}
      size="xs"
      variant="inline"
      minW="5"
      color={
        cell.resultsHidden
          ? colors.foreground.tertiary
          : colors.foreground.secondary
      }
      onClick={() => onChange({ resultsHidden: !cell.resultsHidden })}
    />
  );
  const pinToggle = (
    <IconButton
      aria-label={cell.pinned ? "Unpin results" : "Pin results"}
      aria-pressed={Boolean(cell.pinned)}
      title={
        cell.pinned
          ? "Pinned. Click to put it back in the tabs."
          : "Pin: keep these results in view while you run other cells"
      }
      icon={<PinIcon boxSize="3.5" />}
      size="xs"
      variant="inline"
      minW="5"
      color={cell.pinned ? colors.accent.purple : colors.foreground.tertiary}
      onClick={onTogglePin}
    />
  );
  const pane = resultsPane && (
    <Flex direction="column" height="100%" minH="0">
      <HStack spacing="1" height="8" flexShrink={0} minW="0">
        {pinToggle}
        {liveToggle}
        {viewToggle}
        {dashboardToggle}
        <CellStatus state={state} />
        <RunTiming state={state} />
      </HStack>
      {errorBlock}
      <Box flex="1" minH="0" overflow="auto">
        {results ??
          (!runError && state.status === "idle" && (
            <Text textStyle="text-small" color={colors.foreground.tertiary}>
              Not run yet
            </Text>
          ))}
      </Box>
    </Flex>
  );

  const toolbar = (
    <HStack spacing="1" height="7" pr="1" ml="-1" minW="0">
      <HStack spacing="1.5" minW="0" flex="1" overflow="hidden">
        <CellNameButton
          name={cell.name}
          error={nameError}
          onRename={onRename}
        />
        <CellStatus state={state} />
        {runError && (
          <Text
            textStyle="text-small"
            color={colors.accent.red}
            whiteSpace="nowrap"
            title={runError.message}
          >
            failed
          </Text>
        )}
        {cell.collapsed && hiddenLines > 0 && (
          <Text
            textStyle="text-small"
            color={colors.foreground.tertiary}
            whiteSpace="nowrap"
            title={`${hiddenLines} more line${hiddenLines === 1 ? "" : "s"} hidden`}
          >
            +{hiddenLines} lines
          </Text>
        )}
      </HStack>
    </HStack>
  );
  // Beside the first line, so they read as belonging to this cell rather
  // than to the one below it.
  const controls = (
    <HStack spacing="1">
      <Button
        size="xs"
        variant="inline"
        px="1"
        fontWeight="400"
        whiteSpace="nowrap"
        aria-pressed={Boolean(cell.skipInRunAll)}
        // Only the exception stays visible.
        opacity={cell.skipInRunAll ? 1 : 0}
        _groupHover={{ opacity: 1 }}
        _focusVisible={{ opacity: 1 }}
        color={
          cell.skipInRunAll ? colors.accent.orange : colors.foreground.tertiary
        }
        title={
          cell.skipInRunAll
            ? "Run all skips this cell. Click to include it."
            : "Run all runs this cell. Click to skip it, for example to keep a DROP from running."
        }
        onClick={() => onChange({ skipInRunAll: !cell.skipInRunAll })}
      >
        {cell.skipInRunAll ? "skipped by Run all" : "in Run all"}
      </Button>
      {hasReferences && (
        <Button
          size="xs"
          variant="inline"
          px="1"
          fontWeight="400"
          aria-pressed={showExpanded}
          color={
            showExpanded ? colors.accent.purple : colors.foreground.tertiary
          }
          title={
            showExpanded
              ? "Hide the SQL this cell runs"
              : "Show the SQL this cell runs, with its {{references}} expanded"
          }
          onClick={() => setShowExpanded((v) => !v)}
        >
          SQL
        </Button>
      )}
      {dragHandle}
      <IconButton
        aria-label="Delete cell"
        title="Delete cell"
        icon={<CloseIcon stroke="currentColor" boxSize="3" />}
        size="xs"
        variant="inline"
        minW="5"
        onClick={onRemove}
      />
      {runButton}
    </HStack>
  );

  return (
    <CellFrame
      barColor={
        runError
          ? colors.accent.red
          : state.status === "done" ||
              (isRunning && state.frontier !== undefined)
            ? colors.accent.green
            : isRunning
              ? colors.accent.purple
              : colors.border.secondary
      }
      selected={selected}
      gutter={
        <>
          <IconButton
            aria-label={cell.collapsed ? "Expand query" : "Collapse query"}
            title={cell.collapsed ? "Expand" : "Collapse"}
            icon={cell.collapsed ? <ChevronRightIcon /> : <ChevronDownIcon />}
            size="xs"
            variant="inline"
            minW="4"
            height="6"
            color={colors.foreground.tertiary}
            onClick={() => onChange({ collapsed: !cell.collapsed })}
          />
        </>
      }
      actions={controls}
      editor={
        <Box onFocus={() => cell.collapsed && onChange({ collapsed: false })}>
          <CellEditor
            value={cell.source}
            language={language}
            lineNumbers={lineNumbers}
            collapsed={Boolean(cell.collapsed)}
            onChange={(source) => onChange({ source })}
            onRun={onRun}
            onTab={onTab}
          />
        </Box>
      }
      below={
        <>
          {toolbar}
          {showExpanded && expanded?.sql && (
            <Code
              as="pre"
              display="block"
              whiteSpace="pre-wrap"
              fontSize="xs"
              px="2"
              py="1"
            >
              {expanded.sql}
            </Code>
          )}
          {pane && createPortal(pane, resultsPane)}
        </>
      }
    />
  );
};

export interface CellNameButtonProps {
  name: string;
  error: string | undefined;
  onRename: (name: string) => string | undefined;
}

/** The cell's name, which opens a rename field. */
const CellNameButton = ({ name, error, onRename }: CellNameButtonProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [draft, setDraft] = useState(name);
  const [draftError, setDraftError] = useState<string>();

  const commit = () => {
    if (draft === name) return onClose();
    const refused = onRename(draft);
    if (refused) {
      setDraftError(refused);
    } else {
      onClose();
    }
  };

  return (
    <Popover
      isOpen={isOpen}
      onOpen={() => {
        setDraft(name);
        setDraftError(undefined);
        onOpen();
      }}
      onClose={onClose}
      placement="bottom-end"
      isLazy
    >
      <PopoverTrigger>
        <Button
          size="xs"
          variant="inline"
          px="1"
          maxW="40"
          fontFamily="mono"
          fontWeight="400"
          color={error ? colors.accent.red : colors.foreground.secondary}
          title={error ?? `Rename. Other cells use this one as {{${name}}}.`}
        >
          <Text as="span" noOfLines={1}>
            {name || "unnamed"}
          </Text>
        </Button>
      </PopoverTrigger>
      <Portal>
        <PopoverContent width="72">
          <PopoverBody>
            <VStack alignItems="stretch" spacing="1.5">
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                Other cells use this one as {"{{"}
                {draft || "name"}
                {"}}"}
              </Text>
              <Input
                size="sm"
                fontFamily="mono"
                autoFocus
                value={draft}
                isInvalid={Boolean(draftError)}
                onChange={(e) => {
                  setDraft(e.target.value);
                  setDraftError(undefined);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") commit();
                  if (e.key === "Escape") onClose();
                }}
              />
              {(draftError ?? error) && (
                <Text textStyle="text-small" color={colors.accent.red}>
                  {draftError ?? error}
                </Text>
              )}
              <HStack justifyContent="flex-end">
                <Button size="xs" variant="secondary" onClick={onClose}>
                  Cancel
                </Button>
                <Button size="xs" variant="primary" onClick={commit}>
                  Rename
                </Button>
              </HStack>
            </VStack>
          </PopoverBody>
        </PopoverContent>
      </Portal>
    </Popover>
  );
};

const ROUND_TRIP_HELP =
  "The time until the worksheet had received the whole result, including the network and your browser's processing, measured from sending the query.";
const SNAPSHOT_HELP =
  "The time until the first complete, consistent set of results arrived, measured from sending the query. Updates after that arrive as the data changes.";

/** How long the latest run took, worded as the SQL Shell words it. */
const RunTiming = ({ state }: { state: CellRunState }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { roundTripMs, snapshotMs, executionTime } = state;
  const elapsed =
    roundTripMs !== undefined
      ? { label: "Returned in", ms: roundTripMs, help: ROUND_TRIP_HELP }
      : snapshotMs !== undefined
        ? { label: "Snapshot in", ms: snapshotMs, help: SNAPSHOT_HELP }
        : undefined;
  if (!elapsed) return null;
  const rows = [
    { label: elapsed.label, description: elapsed.help },
    ...(executionTime ? [describeExecutionTime(executionTime)] : []),
  ];
  return (
    <Tooltip
      label={
        <VStack alignItems="stretch" spacing="2">
          {rows.map((row) => (
            <Box key={row.label}>
              <Text textStyle="text-ui-med" color={colors.foreground.primary}>
                {row.label}
              </Text>
              <Text textStyle="text-small">{row.description}</Text>
            </Box>
          ))}
        </VStack>
      }
      placement="top-start"
      color={colors.foreground.secondary}
      background={colors.background.secondary}
      maxWidth="sm"
      hasArrow
    >
      <Text
        as="span"
        tabIndex={0}
        textStyle="text-small"
        fontFamily="mono"
        color={colors.foreground.secondary}
        whiteSpace="nowrap"
      >
        · {elapsed.label} {formatCommandDuration(elapsed.ms)}
        {executionTime && ` · ${formatExecutionTime(executionTime)}`}
      </Text>
    </Tooltip>
  );
};

export interface CellStatusProps {
  state: CellRunState;
}

const CellStatus = ({ state }: CellStatusProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const {
    status,
    mode,
    frontier,
    completed,
    totalRows,
    commandTag,
    statementCount,
  } = state;
  if (status === "idle" || status === "error") return null;
  if (status === "connecting") return <Spinner size="xs" />;

  const rowCount = `${totalRows.toLocaleString()} row${totalRows === 1 ? "" : "s"}`;
  const truncated =
    totalRows > MAX_DISPLAY_ROWS
      ? `, first ${MAX_DISPLAY_ROWS.toLocaleString()} shown`
      : "";
  let label: string;
  let title: string | undefined;
  if (mode === "live") {
    const isLive = status === "running";
    if (isLive && !frontier) {
      label = "loading snapshot…";
    } else {
      const prefix = isLive ? "● " : completed ? "complete · " : "stopped · ";
      label = `${prefix}${rowCount}${truncated}`;
    }
    title = frontier
      ? `${isLive ? "Live. " : ""}Consistent as of mz_timestamp ${frontier}`
      : undefined;
  } else {
    label = `${commandTag ?? rowCount}${truncated}`;
    title =
      statementCount > 1
        ? `Showing the last of ${statementCount} statements`
        : undefined;
  }
  return (
    <Text
      textStyle="text-small"
      color={
        mode === "live" && status === "running" && frontier
          ? colors.accent.green
          : colors.foreground.secondary
      }
      whiteSpace="nowrap"
      title={title}
    >
      {label}
    </Text>
  );
};
