// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  chakra,
  Code,
  Grid,
  HStack,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtomCallback } from "jotai/utils";
import React from "react";

import type { Error } from "~/api/materialize/types";
import { ErrorCode } from "~/api/materialize/types";
import { InfoIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { capitalizeSentence } from "~/util";

import CommandResultNotice from "./CommandResultNotice";
import { ERROR_NOTICE_OUTPUT_MAX_WIDTH, TABLE_PAGE_SIZE } from "./constants";
import ErrorOutput from "./ErrorOutput";
import {
  describeExecutionTime,
  findExecutionTime,
  formatExecutionTime,
} from "./executionTime";
import formatRows from "./formatRows";
import SqlSelectTable, { TablePagination } from "./SqlSelectTable";
import {
  CommandOutput,
  CommandResult as CommandResultType,
  updateDisplayStateAtomCallback,
} from "./store/shell";
import {
  calculateCommandDuration,
  formatCommandDuration,
  getCommandResultStartTime,
} from "./timings";
import useCommandOutputPagination from "./useCommandOutputPagination";

const ROUND_TRIP_HELP =
  "The time until the Shell had received the whole result, including the network and your browser's processing, measured from sending the input.";

const LATER_ROUND_TRIP_HELP =
  "The time until the Shell had received the whole result, including the network and your browser's processing, measured from the previous result.";

const SHORTER_THAN_EXECUTION_TIME_HELP =
  "It can be shorter than the time in Materialize, because Materialize starts the statement while the Shell is still handling the previous result.";

interface TimingRow {
  label: string;
  description: string;
}

const TimingTooltipContent = ({ rows }: { rows: TimingRow[] }) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Grid as="dl" templateColumns="auto 1fr" columnGap={3} rowGap={2}>
      {rows.map(({ label, description }) => (
        <React.Fragment key={label}>
          <Text
            as="dt"
            textStyle="text-ui-med"
            color={colors.foreground.primary}
          >
            {label}
          </Text>
          <Text as="dd" textStyle="text-base">
            {description}
          </Text>
        </React.Fragment>
      ))}
    </Grid>
  );
};

const CommandResult = ({
  commandResultIndex,
  commandResult,
  commandOutput,
  error,
}: {
  commandResultIndex: number;
  commandResult: CommandResultType;
  commandOutput: CommandOutput;
  error?: Error;
}) => {
  const {
    hasRows,
    commandCompletePayload,
    isStreamingResult,
    rows,
    cols,
    notices,
  } = commandResult;
  const isSubscribeManager =
    commandOutput.commandResultsDisplayStates[commandResultIndex]
      .isSubscribeManager;

  const { colors, shadows } = useTheme<MaterializeTheme>();

  const paginationState = useCommandOutputPagination({
    historyId: commandOutput.historyId,
    commandIndex: commandResultIndex,
    pageSize: TABLE_PAGE_SIZE,
    totalNumRows: rows?.length ?? 0,
  });

  const updateDisplayState = useAtomCallback(updateDisplayStateAtomCallback);

  const setCommandResultRawFormat = (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    updateDisplayState(commandOutput.historyId, commandResultIndex, (prev) => ({
      ...prev,
      isSubscribeManager: e.target.checked,
    }));
  };

  let table = null;
  if (hasRows && cols && rows) {
    const colNames = cols.map(({ name }) => name);

    const paginatedRows = paginationState
      ? rows.slice(paginationState.startIndex, paginationState.endIndex)
      : rows;

    const formattedRows = formatRows(cols, paginatedRows ?? []);

    table = (
      <VStack width="100%" alignItems="flex-start">
        <SqlSelectTable
          paginatedRows={formattedRows}
          cols={cols}
          colNames={colNames}
          rows={rows}
          onSubscribeChange={setCommandResultRawFormat}
          isStreamingResult={isStreamingResult}
          isSubscribeManager={isSubscribeManager}
          width="100%"
        />
        {/* We check for empty columns instead of empty rows since a statement like
        'SELECT;' will set rows to [[]] with length 1 */}
        {paginationState && cols.length > 0 && (
          <TablePagination
            {...paginationState}
            totalNumRows={rows?.length ?? 0}
            pageSize={TABLE_PAGE_SIZE}
            width="100%"
          />
        )}
      </VStack>
    );
  }

  const timeTaken = calculateCommandDuration(commandResultIndex, commandOutput);
  const executionTime = findExecutionTime(notices);

  const timeTakenStr = timeTaken
    ? `Returned in ${formatCommandDuration(timeTaken)}` +
      (executionTime ? ` · ${formatExecutionTime(executionTime)}` : "")
    : null;

  const { isMeasuredFromPreviousResult } = getCommandResultStartTime(
    commandOutput,
    commandResultIndex,
  );
  let roundTripHelp = ROUND_TRIP_HELP;
  if (isMeasuredFromPreviousResult) {
    roundTripHelp = executionTime
      ? `${LATER_ROUND_TRIP_HELP} ${SHORTER_THAN_EXECUTION_TIME_HELP}`
      : LATER_ROUND_TRIP_HELP;
  }
  const timingRows: TimingRow[] = [
    { label: "Returned in", description: roundTripHelp },
    ...(executionTime ? [describeExecutionTime(executionTime)] : []),
  ];

  // A cancelled query and a dropped connection both surface as errors so that
  // they get an explanation in the command block, but neither is a failure the
  // server reported. Drop the "Error:" prefix for both.
  const errorOutputPropOverrides =
    error &&
    (error.code === ErrorCode.QUERY_CANCELED ||
      error.code === ErrorCode.CONNECTION_FAILURE)
      ? {
          errorMessageOverride: capitalizeSentence(error.message, false),
        }
      : {};

  // NOTE: this deliberately does not include CONNECTION_FAILURE. An
  // interrupted command never receives an end timestamp, so `timeTakenStr` is
  // null and the element these props apply to is not rendered at all.
  const codePropOverrides =
    error?.code === ErrorCode.QUERY_CANCELED
      ? {
          color: colors.accent.green,
        }
      : {};

  return (
    <>
      {notices.map((notice, noticeIdx) => (
        <CommandResultNotice
          key={noticeIdx}
          notice={notice}
          commandOutput={commandOutput}
          commandResultIndex={commandResultIndex}
        />
      ))}
      {!hasRows && !error && <Code>{commandCompletePayload ?? ""}</Code>}
      {table}

      {error && (
        <ErrorOutput
          error={error}
          command={commandOutput.command}
          width="100%"
          maxWidth={ERROR_NOTICE_OUTPUT_MAX_WIDTH}
          overflow="auto"
          {...errorOutputPropOverrides}
        />
      )}
      {timeTakenStr && (
        <HStack>
          <Code
            color={error ? colors.accent.red : colors.accent.green}
            {...codePropOverrides}
          >
            {timeTakenStr}
          </Code>
          <Tooltip
            label={<TimingTooltipContent rows={timingRows} />}
            placement="right"
            color={colors.foreground.secondary}
            background={colors.background.secondary}
            maxWidth="sm"
            hasArrow
          >
            <chakra.button
              type="button"
              aria-label="Timing details"
              display="inline-flex"
              borderRadius="full"
              _focusVisible={{ boxShadow: shadows.input.focus }}
            >
              <InfoIcon />
            </chakra.button>
          </Tooltip>
        </HStack>
      )}
    </>
  );
};

export default CommandResult;
