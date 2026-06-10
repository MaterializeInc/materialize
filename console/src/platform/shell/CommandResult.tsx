// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, HStack, Tooltip, useTheme, VStack } from "@chakra-ui/react";
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
import formatRows from "./formatRows";
import SqlSelectTable, { TablePagination } from "./SqlSelectTable";
import {
  CommandOutput,
  CommandResult as CommandResultType,
  updateDisplayStateAtomCallback,
} from "./store/shell";
import { calculateCommandDuration, formatCommandDuration } from "./timings";
import useCommandOutputPagination from "./useCommandOutputPagination";

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

  const { colors } = useTheme<MaterializeTheme>();

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

  const timeTakenStr = timeTaken
    ? `Returned in ${formatCommandDuration(timeTaken)}`
    : null;

  // If the error comes from a query cancellation, we don't want to display it as
  // an error
  const errorOutputPropOverrides =
    error && error.code === ErrorCode.QUERY_CANCELED
      ? {
          errorMessageOverride: capitalizeSentence(error.message, false),
        }
      : {};

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
            label="The total time to submit, execute, receive, and render the query results over the network."
            placement="right"
            color={colors.foreground.secondary}
            background={colors.background.secondary}
            hasArrow
          >
            <InfoIcon />
          </Tooltip>
        </HStack>
      )}
    </>
  );
};

export default CommandResult;
