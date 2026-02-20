// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Stack,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React, { memo, useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import {
  LIST_LIMIT,
  QueryHistoryListRow,
} from "~/api/materialize/query-history/queryHistoryList";
import StatusPill from "~/components/StatusPill";
import useIsScrolledHorizontal from "~/hooks/useIsScrolledHorizontal";
import { relativeQueryHistoryPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";
import { formatBytesShort } from "~/utils/format";

import { ListToDetailsPageLocationState } from "./QueryHistoryRoutes";
import { formatDuration } from "./queryHistoryUtils";
import {
  ThrottledCountTooltip,
  TransactionIsolationLevelTooltip,
} from "./tooltipComponents";
import { ColumnItem } from "./useColumns";
import { getFinishedStatusColorScheme } from "./utils";

type QueryHistoryTableProps = {
  columns: ColumnItem[];
  rows: QueryHistoryListRow[];
};

const QUERY_ROW_WIDTH_PX = "320px";

const TableRow = memo(
  ({
    row,
    columns,
    isScrolled,
  }: {
    row: QueryHistoryListRow;
    columns: ColumnItem[];
    isScrolled: boolean;
  }) => {
    const { colors } = useTheme<MaterializeTheme>();
    const navigate = useNavigate();
    const { search } = useLocation();

    const columnSet = useMemo(
      () => new Set(columns.map((column) => column.key)),
      [columns],
    );

    const handleTextSelection = (
      e: React.PointerEvent<HTMLTableCellElement>,
    ) => {
      if (window.getSelection()?.toString()) {
        e.stopPropagation();
      }
    };

    return (
      <Tr
        key={row.executionId}
        cursor="pointer"
        backgroundColor={colors.background.primary}
        _hover={{
          background: colors.background.secondary,
        }}
        onClick={() => {
          navigate(relativeQueryHistoryPath(row.executionId), {
            state: {
              from: {
                search,
              },
            } as ListToDetailsPageLocationState,
          });
        }}
      >
        {columnSet.has("sql") && (
          <Td
            position="sticky"
            left="0"
            backgroundColor="inherit"
            borderRight={isScrolled ? "1px solid" : "none"}
            borderColor={colors.border.primary}
            onClick={handleTextSelection}
          >
            <Text
              textStyle="monospace"
              noOfLines={1}
              minWidth={QUERY_ROW_WIDTH_PX}
              title={row.sql}
              wordBreak="break-all"
            >
              {row.sql}
            </Text>
          </Td>
        )}
        {columnSet.has("executionId") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.executionId}</Text>
          </Td>
        )}
        {columnSet.has("authenticatedUser") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.authenticatedUser}</Text>
          </Td>
        )}
        {columnSet.has("sessionId") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.sessionId}</Text>
          </Td>
        )}
        {columnSet.has("finishedStatus") && (
          <Td onClick={handleTextSelection}>
            {row.finishedStatus ? (
              <StatusPill
                status={row.finishedStatus}
                colorScheme={getFinishedStatusColorScheme(row.finishedStatus)}
              />
            ) : (
              "-"
            )}
          </Td>
        )}
        {columnSet.has("duration") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{formatDuration(row.duration)}</Text>
          </Td>
        )}

        {columnSet.has("startTime") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">
              {row.startTime
                ? formatDate(row.startTime, FRIENDLY_DATETIME_FORMAT)
                : "-"}
            </Text>
          </Td>
        )}

        {columnSet.has("endTime") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">
              {row.endTime
                ? formatDate(row.endTime, FRIENDLY_DATETIME_FORMAT)
                : "-"}
            </Text>
          </Td>
        )}

        {columnSet.has("resultSize") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">
              {row.resultSize ? formatBytesShort(row.resultSize) : "-"}
            </Text>
          </Td>
        )}

        {columnSet.has("rowsReturned") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">
              {row.rowsReturned?.toLocaleString() ?? "-"}
            </Text>
          </Td>
        )}

        {columnSet.has("clusterName") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.clusterName ?? "-"}</Text>
          </Td>
        )}

        {columnSet.has("applicationName") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.applicationName}</Text>
          </Td>
        )}

        {columnSet.has("throttledCount") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">
              {row.throttledCount.toLocaleString()}
            </Text>
          </Td>
        )}

        {columnSet.has("executionStrategy") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.executionStrategy ?? "-"}</Text>
          </Td>
        )}

        {columnSet.has("transactionIsolation") && (
          <Td onClick={handleTextSelection}>
            <Text whiteSpace="nowrap">{row.transactionIsolation}</Text>
          </Td>
        )}
      </Tr>
    );
  },
);

const QueryHistoryTable = ({ rows, columns }: QueryHistoryTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const SCROLL_BORDER_OFFSET = 8;
  const { isScrolled, onScroll } =
    useIsScrolledHorizontal(SCROLL_BORDER_OFFSET);

  return (
    <Stack flexGrow="1" minWidth="0" overflowX="auto" onScroll={onScroll}>
      <Table variant="borderless" flexGrow="0">
        <Thead
          position="sticky"
          top="0"
          background={colors.background.primary}
          zIndex="1"
        >
          <Tr>
            {columns.map(({ key, label }) => {
              if (key === "sql") {
                return (
                  <Th
                    position="sticky"
                    left="0"
                    backgroundColor={colors.background.primary}
                    key={key}
                  >
                    <Text width={QUERY_ROW_WIDTH_PX} whiteSpace="nowrap">
                      {label}
                    </Text>
                  </Th>
                );
              }
              if (key === "transactionIsolation") {
                return (
                  <Th key={key}>
                    <Text whiteSpace="nowrap">
                      {label}
                      <TransactionIsolationLevelTooltip marginLeft="1" />
                    </Text>
                  </Th>
                );
              }
              if (key === "throttledCount") {
                return (
                  <Th key={key}>
                    <Text whiteSpace="nowrap">
                      {label}
                      <ThrottledCountTooltip marginLeft="1" />
                    </Text>
                  </Th>
                );
              }

              return (
                <Th key={key}>
                  <Text whiteSpace="nowrap">{label}</Text>
                </Th>
              );
            })}
          </Tr>
        </Thead>

        <Tbody>
          {rows.map((row) => {
            return (
              <TableRow
                isScrolled={isScrolled}
                key={row.executionId}
                row={row}
                columns={columns}
              />
            );
          })}
          {rows.length === LIST_LIMIT && (
            <Tr>
              <Td colSpan={columns.length}>
                <Text
                  textStyle="text-ui-med"
                  color={colors.foreground.secondary}
                  position="sticky"
                  left="4"
                  marginLeft="-4"
                  width="fit-content"
                >
                  You&apos;ve reached the table limit. Please revise your search
                  filters.
                </Text>
              </Td>
            </Tr>
          )}
        </Tbody>
      </Table>
    </Stack>
  );
};

export default QueryHistoryTable;
