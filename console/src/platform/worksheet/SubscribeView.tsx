import { CloseIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  Flex,
  HStack,
  IconButton,
  Spinner,
  Switch,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React, { useMemo, useState } from "react";

import formatRows from "~/components/formatRows";
import SqlSelectTable, { TablePagination } from "~/components/SqlSelectTable";
import { MaterializeTheme } from "~/theme";

import { METADATA_COLUMNS, PAGE_SIZE } from "./constants";
import type { SubscribeState } from "./store";

/**
 * Displays SUBSCRIBE results in two modes:
 * - Consolidated (default): shows the latest materialized state, hiding metadata columns.
 * - Diffs ("Show diffs" toggle): shows the raw append log with all columns.
 */
export interface SubscribeViewProps {
  subscribeState: SubscribeState;
  onStop: () => void;
  onDismiss: () => void;
}

const SubscribeView = ({
  subscribeState,
  onStop,
  onDismiss,
}: SubscribeViewProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [showDiffs, setShowDiffs] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);
  const [isFollowing, setIsFollowing] = useState(true);

  const {
    columns,
    materializedRows,
    diffRows,
    progressTimestamp,
    isStreaming,
    error,
  } = subscribeState;

  const sourceRows = showDiffs ? diffRows : materializedRows;

  const displayColumns = useMemo(() => {
    if (showDiffs) return columns;
    return columns.filter((c) => !METADATA_COLUMNS.has(c.name));
  }, [columns, showDiffs]);

  const displayColNames = displayColumns.map((c) => c.name);

  const displayRows = useMemo(() => {
    if (showDiffs) return sourceRows;
    const keepIndices = columns
      .map((c, i) => (METADATA_COLUMNS.has(c.name) ? -1 : i))
      .filter((i) => i >= 0);
    return sourceRows.map((row) => keepIndices.map((i) => row[i]));
  }, [columns, sourceRows, showDiffs]);

  const formattedRows = useMemo(
    () => formatRows(displayColumns, displayRows),
    [displayColumns, displayRows],
  );

  const totalPages = Math.ceil(formattedRows.length / PAGE_SIZE);
  const displayPage = isFollowing ? Math.max(0, totalPages - 1) : currentPage;
  const startIndex = displayPage * PAGE_SIZE;
  const endIndex = Math.min(startIndex + PAGE_SIZE, formattedRows.length);
  const paginatedRows = formattedRows.slice(startIndex, endIndex);

  if (error) {
    return (
      <Flex align="center" justify="center" height="100%" p="4">
        <Text color="red.500">{error}</Text>
      </Flex>
    );
  }

  return (
    <Box height="100%" overflow="auto" p="2">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        justifyContent="space-between"
      >
        <HStack spacing="4">
          <HStack spacing="2">
            {isStreaming && <Spinner size="xs" color={colors.accent.green} />}
            <Text textStyle="text-ui-med" color={colors.foreground.secondary}>
              SUBSCRIBE
            </Text>
          </HStack>
          {progressTimestamp && (
            <Text
              textStyle="text-ui-reg"
              color={isStreaming ? "purple.400" : colors.foreground.secondary}
            >
              {progressTimestamp}
            </Text>
          )}
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            {materializedRows.length} row
            {materializedRows.length !== 1 ? "s" : ""}
            {showDiffs && ` (${diffRows.length} diffs)`}
          </Text>
        </HStack>
        <HStack spacing="4">
          <HStack spacing="2">
            <Text
              as="label"
              htmlFor="subscribe-diff-toggle"
              textStyle="text-ui-med"
              userSelect="none"
              cursor="pointer"
            >
              Show diffs
            </Text>
            <Switch
              id="subscribe-diff-toggle"
              isChecked={showDiffs}
              onChange={(e) => setShowDiffs(e.target.checked)}
            />
          </HStack>
          <Button
            size="sm"
            variant="outline"
            onClick={onStop}
            isDisabled={!isStreaming}
          >
            Stop
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

      <SqlSelectTable
        colNames={displayColNames}
        cols={displayColumns}
        paginatedRows={paginatedRows}
        rows={displayRows}
      />

      {totalPages > 1 && (
        <TablePagination
          totalPages={totalPages}
          totalNumRows={formattedRows.length}
          currentPage={displayPage}
          startIndex={startIndex}
          endIndex={endIndex}
          pageSize={PAGE_SIZE}
          prevEnabled={displayPage > 0}
          nextEnabled={displayPage < totalPages - 1}
          isFollowing={isFollowing}
          onToggleFollow={() => setIsFollowing((f) => !f)}
          onPrevPage={() => {
            setIsFollowing(false);
            setCurrentPage((p) => Math.max(0, p - 1));
          }}
          onNextPage={() => {
            setCurrentPage((p) => Math.min(totalPages - 1, p + 1));
          }}
          px="4"
          py="2"
        />
      )}
    </Box>
  );
};

export default SubscribeView;
