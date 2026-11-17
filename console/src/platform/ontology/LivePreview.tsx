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
  Spinner,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { buildLivePreviewSubscribe } from "~/api/materialize/ontology/ontology";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useSubscribe } from "~/api/materialize/useSubscribe";
import { PlayIcon, StopIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { OntologyNode } from "./model";

const PREVIEW_ROW_LIMIT = 25;
/** How long a changed row stays highlighted. */
const CHANGE_HIGHLIGHT_MS = 1_500;

export interface LivePreviewProps {
  node: OntologyNode;
  /** Where the SUBSCRIBE runs. Null when no cluster can read the relation. */
  clusterName: string | null;
}

/**
 * A few rows of the relation, streamed by SUBSCRIBE while open. It starts on
 * request because the SUBSCRIBE maintains a dataflow on `clusterName`.
 */
export const LivePreview = ({ node, clusterName }: LivePreviewProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [isRunning, setIsRunning] = React.useState(false);
  const columns = node.columns?.map((column) => column.name) ?? [];

  if (!node.relation || node.isUnresolved) return null;
  if (columns.length === 0) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        No columns are known for this relation.
      </Text>
    );
  }
  if (!clusterName) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        No cluster is known to read this relation from. Index it, or index the
        ontology table, to preview its rows here.
      </Text>
    );
  }

  return (
    <VStack align="stretch" spacing={3}>
      <HStack justify="space-between">
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {isRunning
            ? `Streaming up to ${PREVIEW_ROW_LIMIT} rows on ${clusterName}.`
            : `Streams up to ${PREVIEW_ROW_LIMIT} rows with SUBSCRIBE on ${clusterName}.`}
        </Text>
        <Button
          size="sm"
          variant={isRunning ? "secondary" : "primary"}
          leftIcon={isRunning ? <StopIcon /> : <PlayIcon />}
          onClick={() => setIsRunning((running) => !running)}
          flexShrink={0}
        >
          {isRunning ? "Stop" : "Watch live"}
        </Button>
      </HStack>
      {isRunning && (
        // Keyed so a different relation or cluster starts a new SUBSCRIBE,
        // since the session's cluster is fixed when the socket opens.
        <LivePreviewRows
          key={`${node.relation}|${clusterName}`}
          relation={node.relation}
          columns={columns}
          clusterName={clusterName}
        />
      )}
    </VStack>
  );
};

const rowKey = (row: SubscribeRow<Record<string, unknown>>) =>
  JSON.stringify(row.data, (_, value) =>
    typeof value === "bigint" ? value.toString() : value,
  );

const formatCell = (value: unknown) => {
  if (value === null || value === undefined) return "NULL";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const LivePreviewRows = ({
  relation,
  columns,
  clusterName,
}: {
  relation: string;
  columns: string[];
  clusterName: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const columnsKey = columns.join(",");
  const subscribe = React.useMemo(
    () => buildLivePreviewSubscribe(relation, columns, PREVIEW_ROW_LIMIT),
    // `columns` is keyed by its contents.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [relation, columnsKey],
  );
  const { data, error, snapshotComplete } = useSubscribe({
    subscribe,
    clusterName,
    upsertKey: rowKey,
  });
  const latestTimestamp = data.reduce(
    (max, row) => Math.max(max, row.mzTimestamp),
    0,
  );
  const [snapshotTimestamp, setSnapshotTimestamp] = React.useState<
    number | null
  >(null);
  if (snapshotComplete && snapshotTimestamp === null && latestTimestamp > 0) {
    setSnapshotTimestamp(latestTimestamp);
  }

  if (error) {
    return (
      <Text textStyle="text-small" color={colors.accent.red}>
        {error.message}
      </Text>
    );
  }
  if (!snapshotComplete) {
    return (
      <HStack>
        <Spinner size="xs" />
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Waiting for the snapshot…
        </Text>
      </HStack>
    );
  }
  if (data.length === 0) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        No rows yet. New rows appear here as they arrive.
      </Text>
    );
  }
  return (
    <Box
      overflowX="auto"
      border="1px solid"
      borderColor={colors.border.primary}
      borderRadius="md"
    >
      <Table variant="borderless" size="sm">
        <Thead>
          <Tr>
            {columns.map((column) => (
              <Th key={column} whiteSpace="nowrap">
                {column}
              </Th>
            ))}
          </Tr>
        </Thead>
        <Tbody>
          {data.map((row) => {
            // Rows from the snapshot are not news, only later changes are.
            const isChanged =
              snapshotTimestamp !== null &&
              row.mzTimestamp > snapshotTimestamp &&
              latestTimestamp - row.mzTimestamp < CHANGE_HIGHLIGHT_MS;
            return (
              <Tr
                key={rowKey(row)}
                background={isChanged ? colors.background.accent : undefined}
                transition="background 600ms ease-out"
              >
                {columns.map((column) => (
                  <Td
                    key={column}
                    whiteSpace="nowrap"
                    fontFamily="mono"
                    fontSize="xs"
                    maxW="240px"
                    overflow="hidden"
                    textOverflow="ellipsis"
                  >
                    {formatCell(row.data[column])}
                  </Td>
                ))}
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </Box>
  );
};
