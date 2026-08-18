// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { ConnectorStatusInfo, snapshotting } from "~/platform/connectors/utils";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

export interface SnapshotProgressProps {
  snapshotRecordsKnown: number;
  snapshotRecordsStaged: number;
  source: ConnectorStatusInfo;
}

const numberFormatter = Intl.NumberFormat("default");

const mapSourceTypeToProgressUnit = (type: string, count: number) => {
  switch (type) {
    case "postgres":
    case "mysql":
      return pluralize(count, "row", "rows");
    case "kafka":
      return pluralize(count, "message", "messages");
    case "webhook":
      return pluralize(count, "event", "events");
    default:
      return pluralize(count, "record", "records");
  }
};

// The known count for database sources comes from table statistics (Postgres
// `reltuples` and equivalents), which are estimates that drift until an ANALYZE.
// Kafka derives it from offset watermarks, which overcount compacted or
// transactional topics, so the note stays type-specific rather than claiming
// table statistics everywhere.
const mapSourceTypeToEstimateNote = (type: string) => {
  switch (type) {
    case "postgres":
    case "mysql":
    case "sql-server":
      return "Snapshot progress and row counts are estimates, based on table statistics.";
    case "kafka":
      return "Snapshot progress and message counts are estimates.";
    default:
      return "Snapshot progress and record counts are estimates.";
  }
};
export const SnapshotProgress = ({
  snapshotRecordsKnown,
  snapshotRecordsStaged,
  source,
}: SnapshotProgressProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  if (!snapshotting(source)) return;

  let ratio = snapshotRecordsStaged / snapshotRecordsKnown;
  ratio = Number.isNaN(ratio) ? 0 : ratio;
  const percent = Math.floor(ratio * 100);

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      padding="4"
      borderRadius="lg"
      boxShadow={shadows.level1}
      spacing="4"
    >
      <HStack width="100%" justifyContent="space-between">
        <Box>
          <Text textStyle="text-ui-med">Snapshotting progress</Text>
          <Text textStyle="text-small">
            Processing the initial snapshot of your source.
          </Text>
        </Box>
        <Text textStyle="heading-md">{percent}%</Text>
      </HStack>
      <VStack spacing="2" width="100%">
        <HStack
          spacing="0"
          height="2"
          width="100%"
          borderRadius="lg"
          overflow="hidden"
        >
          <Box
            height="2"
            flexGrow={ratio}
            backgroundColor={colors.accent.brightPurple}
          />
          <Box
            height="2"
            flexGrow={1 - ratio}
            backgroundColor={colors.background.secondary}
          />
        </HStack>
        <HStack width="100%" justifyContent="space-between">
          <Text textStyle="text-small-heavy">
            {numberFormatter.format(snapshotRecordsStaged)}
          </Text>
          <Text textStyle="text-small-heavy">
            {numberFormatter.format(snapshotRecordsKnown)}{" "}
            {mapSourceTypeToProgressUnit(source.type, snapshotRecordsKnown)}
          </Text>
        </HStack>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {mapSourceTypeToEstimateNote(source.type)}
        </Text>
      </VStack>
    </VStack>
  );
};
