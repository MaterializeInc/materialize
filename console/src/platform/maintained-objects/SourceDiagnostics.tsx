// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Card, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { DetailItem } from "~/platform/connectors/AsideBox";
import { MaterializeTheme } from "~/theme";
import { sumPostgresIntervalMs } from "~/util";

import { useObjectSourceStatistics } from "./queries";

export interface SourceDiagnosticsProps {
  sourceId: string;
}

export const SourceDiagnostics = ({ sourceId }: SourceDiagnosticsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: stats } = useObjectSourceStatistics(sourceId);

  if (!stats) return null;

  const {
    messagesReceived,
    snapshotRecordsKnown: snapshotKnown,
    snapshotRecordsStaged: snapshotStaged,
    rehydrationLatency,
  } = stats;
  const snapshotPercent =
    snapshotKnown > 0
      ? Math.round((snapshotStaged / snapshotKnown) * 100)
      : null;
  const snapshotComplete = snapshotPercent === null || snapshotPercent === 100;
  const rehydrationMs = rehydrationLatency
    ? sumPostgresIntervalMs(rehydrationLatency)
    : null;

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <Text textStyle="heading-sm">Source diagnostics</Text>

        <VStack align="stretch" spacing={2} width="100%">
          <DetailItem
            label="Rehydration latency"
            color={
              rehydrationMs === null
                ? colors.accent.orange
                : colors.foreground.primary
            }
          >
            {rehydrationMs !== null
              ? `${(rehydrationMs / 1000).toFixed(1)}s`
              : "Still rehydrating..."}
          </DetailItem>
          <DetailItem label="Messages received">
            {messagesReceived.toLocaleString()}
          </DetailItem>
          {snapshotComplete ? (
            <DetailItem label="Snapshot" color={colors.accent.green}>
              Complete
            </DetailItem>
          ) : (
            <>
              <DetailItem
                label="Snapshot progress"
                color={colors.accent.orange}
              >
                {`${snapshotPercent}% (${snapshotStaged.toLocaleString()} / ${snapshotKnown.toLocaleString()} records)`}
              </DetailItem>
              <Box
                width="100%"
                height="1.5"
                borderRadius="full"
                bg={colors.background.secondary}
              >
                <Box
                  height="100%"
                  width={`${snapshotPercent}%`}
                  borderRadius="full"
                  bg={colors.accent.brightPurple}
                  transition="width 0.3s ease"
                />
              </Box>
            </>
          )}
        </VStack>
      </VStack>
    </Card>
  );
};

export default SourceDiagnostics;
