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
  Card,
  HStack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { formatBytesShort } from "~/utils/format";

export interface ObjectMemoryCardProps {
  memoryBytes: string | null;
  replicaTotalMemoryBytes: string | null;
  replicaName: string | null;
  replicaSize: string | null;
}

export const ObjectMemoryCard = ({
  memoryBytes,
  replicaTotalMemoryBytes,
  replicaName,
  replicaSize,
}: ObjectMemoryCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!memoryBytes) return null;

  const objectBytes = BigInt(memoryBytes);
  const totalBytes = replicaTotalMemoryBytes
    ? BigInt(replicaTotalMemoryBytes)
    : null;
  const percent =
    totalBytes && totalBytes > 0n
      ? Math.round(Number((objectBytes * 100n) / totalBytes))
      : null;

  const barColor =
    percent !== null && percent > 90
      ? colors.accent.red
      : percent !== null && percent > 70
        ? colors.accent.orange
        : colors.accent.green;

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <Text textStyle="heading-sm">Memory Usage</Text>

        <HStack width="100%" justify="space-between">
          <Text textStyle="heading-lg">
            {formatBytesShort(objectBytes)}
          </Text>
          {percent !== null && (
            <Text textStyle="heading-md" color={barColor}>
              {percent}%
            </Text>
          )}
        </HStack>

        {totalBytes && (
          <>
            <Box
              width="100%"
              height="12px"
              borderRadius="full"
              bg={colors.background.secondary}
              overflow="hidden"
            >
              <Box
                height="100%"
                minWidth={objectBytes > 0n ? "4px" : "0px"}
                width={`${Math.min(percent ?? 0, 100)}%`}
                borderRadius="full"
                bg={barColor}
                transition="width 0.3s ease"
              />
            </Box>

            <HStack width="100%" justify="space-between">
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
              >
                Object arrangement size
              </Text>
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
              >
                {formatBytesShort(totalBytes)} total
                {replicaName && ` (${replicaName}${replicaSize ? ` / ${replicaSize}` : ""})`}
              </Text>
            </HStack>
          </>
        )}

        {!totalBytes && (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
          >
            Replica memory limit not available
          </Text>
        )}
      </VStack>
    </Card>
  );
};

export default ObjectMemoryCard;
