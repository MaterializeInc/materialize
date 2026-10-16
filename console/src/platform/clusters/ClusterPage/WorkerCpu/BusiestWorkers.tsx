// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Grid, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { formatDate, TIME_FORMAT } from "~/utils/dateFormat";
import { formatPercentage } from "~/utils/format";

import {
  objectCpuRows,
  processOfWorker,
  topObjectOnWorker,
  WorkerBusy,
  WorkerReplica,
} from "./workerCpuModel";

const TOP_WORKER_COUNT = 5;

/** A filled span of a track, from `from` to `to`, each a fraction of it. */
export const TrackBar = ({
  from = 0,
  to,
  color,
  height = "10px",
}: {
  from?: number;
  to: number;
  color: string;
  height?: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      flex="1"
      minWidth="80px"
      height={height}
      borderRadius="sm"
      background={colors.background.tertiary}
      position="relative"
    >
      <Box
        position="absolute"
        top={0}
        bottom={0}
        left={`${Math.min(1, from) * 100}%`}
        width={`${Math.max(0.5, (Math.min(1, to) - from) * 100)}%`}
        borderRadius="sm"
        background={color}
      />
    </Box>
  );
};

const WorkerRow = ({
  label,
  bar,
  value,
  note,
  hasBorder,
}: {
  label: React.ReactNode;
  bar: React.ReactNode;
  value: string;
  note?: string;
  hasBorder: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Grid
      as="li"
      templateColumns="140px minmax(0, 1fr) 80px 280px"
      gap={4}
      alignItems="center"
      paddingY={1.5}
      borderBottomWidth={hasBorder ? "1px" : 0}
      borderColor={colors.border.primary}
    >
      {label}
      {bar}
      <Text textStyle="text-small" textAlign="right" whiteSpace="nowrap">
        {value}
      </Text>
      <Text
        textStyle="text-small"
        color={colors.foreground.secondary}
        overflow="hidden"
        textOverflow="ellipsis"
        whiteSpace="nowrap"
      >
        {note}
      </Text>
    </Grid>
  );
};

export interface BusiestWorkersProps {
  /** Busiest first. */
  workers: WorkerBusy[];
  sinceMs: number;
  /** Without skew, a ranked list only reorders noise, so one range shows. */
  isSkewed: boolean;
  objects: ReturnType<typeof objectCpuRows> | undefined;
  objectName: (objectId: string) => string;
  replica: WorkerReplica;
}

export const BusiestWorkers = ({
  workers,
  sinceMs,
  isSkewed,
  objects,
  objectName,
  replica,
}: BusiestWorkersProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const top = isSkewed ? workers.slice(0, TOP_WORKER_COUNT) : [];
  const rest = workers.slice(top.length);
  const restLowest = rest.at(-1)?.busyFraction;
  const restHighest = rest.at(0)?.busyFraction;
  return (
    <VStack spacing={2} alignItems="stretch">
      <HStack spacing={2} alignItems="baseline">
        <Text textStyle="text-ui-med">
          {isSkewed ? "Busiest workers" : "Workers"}
        </Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          % of one core spent running dataflows since{" "}
          {formatDate(new Date(sinceMs), TIME_FORMAT)}
        </Text>
      </HStack>
      <VStack as="ul" spacing={0} alignItems="stretch" listStyleType="none">
        {top.map((worker, index) => {
          const driver =
            objects &&
            topObjectOnWorker(
              objects.rows,
              objects.workerTotalsNs,
              worker.workerId,
            );
          return (
            <WorkerRow
              key={worker.workerId}
              hasBorder
              label={
                <HStack spacing={1.5} alignItems="baseline">
                  <Text textStyle={index === 0 ? "text-ui-med" : "text-ui-reg"}>
                    w{worker.workerId}
                  </Text>
                  {replica.processes > 1 && (
                    <Text
                      textStyle="text-small"
                      color={colors.foreground.secondary}
                    >
                      process{" "}
                      {processOfWorker(
                        worker.workerId,
                        replica.workersPerProcess,
                      )}
                    </Text>
                  )}
                </HStack>
              }
              bar={
                <TrackBar
                  to={worker.busyFraction}
                  color={
                    index === 0
                      ? colors.accent.orange
                      : colors.foreground.tertiary
                  }
                />
              }
              value={formatPercentage(worker.busyFraction, 0)}
              note={
                driver &&
                `mostly ${objectName(driver.objectId)} · ${formatPercentage(driver.share, 0)} of its time`
              }
            />
          );
        })}
        {restLowest !== undefined && restHighest !== undefined && (
          <WorkerRow
            hasBorder={false}
            label={
              <Text textStyle="text-small">
                {top.length > 0
                  ? `Other ${rest.length} workers`
                  : `All ${rest.length} workers`}
              </Text>
            }
            bar={
              <TrackBar
                from={restLowest}
                to={restHighest}
                color={colors.foreground.tertiary}
              />
            }
            value={`${formatPercentage(restLowest, 0)}–${formatPercentage(restHighest, 0)}`}
          />
        )}
      </VStack>
    </VStack>
  );
};
