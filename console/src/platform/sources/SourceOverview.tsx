// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Stack, Tooltip, VStack } from "@chakra-ui/react";
import React from "react";

import { Source } from "~/api/materialize/source/sourceList";
import Alert from "~/components/Alert";
import { ClusterMetrics } from "~/components/ClusterMetrics";
import { CopyButton } from "~/components/copyableComponents";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { InfoIcon } from "~/icons";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { AsideBox, DetailItem } from "~/platform/connectors/AsideBox";
import { usePageHeadingRef } from "~/store/stickyHeader";
import { prettyConnectorType } from "~/util";
import { formatDate, FRIENDLY_DATE_FORMAT } from "~/utils/dateFormat";
import { formatInterval, formatIntervalShort } from "~/utils/format";

import OverviewHeader from "../connectors/OverviewHeader";
import { SourceListResponse, useCurrentSourceStatistics } from "./queries";
import { SnapshotProgress } from "./SourceOverview/SnapshotProgress";
import { SourceStatistics } from "./SourceOverview/SourceStatistics";

export interface SourceOverviewProps {
  sourcesResponse: SourceListResponse;
}

export const SourceOverview = ({ source }: { source: Source }) => {
  const ref = usePageHeadingRef();
  const [timePeriodMinutes, setInitialTimePeriodMinutes] = useTimePeriodMinutes(
    { localStorageKey: "mz-source-stats-time-period" },
  );
  const {
    data: { rows },
  } = useCurrentSourceStatistics({ sourceId: source.id });
  const stats = rows.at(0);

  return (
    <MainContentContainer>
      <VStack width="100%" alignItems="flex-start" spacing={6}>
        {source.error && (
          <Alert variant="error" width="100%" message={source.error} />
        )}
        <Stack
          alignItems="flex-start"
          spacing={10}
          width="100%"
          flexDirection={{ base: "column", xl: "row" }}
        >
          <VStack width="100%" alignItems="flex-start" spacing={6}>
            <OverviewHeader
              connector={source}
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setInitialTimePeriodMinutes}
              clusterName={source.clusterName}
              replicaName={stats?.replicaName}
              ref={ref}
            />
            {stats && (
              <SnapshotProgress
                snapshotRecordsKnown={stats.snapshotRecordsKnown}
                snapshotRecordsStaged={stats.snapshotRecordsStaged}
                source={source}
              />
            )}
            <SourceStatistics
              source={source}
              timePeriodMinutes={timePeriodMinutes}
            />
          </VStack>
          <Stack
            alignItems={{ base: "center", md: "flex-start" }}
            flexDirection={{ base: "column", md: "row", xl: "column" }}
            flexShrink="0"
            justifyContent="center"
            mb="10"
            spacing="6"
            width={{ base: "100%", xl: "400px" }}
          >
            <ClusterMetrics
              clusterId={source.clusterId}
              clusterName={source.clusterName}
              replicaName={stats?.replicaName}
              width="400px"
            />
            <AsideBox title="Details" width="400px">
              <DetailItem label="Type">
                {prettyConnectorType(source.type)}
              </DetailItem>
              {source.webhookUrl && (
                <DetailItem
                  label="URL"
                  whiteSpace="nowrap"
                  display="block"
                  rightGutter={
                    <CopyButton size="xs" contents={source.webhookUrl} />
                  }
                >
                  {source.webhookUrl}
                </DetailItem>
              )}
              {source.connectionName && (
                <DetailItem label="Connection">
                  {source.connectionName}
                </DetailItem>
              )}
              {source.kafkaTopic && (
                <DetailItem
                  label="Topic"
                  rightGutter={
                    <CopyButton size="xs" contents={source.kafkaTopic} />
                  }
                >
                  {source.kafkaTopic}
                </DetailItem>
              )}
              <DetailItem label="Created at">
                {formatDate(source.createdAt, FRIENDLY_DATE_FORMAT)}
              </DetailItem>
              {stats?.rehydrationLatency && (
                <DetailItem
                  label={
                    <>
                      Approx. hydration time{" "}
                      <Tooltip label="Rehydration time reflects the most recent hydration of the source. Time will vary depending on cluster replica size and data volumes.">
                        <InfoIcon />
                      </Tooltip>
                    </>
                  }
                >
                  <Tooltip label={formatInterval(stats.rehydrationLatency)}>
                    {formatIntervalShort(stats.rehydrationLatency)}
                  </Tooltip>
                </DetailItem>
              )}
            </AsideBox>
          </Stack>
        </Stack>
      </VStack>
    </MainContentContainer>
  );
};
