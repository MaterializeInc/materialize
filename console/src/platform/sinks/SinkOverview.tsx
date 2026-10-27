// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Stack, VStack } from "@chakra-ui/react";
import React from "react";

import { Sink } from "~/api/materialize/sink/sinkList";
import Alert from "~/components/Alert";
import { ClusterMetrics } from "~/components/ClusterMetrics";
import { CopyButton } from "~/components/copyableComponents";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { AsideBox, DetailItem } from "~/platform/connectors/AsideBox";
import { usePageHeadingRef } from "~/store/stickyHeader";
import { prettyConnectorType } from "~/util";
import { formatDate, FRIENDLY_DATE_FORMAT } from "~/utils/dateFormat";

import OverviewHeader from "../connectors/OverviewHeader";
import { useCurrentSinkStatistics } from "./queries";
import { SinkStatistics } from "./SinkOverview/SinkStatistics";

const SinkOverview = ({ sink }: { sink: Sink }) => {
  const ref = usePageHeadingRef();
  const [timePeriodMinutes, setInitialTimePeriodMinutes] = useTimePeriodMinutes(
    { localStorageKey: "mz-sink-stats-time-period" },
  );
  const {
    data: { rows },
  } = useCurrentSinkStatistics({ sinkId: sink.id });
  const stats = rows.at(0);
  return (
    <MainContentContainer>
      <VStack width="100%" alignItems="flex-start" spacing={6}>
        {sink.error && (
          <Alert variant="error" width="100%" message={sink.error} />
        )}
        <Stack
          alignItems="flex-start"
          spacing={10}
          width="100%"
          flexDirection={{ base: "column", xl: "row" }}
        >
          <VStack width="100%" alignItems="flex-start" spacing={6}>
            <OverviewHeader
              connector={sink}
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setInitialTimePeriodMinutes}
              clusterName={sink.clusterName}
              replicaName={stats?.replicaName}
              ref={ref}
            />
            <SinkStatistics sink={sink} timePeriodMinutes={timePeriodMinutes} />
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
              clusterId={sink.clusterId}
              clusterName={sink.clusterName}
              replicaName={stats?.replicaName}
              width="400px"
            />
            <AsideBox title="Details" width="400px">
              <DetailItem label="Type">
                {prettyConnectorType(sink.type)}
              </DetailItem>
              {sink.connectionName && (
                <DetailItem label="Connection">
                  {sink.connectionName}
                </DetailItem>
              )}
              {sink.kafkaTopic && (
                <DetailItem
                  label="Topic"
                  rightGutter={
                    <CopyButton size="xs" contents={sink.kafkaTopic} />
                  }
                >
                  {sink.kafkaTopic}
                </DetailItem>
              )}
              <DetailItem label="Created at">
                {formatDate(sink.createdAt, FRIENDLY_DATE_FORMAT)}
              </DetailItem>
            </AsideBox>
          </Stack>
        </Stack>
      </VStack>
    </MainContentContainer>
  );
};

export default SinkOverview;
