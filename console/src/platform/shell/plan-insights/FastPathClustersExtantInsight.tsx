// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, HStack, ListItem, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { formatFullyQualifiedObjectName } from "~/api/materialize";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

import {
  NoticeContainer,
  NoticeContent,
  NoticeExternalLink,
  NoticeFooter,
  NoticeUnorderedList,
} from "./planInsightsComponents";
import { PlanInsights } from "./PlanInsightsNotice";

type FastPathClustersExtantInsightProps = {
  planInsights: PlanInsights;
};

export const INSTRUMENTATION_ID = "fastPathClustersExtant";
export const VERSION_NUMBER = "1";
export const VERSIONED_ID = `${INSTRUMENTATION_ID}V${VERSION_NUMBER}`;

const FastPathClustersExtantInsight = ({
  planInsights,
}: FastPathClustersExtantInsightProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const fastPathClusters = Object.entries(planInsights.fastPathClusters);

  return (
    <NoticeContainer>
      <NoticeContent>
        <HStack>
          <Text color={colors.foreground.primary} textStyle="text-ui-med">
            You have applicable indexes on other clusters
          </Text>
        </HStack>
        <Text color={colors.foreground.primary} textStyle="text-base">
          Did you mean to run your query on a different cluster, or create the
          index on the current cluster?
        </Text>
        <NoticeUnorderedList
          list={fastPathClusters.map(([clusterName, { index, on }]) => {
            return (
              <ListItem key={clusterName}>
                <Text color={colors.foreground.primary} textStyle="text-base">
                  Index{" "}
                  <Code
                    variant="inline-syntax"
                    backgroundColor={colors.background.tertiary}
                    size="xs"
                  >
                    {formatFullyQualifiedObjectName({
                      name: index.item,
                      databaseName: index.database,
                      schemaName: index.schema,
                    })}
                  </Code>{" "}
                  over{" "}
                  <Code
                    variant="inline-syntax"
                    backgroundColor={colors.background.tertiary}
                    size="xs"
                  >
                    {formatFullyQualifiedObjectName({
                      name: on.item,
                      databaseName: on.database,
                      schemaName: on.schema,
                    })}
                  </Code>{" "}
                  is on cluster{" "}
                  <Code
                    variant="inline-syntax"
                    backgroundColor={colors.background.tertiary}
                    size="xs"
                  >
                    {formatFullyQualifiedObjectName({ name: clusterName })}
                  </Code>
                </Text>
              </ListItem>
            );
          })}
        />
      </NoticeContent>

      <NoticeFooter
        insightVersionedId={VERSIONED_ID}
        redactedSql={planInsights.redactedSql}
      >
        <NoticeExternalLink
          insightVersionedId={VERSIONED_ID}
          href={`${
            docUrls["/docs/transform-data/troubleshooting/"]
          }#indexing-and-query-optimization`}
          redactedSql={planInsights.redactedSql}
        >
          How to debug index usage
        </NoticeExternalLink>
      </NoticeFooter>
    </NoticeContainer>
  );
};

export default FastPathClustersExtantInsight;
