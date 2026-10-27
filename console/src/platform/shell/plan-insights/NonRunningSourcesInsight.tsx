// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  Code,
  HStack,
  ListItem,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";

import { formatFullyQualifiedObjectName } from "~/api/materialize";
import { useBuildSourcePath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";

import {
  NoticeContainer,
  NoticeContent,
  NoticeFooter,
  NoticeUnorderedList,
} from "./planInsightsComponents";
import { PlanInsights } from "./PlanInsightsNotice";

type NonRunningSourcesInsightProps = {
  planInsights: PlanInsights;
};

export const INSTRUMENTATION_ID = "nonRunningSourceDependencies";
export const VERSION_NUMBER = "1";
export const VERSIONED_ID = `${INSTRUMENTATION_ID}V${VERSION_NUMBER}`;

const NonRunningSourcesInsight = ({
  planInsights,
}: NonRunningSourcesInsightProps) => {
  const sourcePath = useBuildSourcePath();

  const { colors } = useTheme<MaterializeTheme>();
  const nonRunningSources = Object.entries(planInsights.nonRunningSources);

  return (
    <NoticeContainer>
      <NoticeContent>
        <HStack>
          <Text color={colors.foreground.primary} textStyle="text-ui-med">
            Your query is blocked by an unavailable source
          </Text>
        </HStack>
        <Text color={colors.foreground.primary} textStyle="text-base">
          Your query depends on at least one source that is unavailable.
        </Text>
        <NoticeUnorderedList
          list={nonRunningSources.map(
            ([id, { status, databaseName, schemaName, name }]) => {
              const databaseObject = {
                id,
                name,
                databaseName,
                schemaName,
              };
              const fullyQualifiedName =
                formatFullyQualifiedObjectName(databaseObject);

              return (
                <ListItem key={fullyQualifiedName}>
                  <Text textStyle="text-base">
                    Source{" "}
                    <Code
                      variant="inline-syntax"
                      backgroundColor={colors.background.tertiary}
                      size="xs"
                    >
                      {fullyQualifiedName}
                    </Code>{" "}
                    is in status{" "}
                    <Code
                      variant="inline-syntax"
                      backgroundColor={colors.background.tertiary}
                      size="xs"
                    >
                      {status}
                    </Code>{" "}
                  </Text>
                  <Button
                    variant="primary"
                    size="xs"
                    as={Link}
                    to={sourcePath(databaseObject)}
                    mt="1"
                  >
                    Status page
                  </Button>
                </ListItem>
              );
            },
          )}
        />
      </NoticeContent>
      <NoticeFooter
        insightVersionedId={VERSIONED_ID}
        redactedSql={planInsights.redactedSql}
      />
    </NoticeContainer>
  );
};

export default NonRunningSourcesInsight;
