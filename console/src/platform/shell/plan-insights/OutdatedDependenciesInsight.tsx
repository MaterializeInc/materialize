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
import docUrls from "~/mz-doc-urls.json";
import { useBuildWorkflowGraphPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import { capitalizeSentence } from "~/util";

import {
  NoticeContainer,
  NoticeContent,
  NoticeExternalLink,
  NoticeFooter,
  NoticeUnorderedList,
} from "./planInsightsComponents";
import { PlanInsights } from "./PlanInsightsNotice";
import { determineObjectType } from "./utils";

type OutdatedDependenciesInsightsProps = {
  planInsights: PlanInsights;
};

export const INSTRUMENTATION_ID = "laggingDependencies";
export const VERSION_NUMBER = "1";
export const VERSIONED_ID = `${INSTRUMENTATION_ID}V${VERSION_NUMBER}`;

const OutdatedDependenciesInsights = ({
  planInsights,
}: OutdatedDependenciesInsightsProps) => {
  const workflowGraphPath = useBuildWorkflowGraphPath();
  const { colors } = useTheme<MaterializeTheme>();
  const blockedDependencies = Object.entries(planInsights.blockedDependencies);

  const outdatedDependencies = blockedDependencies.filter(
    ([_, { isOutdated }]) => !!isOutdated,
  );

  return (
    <NoticeContainer>
      <NoticeContent>
        <HStack>
          <Text color={colors.foreground.primary} textStyle="text-ui-med">
            Your query is blocked on lagging objects
          </Text>
        </HStack>
        <Text color={colors.foreground.primary} textStyle="text-base">
          Your query depends on at least one object that is lagging behind
          upstream objects.
        </Text>
        <NoticeUnorderedList
          list={outdatedDependencies.map(
            ([id, { type, sourceType, clusterId, clusterName }]) => {
              const objectType = determineObjectType({
                objectType: type,
                sourceType,
              });

              const { name } = planInsights.imports[id] ?? {};
              const databaseObject = {
                id,
                name: name.item,
                databaseName: name.database ?? null,
                schemaName: name.schema,
              };
              const fullyQualifiedName =
                formatFullyQualifiedObjectName(databaseObject);
              const workflowGraphLink = workflowGraphPath({
                type: objectType,
                databaseObject,
                clusterId,
                clusterName,
              });
              return (
                <ListItem key={fullyQualifiedName}>
                  <Text textStyle="text-base">
                    {capitalizeSentence(objectType, false)}{" "}
                    <Code
                      variant="inline-syntax"
                      backgroundColor={colors.background.tertiary}
                      size="xs"
                    >
                      {fullyQualifiedName}
                    </Code>{" "}
                  </Text>
                  {workflowGraphLink && (
                    <Button
                      as={Link}
                      to={workflowGraphLink}
                      variant="primary"
                      size="xs"
                      my="2"
                    >
                      Workflow graph
                    </Button>
                  )}
                </ListItem>
              );
            },
          )}
        />
      </NoticeContent>
      <NoticeFooter
        insightVersionedId={VERSIONED_ID}
        redactedSql={planInsights.redactedSql}
      >
        <NoticeExternalLink
          insightVersionedId={VERSIONED_ID}
          href={`${docUrls["/docs/transform-data/troubleshooting/"]}#lagging-materialized-views-or-indexes`}
          redactedSql={planInsights.redactedSql}
        >
          How to resolve lag
        </NoticeExternalLink>
      </NoticeFooter>
    </NoticeContainer>
  );
};

export default OutdatedDependenciesInsights;
