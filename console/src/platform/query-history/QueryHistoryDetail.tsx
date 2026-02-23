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
  Grid,
  HStack,
  Spinner,
  Stack,
  Text,
  TextProps,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import React from "react";
import { Link, Navigate, useLocation, useParams } from "react-router-dom";

import { QueryHistoryStatementInfoRow } from "~/api/materialize/query-history/queryHistoryDetail";
import { QueryHistoryListRow } from "~/api/materialize/query-history/queryHistoryList";
import { CopyButton } from "~/components/copyableComponents";
import ErrorBox from "~/components/ErrorBox";
import { ExpandableCodeBlock } from "~/components/ExpandableCodeBlock";
import StatusPill from "~/components/StatusPill";
import { useFlags } from "~/hooks/useFlags";
import { usePrivileges } from "~/hooks/usePrivileges";
import { useToast } from "~/hooks/useToast";
import {
  MainContentContainer,
  PageBreadcrumbs,
  PageHeader,
  PageTabStrip,
} from "~/layouts/BaseLayout";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useCancelQuery } from "~/queries/cancelQuery";
import { useRegionSlug } from "~/store/environments";
import ExternalLinkIcon from "~/svg/ExternalLinkIcon";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";
import { formatDate, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";
import { formatBytesShort } from "~/utils/format";

import {
  queryHistoryQueryKeys,
  useFetchQueryHistoryStatementInfo,
} from "./queries";
import { UnauthorizedState } from "./QueryHistoryList";
import { ListToDetailsPageLocationState } from "./QueryHistoryRoutes";
import {
  formatDuration,
  isAuthorizedSelector,
  shouldShowRedactedSelector,
} from "./queryHistoryUtils";
import QueryLifecycleCard from "./QueryLifecycleCard";
import {
  ThrottledCountTooltip,
  TransactionIsolationLevelTooltip,
} from "./tooltipComponents";
import { getFinishedStatusColorScheme } from "./utils";

const ACTIVITY_LOG_UPDATE_DELAY_MS = 4_000;

const Breadcrumbs = () => {
  const params = useParams();
  const { state }: { state: ListToDetailsPageLocationState } = useLocation();

  assert(params.id);

  const prevSearchString = state?.from?.search ?? "";

  return (
    <PageBreadcrumbs
      crumbs={[
        {
          title: "Query history",
          href: `../${prevSearchString}`,
        },
        { title: `Query - ${params.id}` },
      ]}
    />
  );
};

const DetailItemContainer = ({
  title,
  children,
}: {
  title: string | React.ReactNode;
  children: React.ReactNode | string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack alignItems="flex-start">
      {typeof title === "string" ? (
        <Text
          textStyle="text-small"
          fontWeight="500"
          color={colors.foreground.secondary}
        >
          {title}
        </Text>
      ) : (
        title
      )}
      <HStack width="100%">{children}</HStack>
    </VStack>
  );
};

const ClusterNameDetailItem = ({
  queryHistoryInfo,
}: {
  queryHistoryInfo: QueryHistoryListRow | QueryHistoryStatementInfoRow;
}) => {
  const regionSlug = useRegionSlug();
  const { colors } = useTheme<MaterializeTheme>();
  const isPlaceholderData = !("clusterId" in queryHistoryInfo);

  const textProps = {
    textStyle: "text-small",
    noOfLines: 1,
    wordBreak: "break-all",
  } as TextProps;

  const doesNotExistElement = <Text {...textProps}>-</Text>;

  if (queryHistoryInfo.clusterName === null) {
    return doesNotExistElement;
  }

  if (isPlaceholderData) {
    return (
      <HStack spacing="1" cursor="not-allowed">
        <Text {...textProps} title={queryHistoryInfo.clusterName ?? "-"}>
          {queryHistoryInfo.clusterName ?? "-"}
        </Text>
        <ExternalLinkIcon ml="1" color={colors.foreground.tertiary} />
      </HStack>
    );
  }

  if (queryHistoryInfo.clusterId === null) {
    return doesNotExistElement;
  }

  const { clusterExists } = queryHistoryInfo;

  const isClusterDropped = !clusterExists;

  if (isClusterDropped) {
    return (
      <HStack spacing="1">
        <Text {...textProps} title={queryHistoryInfo.clusterName}>
          {queryHistoryInfo.clusterName} (dropped)
        </Text>
      </HStack>
    );
  }

  return (
    <HStack
      spacing="1"
      as={Link}
      to={absoluteClusterPath(regionSlug, {
        id: queryHistoryInfo.clusterId,
        name: queryHistoryInfo.clusterName,
      })}
    >
      <Text {...textProps} title={queryHistoryInfo.clusterName}>
        {queryHistoryInfo.clusterName}
      </Text>
      <ExternalLinkIcon ml="1" color={colors.foreground.secondary} />
    </HStack>
  );
};

const horizontalOverflowTextProps = {
  noOfLines: 1,
  wordBreak: "break-all",
} as TextProps;

export const QueryHistoryDetailsCard = ({
  queryHistoryInfo,
  isCancelQuerySuccess,
}: {
  queryHistoryInfo?: QueryHistoryListRow | QueryHistoryStatementInfoRow;
  isCancelQuerySuccess?: boolean;
}) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const flags = useFlags();

  if (!queryHistoryInfo) {
    return null;
  }

  const {
    duration,
    startTime,
    endTime,
    executionId,
    sessionId,
    executionStrategy,
    authenticatedUser,
    applicationName,
    rowsReturned,
    resultSize,
    transactionIsolation,
    throttledCount,
  } = queryHistoryInfo;

  const databaseVersion =
    "databaseVersion" in queryHistoryInfo
      ? queryHistoryInfo.databaseVersion
      : "-";

  const databaseName =
    "databaseName" in queryHistoryInfo ? queryHistoryInfo.databaseName : "-";

  const searchPath =
    "searchPath" in queryHistoryInfo
      ? queryHistoryInfo.searchPath.join(", ")
      : "-";

  const finishedStatus = isCancelQuerySuccess
    ? "canceled"
    : queryHistoryInfo.finishedStatus;

  return (
    <Box
      shadow={shadows.level1}
      background={colors.components.card.background}
      borderRadius="lg"
      p="10"
      width="100%"
      minW="460px"
    >
      <Grid gridTemplateColumns="1fr 1fr 1fr" gap="10">
        <DetailItemContainer title="Status">
          <StatusPill
            status={finishedStatus}
            colorScheme={getFinishedStatusColorScheme(finishedStatus)}
          />
        </DetailItemContainer>
        <DetailItemContainer
          title={
            <Text
              textStyle="text-small"
              noOfLines={1}
              fontWeight="500"
              color={colors.foreground.secondary}
            >
              Transaction isolation level
              <TransactionIsolationLevelTooltip
                marginLeft="1"
                marginBottom="1"
              />
            </Text>
          }
        >
          <Text textStyle="text-small" noOfLines={1}>
            {transactionIsolation}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer title="Database version">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {databaseVersion}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer title="Query ID">
          <HStack width="100%">
            <Text textStyle="text-small" {...horizontalOverflowTextProps}>
              {executionId}
            </Text>
            <CopyButton
              color={colors.foreground.secondary}
              contents={executionId}
              size="xs"
              height="4"
            />
          </HStack>
        </DetailItemContainer>
        <DetailItemContainer title="Cluster name">
          <ClusterNameDetailItem queryHistoryInfo={queryHistoryInfo} />
        </DetailItemContainer>
        <DetailItemContainer title="Session ID">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {sessionId}
          </Text>
          <CopyButton
            color={colors.foreground.secondary}
            contents={sessionId}
            size="xs"
            height="4"
          />
        </DetailItemContainer>
        <DetailItemContainer title="Execution strategy">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {executionStrategy ?? "-"}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer title="User">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {authenticatedUser}
          </Text>
          <CopyButton
            color={colors.foreground.secondary}
            contents={authenticatedUser}
            size="xs"
            height="4"
          />
        </DetailItemContainer>
        <DetailItemContainer title="Application name">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {applicationName.length > 0 ? applicationName : "-"}
          </Text>
          {applicationName.length > 0 && (
            <CopyButton
              color={colors.foreground.secondary}
              contents={applicationName}
              size="xs"
              height="4"
            />
          )}
        </DetailItemContainer>
        {!flags["query-history-statement-lifecycle-952"] && (
          <>
            <DetailItemContainer title="Duration">
              <Text textStyle="text-small" {...horizontalOverflowTextProps}>
                {formatDuration(duration)}
              </Text>
            </DetailItemContainer>
            <DetailItemContainer title="Start time">
              <Text textStyle="text-small" {...horizontalOverflowTextProps}>
                {formatDate(startTime, FRIENDLY_DATETIME_FORMAT)}
              </Text>
            </DetailItemContainer>
            <DetailItemContainer title="End time">
              <Text textStyle="text-small" {...horizontalOverflowTextProps}>
                {endTime ? formatDate(endTime, FRIENDLY_DATETIME_FORMAT) : "-"}
              </Text>
            </DetailItemContainer>
          </>
        )}
        <DetailItemContainer title="Result size">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {resultSize ? formatBytesShort(resultSize) : "-"}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer title="Rows returned">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {rowsReturned?.toLocaleString() ?? "-"}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer
          title={
            <Text
              textStyle="text-small"
              noOfLines={1}
              fontWeight="500"
              color={colors.foreground.secondary}
            >
              Throttled count
              <ThrottledCountTooltip marginLeft="1" marginBottom="1" />
            </Text>
          }
        >
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {throttledCount?.toLocaleString() ?? "-"}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer title="Database name">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {databaseName}
          </Text>
        </DetailItemContainer>
        <DetailItemContainer title="Search path">
          <Text textStyle="text-small" {...horizontalOverflowTextProps}>
            {searchPath}
          </Text>
        </DetailItemContainer>
      </Grid>
    </Box>
  );
};

const QueryHistoryDetail = () => {
  const params = useParams<{ id: string }>();
  const flags = useFlags();
  const toast = useToast();

  assert(params.id);

  const {
    hasPrivilege,
    isLoading: isPrivilegesLoading,
    isError: isPrivilegesError,
    isSuccess: isPrivilegesSuccess,
  } = usePrivileges();

  const queryClient = useQueryClient();
  const { mutate: cancelQuery, isSuccess: isCancelQuerySuccess } =
    useCancelQuery();

  const isAuthorized = isAuthorizedSelector(hasPrivilege);
  const shouldShowRedacted = shouldShowRedactedSelector(hasPrivilege);

  const isUnauthorized = isPrivilegesSuccess && !isAuthorized;
  const statementInfoParams = {
    executionId: params.id,
    isRedacted: shouldShowRedacted,
  };

  const {
    data: queryHistoryInfo,
    isError: isQueryHistoryDetailError,
    isLoading: isQueryHistoryDetailLoading,
    isPlaceholderData,
  } = useFetchQueryHistoryStatementInfo(statementInfoParams, {
    enabled: isPrivilegesSuccess && isAuthorized,
  });
  const isCancelButtonVisible =
    queryHistoryInfo?.info?.finishedStatus === "running" &&
    queryHistoryInfo?.info?.sessionId !== undefined &&
    !isCancelQuerySuccess;

  const onCancelQuery = () => {
    if (!queryHistoryInfo?.info?.sessionId) {
      return;
    }
    cancelQuery(
      {
        sessionId: queryHistoryInfo.info.sessionId,
      },
      {
        onSuccess: () => {
          toast({
            status: "success",
            description: "Query successfully cancelled",
          });

          // When a query is cancelled, it takes a few seconds for the activity log to properly update.
          // Thus we invalidate after a delay on success.
          setTimeout(() => {
            queryClient.invalidateQueries({
              queryKey:
                queryHistoryQueryKeys.statementInfo(statementInfoParams),
            });
          }, ACTIVITY_LOG_UPDATE_DELAY_MS);
        },
        onError: () => {
          toast({
            status: "error",
            description: "Unable to cancel query",
          });
        },
      },
    );
  };

  const isError = isPrivilegesError || isQueryHistoryDetailError;

  const isLoading = isPrivilegesLoading || isQueryHistoryDetailLoading;

  if (queryHistoryInfo?.shouldRedirect) {
    return <Navigate to=".." />;
  }

  return (
    <>
      <PageHeader variant="compact" sticky>
        <Breadcrumbs />
        <PageTabStrip tabData={[{ href: "", label: "Overview", end: true }]} />
      </PageHeader>
      {isError ? (
        <ErrorBox />
      ) : isUnauthorized ? (
        <UnauthorizedState />
      ) : isLoading ? (
        <Stack
          width="100%"
          height="100%"
          alignItems="center"
          justifyContent="center"
        >
          <Spinner data-testid="loading-spinner" />
        </Stack>
      ) : (
        <MainContentContainer>
          <VStack spacing="10" paddingBottom="6" width="100%">
            <VStack alignItems="flex-start" spacing="4" width="100%">
              <HStack justifyContent="space-between" width="100%">
                <Text textStyle="heading-md">Details</Text>

                {isCancelButtonVisible && (
                  <Button colorScheme="red" size="sm" onClick={onCancelQuery}>
                    Request cancellation
                  </Button>
                )}
              </HStack>
              {isPlaceholderData ? (
                <QueryHistoryDetailsCard
                  queryHistoryInfo={queryHistoryInfo?.initialPlaceholderData}
                  isCancelQuerySuccess={isCancelQuerySuccess}
                />
              ) : (
                <QueryHistoryDetailsCard
                  queryHistoryInfo={queryHistoryInfo?.info}
                  isCancelQuerySuccess={isCancelQuerySuccess}
                />
              )}
            </VStack>
            <VStack alignItems="flex-start" spacing="4" width="100%">
              <Text textStyle="heading-md">SQL Text</Text>
              <ExpandableCodeBlock
                text={
                  isPlaceholderData
                    ? (queryHistoryInfo?.initialPlaceholderData?.sql ?? "")
                    : (queryHistoryInfo?.info?.sql ?? "")
                }
                errorMessage={queryHistoryInfo?.info?.errorMessage ?? ""}
              />
            </VStack>
            {flags["query-history-statement-lifecycle-952"] && (
              <VStack alignItems="flex-start" spacing="4" width="100%">
                <HStack>
                  <Text textStyle="heading-md">Query Lifecycle</Text>
                </HStack>
                <QueryLifecycleCard
                  executionId={
                    queryHistoryInfo?.initialPlaceholderData?.executionId ??
                    queryHistoryInfo?.info?.executionId
                  }
                />
              </VStack>
            )}
          </VStack>
        </MainContentContainer>
      )}
    </>
  );
};

export default QueryHistoryDetail;
