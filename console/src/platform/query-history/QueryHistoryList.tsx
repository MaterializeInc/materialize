// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Circle,
  Divider,
  HStack,
  Spinner,
  Stack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useMemo } from "react";
import { FormProvider, useFormContext } from "react-hook-form";

import { QueryHistoryListSchema } from "~/api/materialize/query-history/queryHistoryList";
import ErrorBox from "~/components/ErrorBox";
import TextLink from "~/components/TextLink";
import { usePrivileges } from "~/hooks/usePrivileges";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { useEnvironmentGate } from "~/store/environments";
import ActivityIcon from "~/svg/ActivityIcon";
import { AdminShieldIcon } from "~/svg/AdminIcon";
import { MaterializeTheme } from "~/theme";

import ClusterFilter from "./ClusterFilter";
import ColumnFilter from "./ColumnFilter";
import DateRangeInput from "./DateRangeInput";
import FilterMenu from "./FilterMenu";
import { useFetchQueryHistoryList } from "./queries";
import QueryHistoryTable from "./QueryHistoryTable";
import {
  formatSelectedDates,
  formatToURLParamObject,
  isAuthorizedSelector,
  shouldShowRedactedSelector,
} from "./queryHistoryUtils";
import SortFilter from "./SortFilter";
import useColumns, { ColumnKey } from "./useColumns";
import useQueryHistoryFormState from "./useQueryHistoryFormState";
import UserFilter from "./UserFilter";
import { ALL_USERS_OPTION } from "./utils";

type QueryHistoryListProps = {
  initialFilters: QueryHistoryListSchema;
  initialColumns: ColumnKey[];
  currentUserEmail?: string;
};

const EmptyState = () => {
  const { colors } = useTheme<MaterializeTheme>();

  const { watch } = useFormContext<QueryHistoryListSchema>();

  const userFilter = watch("user");

  const dateFilter = watch("dateRange");

  const formattedDateFilterStr = formatSelectedDates(
    dateFilter.map((str) => new Date(str)),
  );

  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <Circle p={2} bg={colors.background.secondary}>
          <ActivityIcon color={colors.foreground.secondary} />
        </Circle>
        <EmptyListHeaderContents
          title="No results found."
          helpText={
            <>
              There are no queries matching your filters.
              <Divider my="6" borderColor={colors.border.secondary} />
              User filter:{" "}
              {userFilter === ALL_USERS_OPTION.id
                ? ALL_USERS_OPTION.name
                : userFilter}
              <br />
              Date range: {formattedDateFilterStr}
            </>
          }
        />
      </EmptyListHeader>
    </EmptyListWrapper>
  );
};

export const UnauthorizedState = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <Circle p={2} bg={colors.background.secondary}>
          <AdminShieldIcon color={colors.accent.brightPurple} w="10" h="10" />
        </Circle>
        <EmptyListHeaderContents
          title="You do not have permission to use this feature"
          helpText={
            <>
              Reach out to your organization admin to be granted the{" "}
              <Text as="span" textStyle="monospace">
                mz_monitor
              </Text>{" "}
              or{" "}
              <Text as="span" textStyle="monospace">
                mz_monitor_redacted
              </Text>{" "}
              roles for this feature.
            </>
          }
        />
        <TextLink
          as="a"
          href={`${docUrls["/docs/security/cloud/access-control/manage-roles/"]}#builtin-roles`}
          target="_blank"
        >
          View our documentation
        </TextLink>
      </EmptyListHeader>
    </EmptyListWrapper>
  );
};

export const QueryHistoryList = ({
  initialFilters,
  initialColumns,
  currentUserEmail,
}: QueryHistoryListProps) => {
  const { draftListFiltersForm, listFilters, onSubmit } =
    useQueryHistoryFormState({ initialFilters, currentUserEmail });

  const { selectedColumnItems, onColumnChange, selectedColumnFilterItems } =
    useColumns({ initialColumns });

  const {
    hasPrivilege,
    isLoading: isPrivilegesLoading,
    isError: isPrivilegesError,
    isSuccess: isPrivilegesSuccess,
  } = usePrivileges();

  const urlParamObject = useMemo(
    () =>
      formatToURLParamObject({
        listFilters,
        columns: selectedColumnItems.map(({ key }) => key),
      }),
    [listFilters, selectedColumnItems],
  );

  const submitForm = draftListFiltersForm.handleSubmit(onSubmit);
  const isAuthorized = isAuthorizedSelector(hasPrivilege);

  const shouldShowRedacted = shouldShowRedactedSelector(hasPrivilege);

  const isV0_132_0 = useEnvironmentGate("0.132.0") ?? true;

  const {
    isError: isQueryHistoryListError,
    data: queryHistoryListData,
    isLoading: isQueryHistoryListLoading,
  } = useFetchQueryHistoryList(
    {
      filters: listFilters,
      isRedacted: shouldShowRedacted,
      isV0_132_0,
    },
    {
      enabled: isPrivilegesSuccess && isAuthorized,
    },
  );

  useSyncObjectToSearchParams(urlParamObject);

  const isError = isPrivilegesError || isQueryHistoryListError;

  const isLoading = isPrivilegesLoading || isQueryHistoryListLoading;
  const isEmpty = queryHistoryListData?.rows.length === 0;

  const isUnauthorized = isPrivilegesSuccess && !isAuthorized;

  return (
    <FormProvider {...draftListFiltersForm}>
      <MainContentContainer padding="0" marginTop="0">
        <PageHeader boxProps={{ paddingTop: "6", paddingX: "10" }}>
          <PageHeading>Query History</PageHeading>
        </PageHeader>
        <VStack
          alignItems="stretch"
          minHeight="0"
          minWidth="0"
          spacing="6"
          flexGrow="1"
        >
          <HStack marginX="10" justifyContent="space-between">
            <HStack>
              <UserFilter
                submitForm={submitForm}
                variant={isEmpty ? "focused" : "default"}
              />
              <ClusterFilter submitForm={submitForm} />
              <DateRangeInput
                onSubmit={onSubmit}
                toggleButtonProps={isEmpty ? { variant: "focused" } : undefined}
              />
              <FilterMenu onSubmit={onSubmit} />
            </HStack>
            <HStack>
              <SortFilter submitForm={submitForm} />
              <ColumnFilter
                selectedColumnFilterItems={selectedColumnFilterItems}
                onColumnChange={onColumnChange}
              />
            </HStack>
          </HStack>
          <Stack flexGrow="1" paddingLeft="10" minHeight="0">
            {isError ? (
              <ErrorBox message="An error occurred loading the query history list" />
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
            ) : isEmpty ? (
              <EmptyState />
            ) : (
              <QueryHistoryTable
                rows={queryHistoryListData?.rows ?? []}
                columns={selectedColumnItems}
              />
            )}
          </Stack>
        </VStack>
      </MainContentContainer>
    </FormProvider>
  );
};

export default QueryHistoryList;
