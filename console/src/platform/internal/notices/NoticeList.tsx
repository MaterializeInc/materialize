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
  Flex,
  HStack,
  Spinner,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { ListPageNotice } from "~/api/materialize/notice/fetchNoticesList";
import ErrorBox from "~/components/ErrorBox";
import DatabaseFilter from "~/components/SchemaObjectFilter/DatabaseFilter";
import SchemaFilter from "~/components/SchemaObjectFilter/SchemaFilter";
import {
  DatabaseFilterState,
  NameFilterState,
  SchemaFilterState,
} from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import SearchInput from "~/components/SearchInput";
import TextLink from "~/components/TextLink";
import { InfoIcon } from "~/icons";
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
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { useNoticesListPage } from "./queries";

const EmptyState = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <Circle p={2} bg={colors.background.secondary}>
          <InfoIcon />
        </Circle>
        <EmptyListHeaderContents
          title="No notices found"
          helpText="Relax the filter or refresh this page once you have created more catalog objects."
        />
        <Text
          fontSize="xs"
          textAlign="center"
          color={colors.foreground.secondary}
        >
          Need help?{" "}
          <TextLink href={docUrls["/docs/"]} target="_blank">
            View the documentation.
          </TextLink>
        </Text>
      </EmptyListHeader>
    </EmptyListWrapper>
  );
};

export interface NoticesListProps {
  databaseFilter: DatabaseFilterState;
  nameFilter: NameFilterState;
  schemaFilter: SchemaFilterState;
}

const Notices = ({
  databaseFilter,
  nameFilter,
  schemaFilter,
}: NoticesListProps) => {
  const { data, isLoading, isError } = useNoticesListPage({
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
  });

  const notices = data?.rows;
  const isEmpty = notices && notices.length === 0;

  return (
    <MainContentContainer>
      <PageHeader>
        <PageHeading>Notices</PageHeading>
        <HStack mb="6" alignItems="center" justifyContent="space-between">
          <HStack>
            <DatabaseFilter {...databaseFilter} />
            <SchemaFilter {...schemaFilter} />
            <SearchInput
              name="notice"
              value={nameFilter.name}
              onChange={(e) => {
                nameFilter.setName(e.target.value);
              }}
            />
          </HStack>
        </HStack>
      </PageHeader>
      {isError ? (
        <ErrorBox message="An error occurred loading notices" />
      ) : isLoading ? (
        <Flex h="100%" w="100%" alignItems="center" justifyContent="center">
          <Spinner data-testid="loading-spinner" />
        </Flex>
      ) : isEmpty ? (
        <EmptyState />
      ) : (
        <NoticeTable notices={notices ?? []} />
      )}
    </MainContentContainer>
  );
};

interface NoticeTableProps {
  notices: ListPageNotice[];
}

const NoticeTable = ({ notices }: NoticeTableProps) => {
  const navigate = useNavigate();
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Table variant="linkable" data-testid="notice-table" borderRadius="xl">
      <Thead>
        <Tr>
          <Th>Type</Th>
          <Th>
            <Text noOfLines={1}>Object name</Text>
          </Th>
          <Th>
            <Text noOfLines={1}>Last updated</Text>
          </Th>
        </Tr>
      </Thead>
      <Tbody>
        {notices.map((notice) => {
          return (
            <Tr
              key={notice.key}
              onClick={() => navigate(`./${encodeURIComponent(notice.key)}`)}
              cursor="pointer"
              textColor="default"
              aria-label={notice.noticeType}
            >
              <Td {...truncateMaxWidth} py="3">
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {notice.noticeType}
                </Text>
              </Td>
              <Td {...truncateMaxWidth} py="3">
                <Text
                  textStyle="text-small"
                  fontWeight="500"
                  mb="2px"
                  noOfLines={1}
                  color={colors.foreground.secondary}
                >
                  {createNamespace(notice.databaseName, notice.schemaName)}
                </Text>
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {notice.objectName}
                </Text>
              </Td>
              <Td width="25%">
                <Text>
                  {formatDate(
                    notice.createdAt,
                    FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
                  )}
                </Text>
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
};

export default Notices;
