// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text } from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import DatabaseFilter from "~/components/SchemaObjectFilter/DatabaseFilter";
import SchemaFilter from "~/components/SchemaObjectFilter/SchemaFilter";
import {
  SchemaObjectFilters,
  useSchemaObjectFilters,
} from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import SearchInput from "~/components/SearchInput";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useConnectorListSortOptions } from "~/platform/connectors/useConnectorListSortOptions";

import { useSinkList } from "../sinks/queries";
import { EmptyState, SinkTable } from "../sinks/SinksList";
import { ClusterParams } from "./ClusterRoutes";

const NAME_FILTER_QUERY_STRING_KEY = "sourceName";

const Sinks = () => {
  const schemaObjectFilters = useSchemaObjectFilters(
    NAME_FILTER_QUERY_STRING_KEY,
  );
  const { databaseFilter, schemaFilter, nameFilter } = schemaObjectFilters;

  return (
    <MainContentContainer>
      <HStack mb="6" alignItems="center" justifyContent="space-between">
        <Text textStyle="heading-sm">Sinks</Text>
        <HStack>
          <DatabaseFilter {...databaseFilter} />
          <SchemaFilter {...schemaFilter} />
          <SearchInput
            name="sink"
            value={nameFilter.name}
            onChange={(e) => {
              nameFilter.setName(e.target.value);
            }}
          />
        </HStack>
      </HStack>
      <React.Suspense fallback={<LoadingContainer />}>
        <AppErrorBoundary message="An error occurred loading sinks">
          <SinksContent schemaObjectFilters={schemaObjectFilters} />
        </AppErrorBoundary>
      </React.Suspense>
    </MainContentContainer>
  );
};

const SinksContent = ({
  schemaObjectFilters,
}: {
  schemaObjectFilters: SchemaObjectFilters;
}) => {
  const { clusterId } = useParams<ClusterParams>();
  const sortOptions = useConnectorListSortOptions();

  const { databaseFilter, schemaFilter, nameFilter } = schemaObjectFilters;

  const { data, refetch: refetchSinks } = useSinkList({
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
    clusterId,
  });

  const isEmpty = data.rows.length === 0;

  if (isEmpty) return <EmptyState />;

  return (
    <SinkTable
      sinks={data.rows}
      refetchSinks={refetchSinks}
      sortOptions={sortOptions}
    />
  );
};

export default Sinks;
