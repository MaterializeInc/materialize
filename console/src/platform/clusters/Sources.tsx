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
import { useSourcesList } from "~/platform/sources/queries";
import { EmptyState, SourceTable } from "~/platform/sources/SourcesList";

import { useConnectorListSortOptions } from "../connectors/useConnectorListSortOptions";
import { ClusterParams } from "./ClusterRoutes";

const NAME_FILTER_QUERY_STRING_KEY = "sourceName";

const Sources = () => {
  const schemaObjectFilters = useSchemaObjectFilters(
    NAME_FILTER_QUERY_STRING_KEY,
  );
  const { databaseFilter, schemaFilter, nameFilter } = schemaObjectFilters;

  return (
    <MainContentContainer>
      <HStack mb="6" alignItems="center" justifyContent="space-between">
        <Text textStyle="heading-sm">Sources</Text>
        <HStack>
          <DatabaseFilter {...databaseFilter} />
          <SchemaFilter {...schemaFilter} />
          <SearchInput
            name="source"
            value={nameFilter.name}
            onChange={(e) => {
              nameFilter.setName(e.target.value);
            }}
          />
        </HStack>
      </HStack>
      <React.Suspense fallback={<LoadingContainer />}>
        <AppErrorBoundary message="An error occurred loading sources">
          <SourcesInner schemaObjectFilters={schemaObjectFilters} />
        </AppErrorBoundary>
      </React.Suspense>
    </MainContentContainer>
  );
};

const SourcesInner = ({
  schemaObjectFilters,
}: {
  schemaObjectFilters: SchemaObjectFilters;
}) => {
  const { clusterId } = useParams<ClusterParams>();

  const { databaseFilter, schemaFilter, nameFilter } = schemaObjectFilters;

  const { data: sources, refetch: refetchSources } = useSourcesList({
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
    clusterId,
  });
  const sortOptions = useConnectorListSortOptions();

  const isEmpty = sources && sources?.rows.length === 0;

  if (isEmpty) return <EmptyState />;

  return (
    <SourceTable
      sortOptions={sortOptions}
      sources={sources?.rows ?? []}
      refetchSources={refetchSources}
    />
  );
};

export default Sources;
