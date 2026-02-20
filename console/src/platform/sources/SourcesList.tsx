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
  HStack,
  MenuItem,
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
import { Link, useNavigate } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import { createNamespace } from "~/api/materialize";
import { Source } from "~/api/materialize/source/sourceList";
import { ConnectorStatus } from "~/api/materialize/types";
import useCanCreateObjects from "~/api/materialize/useCanCreateObjects";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { ConnectorHealthFilter } from "~/components/ConnectorHealthFilter";
import { CodeBlock } from "~/components/copyableComponents";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import DatabaseFilter from "~/components/SchemaObjectFilter/DatabaseFilter";
import SchemaFilter from "~/components/SchemaObjectFilter/SchemaFilter";
import {
  DatabaseFilterState,
  NameFilterState,
  SchemaFilterState,
  useSchemaObjectFilters,
} from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import SearchInput from "~/components/SearchInput";
import { SortToggleButton } from "~/components/SortToggleButton";
import { SourceTypeFilter } from "~/components/SourceTypeFilter";
import { ConnectorStatusPill } from "~/components/StatusPill";
import {
  UseConnectorHealthFilter,
  useConnectorHealthFilter,
} from "~/hooks/useConnectorHealthFilter";
import {
  SourceTypeFilterState,
  useSourceTypeFilter,
} from "~/hooks/useSourceTypeFilter";
import { SourcesIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { sortConnectors } from "~/platform/connectors/sort";
import { connectorHealthStatus } from "~/platform/connectors/utils";
import { useBuildSourcePath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { prettyConnectorType } from "~/util";

import {
  ConnectorListSortOptionsState,
  useConnectorListSortOptions,
} from "../connectors/useConnectorListSortOptions";
import { SOURCES_FETCH_ERROR_MESSAGE } from "./constants";
import { useSourcesList } from "./queries";

const NAME_FILTER_QUERY_STRING_KEY = "sourceName";

export const EmptyFilteredState = (props: { clearFilters: () => void }) => {
  return (
    <EmptyListWrapper>
      <Text textStyle="heading-sm">No sources match the selected filter</Text>
      <Button variant="primary" onClick={props.clearFilters}>
        Clear filters
      </Button>
    </EmptyListWrapper>
  );
};

export const EmptyState = () => {
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <IconBox type="Empty">
          <Box mt="-1px">
            <SourcesIcon />
          </Box>
        </IconBox>
        <EmptyListHeaderContents
          title="No available sources"
          helpText="Connect a source to begin streaming data to Materialize."
        />
      </EmptyListHeader>
      <SampleCodeBoxWrapper docsUrl={docUrls["/docs/sql/create-source/"]}>
        <CodeBlock
          title="Create a source"
          contents={`CREATE CONNECTION <connection_name>
  TO <connection_type> (<options>);

CREATE SOURCE <source_name>
  FROM <source>
  FORMAT <format>;`}
          lineNumbers
        />
      </SampleCodeBoxWrapper>
    </EmptyListWrapper>
  );
};

const SourcesListPage = () => {
  const { track } = useSegment();

  const { results: canCreate } = useCanCreateObjects();

  const { databaseFilter, schemaFilter, nameFilter } = useSchemaObjectFilters(
    NAME_FILTER_QUERY_STRING_KEY,
  );
  const healthFilter = useConnectorHealthFilter();
  const typeFilter = useSourceTypeFilter();
  const sortOptions = useConnectorListSortOptions();

  const buttonProps = {
    variant: "primary",
    size: "sm",
    onClick: () => {
      track("New Source Clicked", { source: "Source list" });
    },
  };

  return (
    <MainContentContainer>
      <PageHeader boxProps={{ mb: "4" }}>
        <PageHeading>Sources</PageHeading>
        <HStack spacing="4">
          <SearchInput
            name="source"
            value={nameFilter.name}
            onChange={(e) => {
              nameFilter.setName(e.target.value);
            }}
          />
          {canCreate && (
            <Button {...buttonProps} as={Link} to="new">
              New Source
            </Button>
          )}
        </HStack>
      </PageHeader>
      <HStack width="100%" justifyContent="space-between" mb="4">
        <HStack>
          <SourceTypeFilter {...typeFilter} />
          <DatabaseFilter {...databaseFilter} />
          <SchemaFilter {...schemaFilter} />
          <ConnectorHealthFilter {...healthFilter} />
        </HStack>
      </HStack>
      <AppErrorBoundary message={SOURCES_FETCH_ERROR_MESSAGE}>
        <React.Suspense fallback={<LoadingContainer />}>
          <SourcesListContent
            databaseFilter={databaseFilter}
            schemaFilter={schemaFilter}
            nameFilter={nameFilter}
            typeFilter={typeFilter}
            healthFilter={healthFilter}
            sortOptions={sortOptions}
          />
        </React.Suspense>
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

interface SourceListContentProps {
  databaseFilter: DatabaseFilterState;
  nameFilter: NameFilterState;
  schemaFilter: SchemaFilterState;
  typeFilter: SourceTypeFilterState;
  healthFilter: UseConnectorHealthFilter;
  sortOptions: ConnectorListSortOptionsState;
}

const SourcesListContent = ({
  databaseFilter,
  schemaFilter,
  nameFilter,
  typeFilter,
  healthFilter,
  sortOptions,
}: SourceListContentProps) => {
  const { data: sources, refetch } = useSourcesList({
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
    type: typeFilter.selectedType?.id,
  });

  const filtered = React.useMemo(() => {
    if (healthFilter.selected === "all") return sources.rows;

    return sources.rows.filter((source) => {
      const status = connectorHealthStatus(source.status as ConnectorStatus);
      return status === (healthFilter.selected ?? "healthy");
    });
  }, [healthFilter, sources.rows]);

  const sorted = React.useMemo(
    () =>
      sortConnectors(
        filtered,
        sortOptions.selectedSortColumn,
        sortOptions.selectedSortOrder,
      ),
    [filtered, sortOptions.selectedSortColumn, sortOptions.selectedSortOrder],
  );

  if (sources.rows.length === 0) {
    return <EmptyState />;
  }
  if (filtered.length === 0) {
    return (
      <EmptyFilteredState
        clearFilters={() => healthFilter.setSelected("all")}
      />
    );
  }
  return (
    <SourceTable
      sources={sorted}
      refetchSources={refetch}
      sortOptions={sortOptions}
    />
  );
};

interface SourceTableProps {
  sources: Source[];
  refetchSources: () => void;
  sortOptions: ConnectorListSortOptionsState;
}

export const SourceTable = (props: SourceTableProps) => {
  const sourcePath = useBuildSourcePath();
  const navigate = useNavigate();
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Table variant="linkable" data-testid="source-table" borderRadius="xl">
      <Thead>
        <Tr>
          <Th>
            <SortToggleButton
              label="Name"
              column="name"
              {...props.sortOptions}
            />
          </Th>
          <Th width="25%">
            <SortToggleButton
              label="Status"
              column="status"
              {...props.sortOptions}
            />
          </Th>
          <Th width="25%">
            <SortToggleButton
              label="Type"
              column="type"
              {...props.sortOptions}
            />
          </Th>
          <Th width={OVERFLOW_BUTTON_WIDTH}></Th>
        </Tr>
      </Thead>
      <Tbody>
        {props.sources.map((s) => (
          <Tr
            key={s.id}
            onClick={() => navigate(sourcePath(s))}
            cursor="pointer"
          >
            <Td {...truncateMaxWidth} py="2">
              <Text
                textStyle="text-small"
                fontWeight="500"
                noOfLines={1}
                color={colors.foreground.secondary}
              >
                {createNamespace(s.databaseName, s.schemaName)}
              </Text>
              <Text textStyle="text-ui-med" noOfLines={1}>
                {s.name}
              </Text>
            </Td>
            <Td>{s.status ? <ConnectorStatusPill connector={s} /> : "-"}</Td>
            <Td {...truncateMaxWidth}>
              <Text textStyle="text-ui-reg" noOfLines={1}>
                {prettyConnectorType(s.type)}
              </Text>
            </Td>
            <Td>
              <OverflowMenu
                items={[
                  {
                    visible: s.isOwner,
                    render: () => (
                      <DeleteObjectMenuItem
                        key="delete-object"
                        selectedObject={s}
                        onSuccessAction={props.refetchSources}
                        objectType="SOURCE"
                      />
                    ),
                  },
                  {
                    visible: true,
                    render: () => (
                      <MenuItem
                        key="dependency-graph"
                        to={`${sourcePath(s)}/workflow`}
                        as={Link}
                        onClick={(e) => e.stopPropagation()}
                        textStyle="text-ui-med"
                      >
                        View workflow
                      </MenuItem>
                    ),
                  },
                ]}
              />
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
};

export default SourcesListPage;
