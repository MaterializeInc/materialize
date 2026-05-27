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
import { Sink } from "~/api/materialize/sink/sinkList";
import { ConnectorStatus } from "~/api/materialize/types";
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
import { ConnectorStatusPill } from "~/components/StatusPill";
import {
  UseConnectorHealthFilter,
  useConnectorHealthFilter,
} from "~/hooks/useConnectorHealthFilter";
import { SinksIcon } from "~/icons";
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
import {
  ConnectorListSortOptionsState,
  useConnectorListSortOptions,
} from "~/platform/connectors/useConnectorListSortOptions";
import { connectorHealthStatus } from "~/platform/connectors/utils";
import { useBuildSinkPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { kebabToTitleCase } from "~/util";

import { sortConnectors } from "../connectors/sort";
import { SINKS_FETCH_ERROR_MESSAGE } from "./constants";
import { useSinkList } from "./queries";

const SINK_CREATE_SQL = `CREATE SINK <sink_name>
  FROM <view_name>
  INTO <item_name>
  FORMAT <format>
  ENVELOPE <envelope>;`;

const NAME_FILTER_QUERY_STRING_KEY = "sinkName";

export const EmptyFilteredState = (props: { clearFilters: () => void }) => {
  return (
    <EmptyListWrapper>
      <Text textStyle="heading-sm">No sink match the selected filter</Text>
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
          <Box mt="1px">
            <SinksIcon />
          </Box>
        </IconBox>

        <EmptyListHeaderContents
          title="No available sinks"
          helpText="Create a sink to begin streaming data out of Materialize."
        />
      </EmptyListHeader>
      <SampleCodeBoxWrapper docsUrl={docUrls["/docs/sql/create-sink/"]}>
        <CodeBlock
          title="Create a sink"
          contents={SINK_CREATE_SQL}
          lineNumbers
        />
      </SampleCodeBoxWrapper>
    </EmptyListWrapper>
  );
};

const SinksListPage = () => {
  const { databaseFilter, schemaFilter, nameFilter } = useSchemaObjectFilters(
    NAME_FILTER_QUERY_STRING_KEY,
  );
  const healthFilter = useConnectorHealthFilter();
  const sortOptions = useConnectorListSortOptions();

  return (
    <MainContentContainer>
      <PageHeader>
        <PageHeading>Sinks</PageHeading>
        <HStack spacing="4">
          <DatabaseFilter {...databaseFilter} />
          <SchemaFilter {...schemaFilter} />
          <ConnectorHealthFilter {...healthFilter} />
          <SearchInput
            name="sink"
            value={nameFilter.name}
            onChange={(e) => {
              nameFilter.setName(e.target.value);
            }}
          />
        </HStack>
      </PageHeader>
      <AppErrorBoundary message={SINKS_FETCH_ERROR_MESSAGE}>
        <React.Suspense fallback={<LoadingContainer />}>
          <SinksListContent
            databaseFilter={databaseFilter}
            schemaFilter={schemaFilter}
            nameFilter={nameFilter}
            healthFilter={healthFilter}
            sortOptions={sortOptions}
          />
        </React.Suspense>
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

interface SinkTableProps {
  sinks: Sink[];
  refetchSinks: () => void;
  sortOptions: ConnectorListSortOptionsState;
}

type SinkListContentProps = {
  databaseFilter: DatabaseFilterState;
  nameFilter: NameFilterState;
  schemaFilter: SchemaFilterState;
  healthFilter: UseConnectorHealthFilter;
  sortOptions: ConnectorListSortOptionsState;
};

const SinksListContent = ({
  databaseFilter,
  schemaFilter,
  nameFilter,
  healthFilter,
  sortOptions,
}: SinkListContentProps) => {
  const { data: sinks, refetch } = useSinkList({
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
  });
  const filtered = React.useMemo(() => {
    if (healthFilter.selected === "all") return sinks.rows;

    return sinks.rows.filter((sink) => {
      const status = connectorHealthStatus(sink.status as ConnectorStatus);
      return status === (healthFilter.selected ?? "healthy");
    });
  }, [healthFilter.selected, sinks.rows]);

  const sorted = React.useMemo(
    () =>
      sortConnectors(
        filtered,
        sortOptions.selectedSortColumn,
        sortOptions.selectedSortOrder,
      ),
    [filtered, sortOptions.selectedSortColumn, sortOptions.selectedSortOrder],
  );

  if (sinks.rows.length === 0) {
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
    <SinkTable
      sinks={sorted}
      refetchSinks={refetch}
      sortOptions={sortOptions}
    />
  );
};

export const SinkTable = (props: SinkTableProps) => {
  const sinkPath = useBuildSinkPath();
  const navigate = useNavigate();
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Table variant="linkable" data-testid="sink-table" borderRadius="xl">
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
        {props.sinks.map((s) => (
          <Tr key={s.id} onClick={() => navigate(sinkPath(s))} cursor="pointer">
            <Td py="2" {...truncateMaxWidth}>
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
            <Td>{kebabToTitleCase(s.type)}</Td>
            <Td>
              <OverflowMenu
                items={[
                  {
                    visible: s.isOwner,
                    render: () => (
                      <DeleteObjectMenuItem
                        key="delete-object"
                        selectedObject={s}
                        onSuccessAction={props.refetchSinks}
                        objectType="SINK"
                      />
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

export default SinksListPage;
