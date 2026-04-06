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
  Spinner,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useMemo } from "react";
import { useNavigate, useParams } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import { MAINTAINED_OBJECT_TYPES } from "~/api/materialize/maintained-objects/maintainedObjectsList";
import SearchInput from "~/components/SearchInput";
import SimpleSelect from "~/components/SimpleSelect";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
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
import { SideDrawer } from "~/components/SideDrawer";
import { TablePagination } from "~/platform/shell/SqlSelectTable";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { formatBytesShort, formatIntervalShort } from "~/utils/format";
import { useQueryStringState } from "~/useQueryString";

import { ObjectDetailPanel } from "./ObjectDetailPanel";
import {
  useAllObjectSizes,
  useClusterUtilization,
  useMaintainedObjectsList,
} from "./queries";
import { MaintainedObjectListRow } from "./types";

const PAGE_SIZE = 15;

const FRESHNESS_TIME_PERIOD_OPTIONS: Record<string, string> = {
  "1": "Past 1 minute",
  "5": "Past 5 minutes",
  "15": "Past 15 minutes",
  "30": "Past 30 minutes",
  "60": "Past 1 hour",
  "180": "Past 3 hours",
  "360": "Past 6 hours",
  "1440": "Past 24 hours",
};

const MaintainedObjects = () => {
  const { colors } = useTheme<MaterializeTheme>();

  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "maintained-objects-time-period",
    defaultValue: "15",
    timePeriodOptions: FRESHNESS_TIME_PERIOD_OPTIONS,
  });

  const { data: objects, isLoading: objectsLoading } =
    useMaintainedObjectsList({ lookbackMinutes: timePeriodMinutes });
  const { data: utilization } = useClusterUtilization();
  const { data: objectSizes } = useAllObjectSizes();

  const { objectId } = useParams<{ objectId: string }>();
  const navigate = useNavigate();

  const [nameFilter, setNameFilter] = useQueryStringState("name");
  const [typeFilter, setTypeFilter] = useQueryStringState("type");
  const [clusterFilter, setClusterFilter] = useQueryStringState("cluster");
  const [page, setPage] = React.useState(0);

  const clusters = useMemo(() => {
    if (!objects) return [];
    const clusterNames = new Set<string>();
    for (const obj of objects) {
      if (obj.clusterName) clusterNames.add(obj.clusterName);
    }
    return [...clusterNames].sort();
  }, [objects]);

  const filtered = useMemo(() => {
    if (!objects) return [];
    return objects.filter((obj) => {
      if (
        nameFilter &&
        !obj.name.toLowerCase().includes(nameFilter.toLowerCase())
      ) {
        return false;
      }
      if (typeFilter && obj.objectType !== typeFilter) {
        return false;
      }
      if (clusterFilter && obj.clusterName !== clusterFilter) {
        return false;
      }
      return true;
    });
  }, [objects, nameFilter, typeFilter, clusterFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const currentPage = Math.min(page, totalPages - 1);
  const paginatedRows = filtered.slice(
    currentPage * PAGE_SIZE,
    (currentPage + 1) * PAGE_SIZE,
  );

  React.useEffect(() => {
    setPage(0);
  }, [nameFilter, typeFilter, clusterFilter]);

  const clearFilters = () => {
    setNameFilter(undefined);
    setTypeFilter(undefined);
    setClusterFilter(undefined);
  };

  // Store the clicked object directly so we don't depend on the list's
  // polling cycle for the drawer's data. This prevents re-renders when
  // the list query refetches and creates new object references.
  const [selectedObject, setSelectedObject] =
    React.useState<MaintainedObjectListRow | null>(null);

  // Breadcrumb trail for drill-down navigation
  const [breadcrumbs, setBreadcrumbs] = React.useState<
    { id: string; name: string }[]
  >([]);

  React.useEffect(() => {
    if (objectId && objects) {
      // Only set if we don't already have one, or the ID changed
      setSelectedObject((prev) => {
        if (prev?.id === objectId) return prev;
        return objects.find((obj) => obj.id === objectId) ?? prev;
      });
    } else if (!objectId) {
      setSelectedObject(null);
      setBreadcrumbs([]);
    }
  }, [objectId, objects]);

  const handleRowClick = (obj: MaintainedObjectListRow) => {
    setSelectedObject(obj);
    setBreadcrumbs([]);
    navigate(obj.id);
  };

  const handleDrillDown = (targetId: string) => {
    // Add current object to breadcrumbs before navigating
    if (selectedObject) {
      setBreadcrumbs((prev) => {
        // Don't add duplicates if going back and forth
        if (prev.length > 0 && prev[prev.length - 1].id === selectedObject.id)
          return prev;
        return [...prev, { id: selectedObject.id, name: selectedObject.name }];
      });
    }
    navigate(`../${targetId}`, { relative: "path" });
  };

  const handleBreadcrumbClick = (targetId: string, index: number) => {
    // Trim breadcrumbs to the clicked position
    setBreadcrumbs((prev) => prev.slice(0, index));
    navigate(`../${targetId}`, { relative: "path" });
  };

  if (objectsLoading) {
    return (
      <MainContentContainer>
        <PageHeader boxProps={{ mb: "4" }}>
          <PageHeading>Maintained Objects</PageHeading>
        </PageHeader>
        <Spinner data-testid="loading-spinner" />
      </MainContentContainer>
    );
  }

  return (
    <>
    <MainContentContainer>
      <PageHeader boxProps={{ mb: "4" }}>
        <PageHeading>Maintained Objects</PageHeading>
        <HStack spacing="4">
          <SearchInput
            name="objectName"
            value={nameFilter ?? ""}
            onChange={(e) => setNameFilter(e.target.value || undefined)}
          />
        </HStack>
      </PageHeader>

      <HStack width="100%" justifyContent="space-between" mb="4">
        <HStack spacing="3">
          <SimpleSelect
            placeholder="All types"
            value={typeFilter ?? ""}
            onChange={(e) => setTypeFilter(e.target.value || undefined)}
          >
            {MAINTAINED_OBJECT_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </SimpleSelect>

          <SimpleSelect
            placeholder="All clusters"
            value={clusterFilter ?? ""}
            onChange={(e) => setClusterFilter(e.target.value || undefined)}
          >
            {clusters.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </SimpleSelect>
        </HStack>

        <HStack spacing="3">
          <TimePeriodSelect
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={setTimePeriodMinutes}
            options={FRESHNESS_TIME_PERIOD_OPTIONS}
          />
          <Text
            textStyle="text-ui-reg"
            color={colors.foreground.secondary}
            whiteSpace="nowrap"
          >
            Showing {filtered.length} of {objects?.length ?? 0} objects
          </Text>
        </HStack>
      </HStack>

      {filtered.length === 0 ? (
        <EmptyListWrapper>
          <EmptyListHeader>
            <EmptyListHeaderContents
              title="No objects match the selected filters"
              helpText="Try adjusting your filters or time period."
            />
          </EmptyListHeader>
          <Button variant="primary" onClick={clearFilters}>
            Clear filters
          </Button>
        </EmptyListWrapper>
      ) : (
        <VStack alignItems="stretch" width="100%" spacing="4">
          <MaintainedObjectsTable
            rows={paginatedRows}
            utilization={utilization}
            objectSizes={objectSizes}
            onRowClick={handleRowClick}
          />

          <TablePagination
            totalPages={totalPages}
            totalNumRows={filtered.length}
            onNextPage={() =>
              setPage(Math.min(totalPages - 1, currentPage + 1))
            }
            onPrevPage={() => setPage(Math.max(0, currentPage - 1))}
            pageSize={PAGE_SIZE}
            currentPage={currentPage}
            startIndex={currentPage * PAGE_SIZE}
            endIndex={Math.min(
              (currentPage + 1) * PAGE_SIZE,
              filtered.length,
            )}
            prevEnabled={currentPage > 0}
            nextEnabled={currentPage < totalPages - 1}
            isFollowing={null}
            onToggleFollow={() => {}}
          />
        </VStack>
      )}
    </MainContentContainer>

      <SideDrawer
        isOpen={!!objectId}
        onClose={() => navigate("..", { relative: "path" })}
        title={selectedObject?.name}
        width="66%"
        trapFocus={false}
      >
        {selectedObject && (
          <ObjectDetailPanel
            object={selectedObject}
            breadcrumbs={breadcrumbs}
            onObjectClick={handleDrillDown}
            onBreadcrumbClick={handleBreadcrumbClick}
          />
        )}
      </SideDrawer>
    </>
  );
};

interface MaintainedObjectsTableProps {
  rows: ReturnType<typeof useMaintainedObjectsList>["data"];
  utilization: ReturnType<typeof useClusterUtilization>["data"];
  objectSizes: ReturnType<typeof useAllObjectSizes>["data"];
  onRowClick: (obj: MaintainedObjectListRow) => void;
}

const MaintainedObjectsTable = ({
  rows,
  utilization,
  objectSizes,
  onRowClick,
}: MaintainedObjectsTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!rows) return null;

  return (
    <Table
      variant="linkable"
      data-testid="maintained-objects-table"
      borderRadius="xl"
    >
      <Thead>
        <Tr>
          <Th>Object Name</Th>
          <Th>Object Type</Th>
          <Th>Memory Utilization</Th>
          <Th>Freshness (pMAX)</Th>
          <Th>Cluster</Th>
        </Tr>
      </Thead>
      <Tbody>
        {rows.map((obj) => {
          const clusterUtil = obj.clusterId
            ? utilization?.get(obj.clusterId)
            : undefined;
          const objSize = objectSizes?.get(obj.id);

          return (
            <Tr
              key={obj.id}
              cursor="pointer"
              _hover={{ bg: colors.background.secondary }}
              onClick={() => onRowClick(obj)}
            >
              <Td {...truncateMaxWidth} py="2">
                <Text
                  textStyle="text-small"
                  fontWeight="500"
                  noOfLines={1}
                  color={colors.foreground.secondary}
                >
                  {createNamespace(obj.databaseName, obj.schemaName)}
                </Text>
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {obj.name}
                </Text>
              </Td>
              <Td>
                <Text textStyle="text-ui-reg">{obj.objectType}</Text>
              </Td>
              <Td>
                <PercentCell value={objSize?.heapPercent} />
              </Td>
              <Td>
                <LagCell lag={obj.lag} lagMs={obj.lagMs} />
              </Td>
              <Td>
                <Text textStyle="text-ui-reg">
                  {obj.clusterName ?? "—"}
                </Text>
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
};

export interface LagCellProps {
  lag: unknown;
  lagMs: number | null;
}

const LagCell = ({ lag, lagMs }: LagCellProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!lag || lagMs === null) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  const outdatedMs = OUTDATED_THRESHOLD_SECONDS * 1_000;
  const warningMs = outdatedMs / 2;
  const isOutdated = lagMs >= outdatedMs;
  const isWarning = lagMs >= warningMs;

  return (
    <Text
      textStyle="text-ui-med"
      color={
        isOutdated
          ? colors.accent.red
          : isWarning
            ? colors.accent.orange
            : colors.foreground.primary
      }
    >
      {formatIntervalShort(lag as Parameters<typeof formatIntervalShort>[0])}
    </Text>
  );
};

export interface PercentCellProps {
  value: number | null | undefined;
}

const getPercentColor = (
  percent: number,
  colors: MaterializeTheme["colors"],
) => {
  if (percent > 90) return colors.accent.red;
  if (percent > 70) return colors.accent.orange;
  return colors.accent.green;
};

const PercentCell = ({ value }: PercentCellProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (value === null || value === undefined) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  const displayValue = value < 1 && value > 0
    ? value < 0.1 ? "<0.1" : value.toFixed(1)
    : Math.round(value).toString();
  const barWidth = Math.max(value, value > 0 ? 2 : 0);
  const barColor = getPercentColor(Math.round(value), colors);

  return (
    <HStack spacing="2" minWidth="80px">
      <Text textStyle="text-ui-med" whiteSpace="nowrap" minWidth="36px">
        {displayValue}%
      </Text>
      <Box
        width="48px"
        height="8px"
        borderRadius="full"
        bg={colors.background.secondary}
        overflow="hidden"
        flexShrink={0}
      >
        <Box
          height="100%"
          width={`${Math.min(barWidth, 100)}%`}
          minWidth={value > 0 ? "4px" : "0px"}
          borderRadius="full"
          bg={barColor}
          transition="width 0.3s ease"
        />
      </Box>
    </HStack>
  );
};


const ObjectMemoryCell = ({
  sizeBytes,
  heapPercent,
}: {
  sizeBytes: string | undefined;
  heapPercent: number | null | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!sizeBytes) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  const bytes = BigInt(sizeBytes);
  const pct = heapPercent != null ? heapPercent : null;

  return (
    <VStack align="start" spacing={0}>
      <Text textStyle="text-ui-med">
        {formatBytesShort(bytes)}
      </Text>
      {pct !== null && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {pct < 0.1 ? "<0.1" : pct.toFixed(1)}% of heap
        </Text>
      )}
    </VStack>
  );
};

const CostCell = ({ credits }: { credits: number | null | undefined }) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (credits === null || credits === undefined) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  return (
    <Text textStyle="text-ui-med">
      {credits < 0.01 ? "<0.01" : credits.toFixed(2)}
    </Text>
  );
};

export { MaintainedObjects };
export default MaintainedObjects;
