// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  HStack,
  Select,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { isSystemCluster } from "~/api/materialize";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { DataPoint } from "~/components/FreshnessGraph/types";
import { LoadingContainer } from "~/components/LoadingContainer";
import SearchableSelect, {
  SelectOption,
} from "~/components/SearchableSelect/SearchableSelect";
import { ThresholdInput } from "~/components/ThresholdLineGraph/ThresholdInput";
import { ThresholdLineGraph } from "~/components/ThresholdLineGraph/ThresholdLineGraph";
import {
  ThresholdControl,
  useThresholdControl,
} from "~/components/ThresholdLineGraph/useThresholdControl";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { ClustersIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  useClusterFreshness,
  useFreshnessObjects,
} from "~/platform/clusters/queries";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { formatDurationForAxis } from "~/utils/format";

import { OBJECT_TYPE_FILTERS, TIME_PERIOD_OPTIONS } from "./constants";
import {
  buildFreshnessRows,
  buildStats,
  judgeLines,
  Predicate,
  PREDICATE_LABELS,
} from "./freshnessRows";
import { FreshnessTable } from "./FreshnessTable";
import { useFreshnessHydration } from "./queries";
import { useFreshnessParams } from "./useFreshnessParams";

const SectionHeader = ({
  title,
  count,
}: {
  title: string;
  count?: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <AccordionButton px="0">
      <AccordionIcon />
      <Text textStyle="text-ui-med" ml="2">
        {title}
      </Text>
      {count !== undefined && (
        <Text
          textStyle="text-ui-reg"
          color={colors.foreground.secondary}
          ml="2"
        >
          {count}
        </Text>
      )}
    </AccordionButton>
  );
};

const FreshnessContent = ({
  clusterId,
  lookbackMs,
  rangeLabel,
  predicate,
  thresholdControl,
  typeFilters,
}: {
  clusterId: string;
  lookbackMs: number;
  rangeLabel: string;
  predicate: Predicate;
  thresholdControl: ThresholdControl;
  typeFilters: string[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const objects = useFreshnessObjects(clusterId);
  const {
    data: {
      historicalData,
      startTime,
      endTime,
      lines,
      objectsById,
      latestByObjectId,
    },
  } = useClusterFreshness({ lookbackMs, objects });

  // Hydration is its own query: `buildLagHistoryQuery` is shared with pages
  // that never show it, and joined there it cost all of them a scan.
  //
  // Its IDs come from `objects` rather than from the lag result, so the two
  // requests go together. Taking them from the result would make this one wait,
  // since a suspending query stops the component before this line is reached.
  const { data: hydrationByObjectId } = useFreshnessHydration(
    objects.map((object) => object.objectId),
  );

  // Picked by hand in the All objects table, on top of whatever breaches.
  const [selectedKeys, setSelectedKeys] = React.useState<ReadonlySet<string>>(
    new Set(),
  );
  const toggleRow = React.useCallback((key: string) => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (!next.delete(key)) next.add(key);
      return next;
    });
  }, []);

  const visibleLines = React.useMemo(
    () =>
      lines.filter((line) => {
        const object = objectsById.get(line.key);
        return (
          typeFilters.length === 0 ||
          (object !== undefined && typeFilters.includes(object.objectType))
        );
      }),
    [lines, objectsById, typeFilters],
  );

  const statsByKey = React.useMemo(
    () => buildStats(visibleLines, historicalData, latestByObjectId),
    [visibleLines, historicalData, latestByObjectId],
  );
  const judged = React.useMemo(
    () => judgeLines(visibleLines, historicalData, predicate, statsByKey),
    [visibleLines, historicalData, predicate, statsByKey],
  );

  // Built from the settled threshold so a drag does not re-render a row per
  // object on every pointer move.
  const rows = React.useMemo(
    () =>
      buildFreshnessRows({
        judged,
        statsByKey,
        objectsById,
        hydrationByObjectId,
        threshold: thresholdControl.settled,
        selectedKeys,
      }),
    [
      judged,
      statsByKey,
      objectsById,
      hydrationByObjectId,
      thresholdControl.settled,
      selectedKeys,
    ],
  );

  // Memoized for the same reason as `rows`: a fresh array per render reaches
  // `FreshnessTable` as a changed prop, so the memo on the table cannot hold
  // during a drag.
  const breaching = React.useMemo(
    () => rows.filter((row) => row.breaching),
    [rows],
  );
  const ok = breaching.length === 0;
  const predicateLabel = PREDICATE_LABELS[predicate];
  const window =
    predicate === "current" ? "" : ` in the ${rangeLabel.toLowerCase()}`;

  return (
    <VStack alignItems="stretch" width="100%" spacing="4">
      <HStack spacing="2" alignItems="center">
        <Box
          boxSize="2.5"
          borderRadius="full"
          background={ok ? colors.accent.green : colors.accent.red}
        />
        <Text textStyle="text-base">
          <b>
            {breaching.length} of {rows.length}
          </b>{" "}
          {rows.length === 1 ? "object" : "objects"} exceeded{" "}
          {formatDurationForAxis(thresholdControl.settled)} {predicateLabel}
          {window}.
        </Text>
      </HStack>

      <Accordion allowMultiple defaultIndex={[0, 1]}>
        <AccordionItem>
          <SectionHeader
            title="Freshness"
            count={`${rows.length} ${rows.length === 1 ? "object" : "objects"}`}
          />
          <AccordionPanel px="0">
            <ThresholdLineGraph<DataPoint>
              data={historicalData}
              lines={judged}
              xAccessor={(d) => d.timestamp}
              startTime={startTime}
              endTime={endTime}
              {...thresholdControl.graphProps}
              formatValue={formatDurationForAxis}
              thresholdLabel="Freshness threshold"
              graphLabel="Object freshness over time"
              selectedKeys={selectedKeys}
            />
          </AccordionPanel>
        </AccordionItem>

        <AccordionItem>
          <SectionHeader
            title="Exceeding threshold"
            count={`(${breaching.length}/${rows.length})`}
          />
          <AccordionPanel px="0">
            {breaching.length === 0 ? (
              <Box padding="4" color={colors.foreground.secondary}>
                <Text as="span" color={colors.accent.green}>
                  No objects exceeded{" "}
                  {formatDurationForAxis(thresholdControl.settled)}{" "}
                  {predicateLabel}
                  {window}.
                </Text>{" "}
                All {rows.length} objects are within target.
              </Box>
            ) : (
              <FreshnessTable rows={breaching} itemLabel="objects" />
            )}
          </AccordionPanel>
        </AccordionItem>

        <AccordionItem>
          <SectionHeader title="All objects" count={`(${rows.length})`} />
          <AccordionPanel px="0">
            {rows.length === 0 ? (
              <Box padding="4" color={colors.foreground.secondary}>
                No objects with freshness data on this cluster.
              </Box>
            ) : (
              <FreshnessTable
                rows={rows}
                onToggleRow={toggleRow}
                itemLabel="objects"
              />
            )}
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
    </VStack>
  );
};

const FreshnessPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: clusters } = useAllClusters();
  const {
    clusterId,
    threshold,
    timePeriodMinutes,
    predicate,
    objectTypes,
    setClusterId,
    setThreshold,
    setTimePeriodMinutes,
    setPredicate,
    setObjectTypes,
  } = useFreshnessParams();
  const thresholdControl = useThresholdControl(threshold, setThreshold);

  // Filter out system clusters
  const selectable = React.useMemo(
    () =>
      clusters
        .filter((c) => !isSystemCluster(c.id))
        .sort((a, b) => a.name.localeCompare(b.name)),
    [clusters],
  );
  const selected = selectable.find((c) => c.id === clusterId) ?? selectable[0];
  const options: SelectOption[] = selectable.map((c) => ({
    id: c.id,
    name: c.name,
  }));

  const rangeLabel =
    TIME_PERIOD_OPTIONS[
      String(timePeriodMinutes) as keyof typeof TIME_PERIOD_OPTIONS
    ] ?? `${timePeriodMinutes}m`;

  return (
    <MainContentContainer>
      <PageHeader variant="compact" sticky boxProps={{ pb: "4" }}>
        <PageHeading>Freshness</PageHeading>
        <VStack
          mt="4"
          width="100%"
          flexWrap="wrap"
          gap="3"
          alignItems="left"
          borderWidth="1px"
          borderColor={colors.border.primary}
          borderRadius="lg"
          padding="3"
        >
          <HStack spacing="2" alignItems="center">
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              Highlight objects that exceeded
            </Text>
            <ThresholdInput
              {...thresholdControl.inputProps}
              ariaLabel="Freshness threshold in seconds"
            />
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              seconds
            </Text>
            <Select
              size="sm"
              width="auto"
              aria-label="When the threshold must be exceeded"
              value={predicate}
              onChange={(e) => setPredicate(e.target.value as Predicate)}
            >
              {(Object.keys(PREDICATE_LABELS) as Predicate[]).map((key) => (
                <option key={key} value={key}>
                  {PREDICATE_LABELS[key]}
                </option>
              ))}
            </Select>
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              over the last
            </Text>
            <TimePeriodSelect
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
              options={TIME_PERIOD_OPTIONS}
            />
          </HStack>
          <HStack spacing="2" alignItems="center">
            <Box minWidth="52">
              <SearchableSelect<SelectOption, false>
                ariaLabel="Cluster"
                options={options}
                value={
                  selected ? { id: selected.id, name: selected.name } : null
                }
                onChange={(option) => option && setClusterId(option.id)}
                leftIcon={
                  <ClustersIcon
                    height="4"
                    width="4"
                    color={colors.foreground.secondary}
                  />
                }
              />
            </Box>
            <Box minWidth="60">
              <SearchableSelect<SelectOption, true>
                isMulti
                ariaLabel="Object types"
                placeholder="All object types"
                options={OBJECT_TYPE_FILTERS.map((filter) => ({
                  id: filter.value,
                  name: filter.label,
                }))}
                value={OBJECT_TYPE_FILTERS.filter((filter) =>
                  objectTypes.includes(filter.value),
                ).map((filter) => ({ id: filter.value, name: filter.label }))}
                onChange={(chosen) =>
                  setObjectTypes(chosen.map((option) => option.id))
                }
              />
            </Box>
          </HStack>
        </VStack>
      </PageHeader>
      <VStack alignItems="stretch" width="100%" spacing="4">
        {selected ? (
          // The key is on the boundary, not on the content: once a boundary
          // has caught an error it renders its fallback instead of its
          // children, so re-keying a child it is no longer rendering does
          // nothing. A failed query would otherwise survive a change of
          // cluster or range until a reload.
          <AppErrorBoundary
            key={`${selected.id}:${timePeriodMinutes}`}
            message="An error occurred fetching freshness data."
          >
            <React.Suspense
              fallback={
                <Box height="320px" width="100%">
                  <LoadingContainer />
                </Box>
              }
            >
              <FreshnessContent
                clusterId={selected.id}
                lookbackMs={timePeriodMinutes * 60_000}
                rangeLabel={rangeLabel}
                predicate={predicate}
                thresholdControl={thresholdControl}
                typeFilters={objectTypes}
              />
            </React.Suspense>
          </AppErrorBoundary>
        ) : (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            No clusters to show freshness for.
          </Text>
        )}

        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Freshness is wallclock lag: how far behind wall clock time an
          object&rsquo;s results are. Readings are binned to 60 points per
          range, and each point is the worst lag in its span, so p90 is a
          percentile of those maxima. The &ldquo;Now&rdquo; column is the most
          recent single reading rather than a binned point, so at wider ranges a
          point on the graph can sit above the &ldquo;Now&rdquo; value in its
          row.
        </Text>
      </VStack>
    </MainContentContainer>
  );
};

export default FreshnessPage;
