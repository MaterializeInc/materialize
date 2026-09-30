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
  Input,
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
import { ThresholdLineGraph } from "~/components/ThresholdLineGraph/ThresholdLineGraph";
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

import {
  OBJECT_TYPE_FILTERS,
  THRESHOLD_STEP_MS,
  TIME_PERIOD_OPTIONS,
} from "./constants";
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
import { useSettledThreshold } from "./useSettledThreshold";

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
  liveThreshold,
  settledThreshold,
  onThresholdChange,
  typeFilters,
}: {
  clusterId: string;
  lookbackMs: number;
  rangeLabel: string;
  predicate: Predicate;
  liveThreshold: number;
  settledThreshold: number;
  onThresholdChange: (value: number) => void;
  typeFilters: string[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const objects = useFreshnessObjects(clusterId);
  const {
    data: { historicalData, startTime, endTime, lines, objectsById },
  } = useClusterFreshness({ lookbackMs, objects });

  // Hydration is its own query: `buildLagHistoryQuery` is shared with pages
  // that never show it, and joined there it cost all of them a scan.
  const objectIds = React.useMemo(
    () => Array.from(objectsById.keys()),
    [objectsById],
  );
  const { data: hydrationByObjectId } = useFreshnessHydration(objectIds);

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
    () => buildStats(visibleLines, historicalData),
    [visibleLines, historicalData],
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
        threshold: settledThreshold,
        selectedKeys,
      }),
    [
      judged,
      statsByKey,
      objectsById,
      hydrationByObjectId,
      settledThreshold,
      selectedKeys,
    ],
  );

  const breaching = rows.filter((row) => row.breaching);
  const ok = breaching.length === 0;
  const predicateLabel = PREDICATE_LABELS[predicate];
  const window =
    predicate === "current" ? "" : ` in the ${rangeLabel.toLowerCase()}`;

  return (
    <VStack alignItems="stretch" width="100%" spacing="4">
      <HStack spacing="2" alignItems="center">
        <Box
          width="9px"
          height="9px"
          borderRadius="full"
          background={ok ? colors.accent.green : colors.accent.red}
        />
        <Text textStyle="text-base">
          <b>
            {breaching.length} of {rows.length}
          </b>{" "}
          {rows.length === 1 ? "object" : "objects"} exceeded{" "}
          {formatDurationForAxis(settledThreshold)} {predicateLabel}
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
              threshold={liveThreshold}
              onThresholdChange={onThresholdChange}
              thresholdStep={THRESHOLD_STEP_MS}
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
            <FreshnessTable
              rows={breaching}
              emptyMessage={
                <>
                  <Text as="span" color={colors.accent.green}>
                    No objects exceeded{" "}
                    {formatDurationForAxis(settledThreshold)} {predicateLabel}
                    {window}.
                  </Text>{" "}
                  All {rows.length} objects are within target.
                </>
              }
            />
          </AccordionPanel>
        </AccordionItem>

        <AccordionItem>
          <SectionHeader title="All objects" count={`(${rows.length})`} />
          <AccordionPanel px="0">
            <FreshnessTable
              rows={rows}
              onToggleRow={toggleRow}
              emptyMessage="No objects with freshness data on this cluster."
            />
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
  const {
    live: liveThreshold,
    settled: settledThreshold,
    onChange: onThresholdChange,
  } = useSettledThreshold(threshold, setThreshold);

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
            <Input
              type="number"
              size="sm"
              width="20"
              min={0}
              step={THRESHOLD_STEP_MS / 1000}
              aria-label="Freshness threshold in seconds"
              value={(liveThreshold / 1000).toString()}
              onChange={(e) => {
                const seconds = Number(e.target.value);
                if (Number.isFinite(seconds) && seconds >= 0) {
                  onThresholdChange(seconds * 1000);
                }
              }}
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
          <AppErrorBoundary message="An error occurred fetching freshness data.">
            <React.Suspense
              fallback={
                <Box height="320px" width="100%">
                  <LoadingContainer />
                </Box>
              }
            >
              <FreshnessContent
                key={selected.id}
                clusterId={selected.id}
                lookbackMs={timePeriodMinutes * 60_000}
                rangeLabel={rangeLabel}
                predicate={predicate}
                liveThreshold={liveThreshold}
                settledThreshold={settledThreshold}
                onThresholdChange={onThresholdChange}
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
          object&rsquo;s results are. Series from{" "}
          <Text as="span" fontFamily="mono">
            mz_wallclock_global_lag_recent_history
          </Text>
          , hydration from{" "}
          <Text as="span" fontFamily="mono">
            mz_hydration_statuses
          </Text>
          . Readings are binned to 60 points per range and each point is the
          worst lag in its span, so p90 is a percentile of those maxima.
        </Text>
      </VStack>
    </MainContentContainer>
  );
};

export default FreshnessPage;
