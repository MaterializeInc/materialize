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
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { isSystemCluster } from "~/api/materialize";
import { LATEST_READING_INTERVAL_MS } from "~/api/materialize/freshness/lagHistory";
import Alert from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import ErrorBox from "~/components/ErrorBox";
import { DataPoint } from "~/components/FreshnessGraph/types";
import { LoadingContainer } from "~/components/LoadingContainer";
import SchemaSelect, { SchemaOption } from "~/components/SchemaSelect";
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
import { useNow } from "~/hooks/useNow";
import { ClustersIcon, SchemaIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  FreshnessData,
  useClusterFreshness,
  useFreshnessObjectsWhere,
} from "~/platform/clusters/queries";
import { useAllClusters } from "~/store/allClusters";
import { useAllObjects } from "~/store/allObjects";
import { useAllSchemas } from "~/store/allSchemas";
import { MaterializeTheme } from "~/theme";
import { formatDurationExact, formatDurationForAxis } from "~/utils/format";

import {
  FreshnessScope,
  OBJECT_TYPE_FILTERS,
  SCOPE_LABELS,
  TIME_PERIOD_OPTIONS,
} from "./constants";
import {
  describeThreshold,
  latestPeak,
  Predicate,
  PREDICATE_LABELS,
  qualifiedSchemaName,
} from "./freshnessRows";
import { FreshnessStatboxes } from "./FreshnessStatboxes";
import { FreshnessTable } from "./FreshnessTable";
import { fitToRange } from "./graphWindow";
import {
  HydrationCounts,
  inFreshnessScope,
  LagPercentiles,
  useFreshnessHydration,
  useLatestLag,
  useObjectLagPercentiles,
} from "./queries";
import {
  parsePredicate,
  parseScope,
  useFreshnessParams,
} from "./useFreshnessParams";
import { useFreshnessRows } from "./useFreshnessRows";

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

// Shared so that every render before hydration lands passes the same map, which
// keeps the rows cache in `useFreshnessRows` from rebuilding each time.
const NO_HYDRATION = new Map<string, HydrationCounts>();

interface FreshnessViewOptions {
  scope: FreshnessScope;
  rangeLabel: string;
  predicate: Predicate;
  thresholdControl: ThresholdControl;
  typeFilters: string[];
}

interface HighlightControlsProps {
  thresholdControl: ThresholdControl;
  predicate: Predicate;
  setPredicate: (predicate: Predicate) => void;
}

const HighlightControls = ({
  thresholdControl,
  predicate,
  setPredicate,
}: HighlightControlsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack
      spacing="2"
      alignItems="center"
      flexWrap="wrap"
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="lg"
      padding="3"
    >
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        Highlight objects whose
      </Text>
      <Select
        size="sm"
        width="auto"
        aria-label="Freshness statistic"
        value={predicate}
        onChange={(e) =>
          setPredicate(parsePredicate(e.target.value) ?? predicate)
        }
      >
        {(Object.keys(PREDICATE_LABELS) as Predicate[]).map((key) => (
          <option key={key} value={key}>
            {PREDICATE_LABELS[key].menu}
          </option>
        ))}
      </Select>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {/* "Current" is this minute; the others summarize the range. */}
        freshness {predicate === "current" ? "exceeds" : "exceeded"}
      </Text>
      <ThresholdInput
        {...thresholdControl.inputProps}
        ariaLabel="Freshness threshold in seconds"
      />
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        seconds
      </Text>
    </HStack>
  );
};

const FreshnessContent = ({
  scope,
  scopeId,
  lookbackMs,
  setPredicate,
  ...viewOptions
}: FreshnessViewOptions & {
  setPredicate: (predicate: Predicate) => void;
  scopeId: string;
  lookbackMs: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const objects = useFreshnessObjectsWhere(inFreshnessScope(scope, scopeId));
  // Distinguishes a scope that has nothing in it from one whose objects have
  // not arrived yet. Both are an empty list, and they are not the same state.
  const { snapshotComplete } = useAllObjects();
  const objectIds = objects.map((object) => object.objectId);
  const freshness = useClusterFreshness({ lookbackMs, objects });
  // The graph ends at now, advanced each minute with "Now", not at the last
  // fetch, which at 24 hours can be a whole bin (24 minutes) old.
  const now = useNow(LATEST_READING_INTERVAL_MS);
  const graphData = React.useMemo(
    () => freshness.data && fitToRange(freshness.data, lookbackMs, now),
    [freshness.data, lookbackMs, now],
  );
  const latest = useLatestLag(objectIds);
  const percentiles = useObjectLagPercentiles({ objectIds, lookbackMs });
  const hydration = useFreshnessHydration(objectIds);
  const { typeFilters, thresholdControl, predicate } = viewOptions;
  // The statboxes rank the scope's objects, not the loaded rows, so they stay
  // up while a new range loads. An object with no readings adds nothing to
  // the ranking either way.
  const statboxObjectIds = React.useMemo(
    () =>
      objects
        .filter(
          (object) =>
            typeFilters.length === 0 || typeFilters.includes(object.objectType),
        )
        .map((object) => object.objectId),
    [objects, typeFilters],
  );
  const statboxLatestPeak = latestPeak(statboxObjectIds, latest.data);

  // Before the Summary, the graph and the tables, because each of them would
  // otherwise render its own empty state and the page would read as a passing
  // health check for a scope with nothing in it.
  if (objects.length === 0 && snapshotComplete) {
    return (
      <Box padding="4" color={colors.foreground.secondary}>
        No objects {SCOPE_LABELS[scope].place}.
      </Box>
    );
  }

  // Waits for all three, because drawing the graph before the latest readings
  // or the percentiles would show columns that disagree with it. An error only
  // replaces the page when there is nothing to show, so a failed refetch keeps
  // the last good readings on screen.
  return (
    <VStack alignItems="stretch" width="100%" spacing="4">
      <Accordion allowMultiple defaultIndex={[0]}>
        <AccordionItem>
          <SectionHeader title="Summary" />
          <AccordionPanel px="0">
            <FreshnessStatboxes
              objectIds={statboxObjectIds}
              lookbackMs={lookbackMs}
              latestPeak={statboxLatestPeak}
              objectsReady={snapshotComplete}
            />
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
      <HighlightControls
        thresholdControl={thresholdControl}
        predicate={predicate}
        setPredicate={setPredicate}
      />
      {graphData && latest.data && percentiles.data && objects.length > 0 ? (
        // A failed hydration request leaves the column at "—" rather than
        // taking the page down: the lag readings are what the page is for.
        <FreshnessView
          data={graphData}
          latestByObjectId={latest.data}
          percentilesByObjectId={percentiles.data}
          hydrationByObjectId={hydration.data ?? NO_HYDRATION}
          scope={scope}
          {...viewOptions}
        />
      ) : freshness.isError || latest.isError || percentiles.isError ? (
        <ErrorBox message="An error occurred fetching freshness data." />
      ) : (
        <Box height="320px" width="100%">
          <LoadingContainer />
        </Box>
      )}
    </VStack>
  );
};

const FreshnessView = ({
  data: { historicalData, startTime, endTime, lines, objectsById },
  latestByObjectId,
  percentilesByObjectId,
  hydrationByObjectId,
  scope,
  rangeLabel,
  predicate,
  thresholdControl,
  typeFilters,
}: FreshnessViewOptions & {
  data: FreshnessData;
  latestByObjectId: Map<string, number | null>;
  percentilesByObjectId: Map<string, LagPercentiles>;
  hydrationByObjectId: Map<string, HydrationCounts>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { judged, rows, breaching } = useFreshnessRows({
    lines,
    latestByObjectId,
    percentilesByObjectId,
    objectsById,
    hydrationByObjectId,
    typeFilters,
    predicate,
    threshold: thresholdControl.settled,
  });

  const thresholdClause = describeThreshold(
    formatDurationExact(thresholdControl.settled),
    predicate,
    rangeLabel,
  );

  return (
    <VStack alignItems="stretch" width="100%" spacing="4">
      <Accordion allowMultiple defaultIndex={[0, 1]}>
        <AccordionItem>
          <SectionHeader title="Lag Over Time" count={`(${rows.length})`} />
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
            />
          </AccordionPanel>
        </AccordionItem>

        <AccordionItem>
          <SectionHeader
            title="Exceeding Threshold"
            count={`(${breaching.length}/${rows.length})`}
          />
          <AccordionPanel px="0">
            {breaching.length === 0 ? (
              <Box padding="4" color={colors.foreground.secondary}>
                <Text as="span" color={colors.accent.green}>
                  No objects exceeded {thresholdClause}.
                </Text>{" "}
                All {rows.length} objects are within target.
              </Box>
            ) : (
              <FreshnessTable
                rows={breaching}
                scope={scope}
                itemLabel="objects"
              />
            )}
          </AccordionPanel>
        </AccordionItem>

        <AccordionItem>
          <SectionHeader title="All Objects" count={`(${rows.length})`} />
          <AccordionPanel px="0">
            {rows.length === 0 ? (
              <Box padding="4" color={colors.foreground.secondary}>
                No objects with freshness data {SCOPE_LABELS[scope].place}.
              </Box>
            ) : (
              <FreshnessTable rows={rows} scope={scope} itemLabel="objects" />
            )}
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
    </VStack>
  );
};

const FreshnessPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const {
    data: clusters,
    error: clustersError,
    snapshotComplete: clustersReady,
  } = useAllClusters();
  const {
    data: schemas,
    error: schemasError,
    snapshotComplete: schemasReady,
  } = useAllSchemas({ includeSystemSchemas: false });
  const {
    scope,
    clusterId,
    schemaId,
    threshold,
    timePeriodMinutes,
    predicate,
    objectTypes,
    setScope,
    setClusterId,
    setSchemaId,
    setThreshold,
    setTimePeriodMinutes,
    setPredicate,
    setObjectTypes,
  } = useFreshnessParams();
  const thresholdControl = useThresholdControl(threshold, setThreshold);

  const scopeLabels = SCOPE_LABELS[scope];
  // The shape `SchemaSelect` takes, which groups schemas under their
  // database. A cluster has none.
  const options: SchemaOption[] = (
    scope === "cluster"
      ? clusters
          .filter((c) => !isSystemCluster(c.id))
          .map((c) => ({ id: c.id, name: c.name, databaseName: null }))
      : schemas.map((s) => ({
          id: s.id,
          name: s.name,
          databaseName: s.databaseName,
        }))
  ).sort((a, b) =>
    qualifiedSchemaName(a.databaseName, a.name).localeCompare(
      qualifiedSchemaName(b.databaseName, b.name),
    ),
  );
  const selectedId = scope === "cluster" ? clusterId : schemaId;
  // Nothing loads until a cluster or schema is chosen, so opening the page
  // never spends queries on one nobody asked about. A param naming one that
  // is not there does not fall back either: an old link, or one to a dropped
  // or system cluster or schema, would otherwise show a different one under
  // the name the URL still carries.
  const selected = selectedId
    ? options.find((option) => option.id === selectedId)
    : undefined;
  const notFound = Boolean(selectedId) && selected === undefined;
  const ScopeIcon = scope === "cluster" ? ClustersIcon : SchemaIcon;
  const pickerProps = {
    ariaLabel: scopeLabels.menu,
    placeholder: `Select a ${scopeLabels.noun}`,
    // Cluster and schema names run long and are often distinguished only by
    // their last few characters, so a truncated one is unusable.
    // `max-content` rather than the base `fit-content`, which resolves
    // against the select's own container and so cannot grow past the closed
    // control.
    containerWidth: "max-content",
    menuWidth: "240px",
    leftIcon: (
      <ScopeIcon height="4" width="4" color={colors.foreground.secondary} />
    ),
  };
  const scopeError = scope === "cluster" ? clustersError : schemasError;
  const scopeReady = scope === "cluster" ? clustersReady : schemasReady;

  const rangeLabel =
    TIME_PERIOD_OPTIONS[
      String(timePeriodMinutes) as keyof typeof TIME_PERIOD_OPTIONS
    ] ?? `${timePeriodMinutes}m`;

  // The heading stays and the body swaps, so the page does not flash empty on
  // every load. The control bar goes with the body: its picker has nothing to
  // offer until the subscribe lands.
  if (scopeError || !scopeReady) {
    return (
      <MainContentContainer>
        <PageHeader variant="compact" boxProps={{ pb: "4" }}>
          <PageHeading>Freshness</PageHeading>
        </PageHeader>
        {scopeError ? (
          <Alert
            variant="error"
            message={`An error occurred loading ${scopeLabels.plural}.`}
          />
        ) : (
          <Spinner data-testid="loading-spinner" />
        )}
      </MainContentContainer>
    );
  }

  return (
    <MainContentContainer>
      <PageHeader variant="compact" boxProps={{ pb: "4" }}>
        <PageHeading>Freshness</PageHeading>
        <VStack
          mt="4"
          width="100%"
          gap="3"
          alignItems="flex-start"
          borderWidth="1px"
          borderColor={colors.border.primary}
          borderRadius="lg"
          padding="3"
        >
          <HStack spacing="2" alignItems="center" flexWrap="wrap">
            <Box minWidth="60">
              <SearchableSelect<SelectOption, true>
                isMulti
                ariaLabel="Object types"
                placeholder="All object types"
                options={OBJECT_TYPE_FILTERS[scope].map((filter) => ({
                  id: filter.value,
                  name: filter.label,
                }))}
                value={OBJECT_TYPE_FILTERS[scope]
                  .filter((filter) => objectTypes.includes(filter.value))
                  .map((filter) => ({ id: filter.value, name: filter.label }))}
                onChange={(chosen) =>
                  setObjectTypes(chosen.map((option) => option.id))
                }
              />
            </Box>
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              within
            </Text>
            <Select
              size="sm"
              width="auto"
              aria-label="Show objects by"
              value={scope}
              onChange={(e) => setScope(parseScope(e.target.value) ?? scope)}
            >
              {(Object.keys(SCOPE_LABELS) as FreshnessScope[]).map((key) => (
                <option key={key} value={key}>
                  {SCOPE_LABELS[key].menu}
                </option>
              ))}
            </Select>
            <Box minWidth="52">
              {scope === "cluster" ? (
                <SearchableSelect<SchemaOption, false>
                  {...pickerProps}
                  options={options}
                  value={selected ?? null}
                  onChange={(option) => option && setClusterId(option.id)}
                />
              ) : (
                <SchemaSelect
                  {...pickerProps}
                  schemas={options}
                  value={selected ?? null}
                  onChange={(option) => option && setSchemaId(option.id)}
                />
              )}
            </Box>
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              over the last
            </Text>
            <TimePeriodSelect
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
              options={TIME_PERIOD_OPTIONS}
            />
          </HStack>
        </VStack>
      </PageHeader>
      <VStack alignItems="stretch" width="100%" spacing="4">
        {notFound ? (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            That {scopeLabels.noun} was not found. Select another{" "}
            {scopeLabels.noun} using the menu above.
          </Text>
        ) : selected ? (
          <AppErrorBoundary message="An error occurred fetching freshness data.">
            <FreshnessContent
              scope={scope}
              scopeId={selected.id}
              lookbackMs={timePeriodMinutes * 60_000}
              rangeLabel={rangeLabel}
              predicate={predicate}
              setPredicate={setPredicate}
              thresholdControl={thresholdControl}
              typeFilters={objectTypes}
            />
          </AppErrorBoundary>
        ) : options.length === 0 ? (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            No {scopeLabels.plural} to show freshness for.
          </Text>
        ) : (
          // Names both scopes whichever is active, so a reader who arrived by
          // cluster learns that schemas are on offer too.
          <Text textStyle="text-base">
            Select a cluster or schema to see the freshness of its objects.
          </Text>
        )}

        {/* Explains the numbers above, so it waits until there are some. */}
        {selected && (
          <VStack
            alignItems="flex-start"
            spacing="2"
            color={colors.foreground.secondary}
          >
            <Text textStyle="text-small">
              Freshness is wallclock lag: how far behind wall clock time an
              object&rsquo;s results are. Each object reports it once a minute.
              A percentile ranks those minutes, so p99 is the lag that 99% of
              minutes stayed at or under. The Summary section pools the readings
              for all objects shown and ranks those results together.
            </Text>
            <Text textStyle="text-small">
              The graph displays 60 points per object. Each point describes the
              worst lag observed on that object during a timespan equal to 1/60
              of the selected time window. At 6 hours, for example, each point
              represents the worst minute during a 6-minute span.
            </Text>
            <Text textStyle="text-small">
              A percentile needs enough readings to differ from pMAX: about 100
              for p99 and 1,000 for p99.9. Over 1 hour an object has 60, so its
              p99 equals its pMAX. A scope with few objects displays this same
              convergence in the Summary section.
            </Text>
            <Text textStyle="text-small">
              The &ldquo;Now&rdquo; column in the tables and the
              &ldquo;current&rdquo; option in the threshold menu display the
              reading from the latest minute. The final point on the graph
              displays the worst minute in the most recent timespan (e.g., 24
              minutes at a 24 hour time window). Therefore, at longer time
              windows, the last point on the graph can sit above the
              &ldquo;Now&rdquo; reading in the table.
            </Text>
            <Text textStyle="text-small">
              pMAX (in the table, as a threshold option and in the Summary)
              updates once per minute. The graph and all percentiles refresh
              once per timespan.
            </Text>
          </VStack>
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default FreshnessPage;
