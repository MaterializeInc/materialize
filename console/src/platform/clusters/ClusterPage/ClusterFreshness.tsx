// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, useTheme, VStack } from "@chakra-ui/react";
import { useSetAtom } from "jotai";
import React from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { useThresholdControl } from "~/components/ThresholdLineGraph/useThresholdControl";
import { TIME_PERIOD_SEARCH_PARAM_KEY } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { CLUSTER_TIME_PERIOD_STORAGE_KEY } from "~/platform/clusters/constants";
import { TIME_PERIOD_OPTIONS } from "~/platform/freshness/constants";
import {
  FreshnessContent,
  FreshnessFilterControls,
  FreshnessNotes,
} from "~/platform/freshness/FreshnessPage";
import {
  freshnessControlsSearch,
  parseTimePeriod,
  useFreshnessParams,
} from "~/platform/freshness/useFreshnessParams";
import { absoluteFreshnessPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";
import storageAvailable from "~/utils/storageAvailable";

import { freshnessTabSearchAtom } from "./tabLinks";

// Lag history goes back 24 hours, so the Metrics tab's longer ranges have no
// Freshness counterpart.
const LONGEST_TIME_PERIOD_MINUTES = Math.max(
  ...Object.keys(TIME_PERIOD_OPTIONS).map((key) => parseInt(key, 10)),
);

/** The Metrics tab's time range, capped to the longest Freshness offers. */
const readSharedTimePeriod = () => {
  if (!storageAvailable("localStorage")) return undefined;
  const stored = parseInt(
    window.localStorage.getItem(CLUSTER_TIME_PERIOD_STORAGE_KEY) ?? "",
    10,
  );
  if (Number.isNaN(stored)) return undefined;
  return (
    parseTimePeriod(String(Math.min(stored, LONGEST_TIME_PERIOD_MINUTES))) ??
    undefined
  );
};

const writeSharedTimePeriod = (minutes: number) => {
  if (storageAvailable("localStorage")) {
    window.localStorage.setItem(
      CLUSTER_TIME_PERIOD_STORAGE_KEY,
      minutes.toString(),
    );
  }
};

/**
 * The Freshness page fixed to this cluster. Its schema links open the Freshness
 * page, since this tab cannot show a schema.
 */
export const ClusterFreshness = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const regionSlug = useRegionSlug();
  const [sharedTimePeriod] = React.useState(readSharedTimePeriod);
  const {
    threshold,
    timePeriodMinutes,
    predicate,
    objectTypes,
    setThreshold,
    setTimePeriodMinutes,
    setPredicate,
    setObjectTypes,
  } = useFreshnessParams({ defaultTimePeriodMinutes: sharedTimePeriod });
  const setSharedTimePeriodMinutes = (minutes: number) => {
    writeSharedTimePeriod(minutes);
    setTimePeriodMinutes(minutes);
  };
  const thresholdControl = useThresholdControl(threshold, setThreshold);

  // Only the Freshness settings, because the URL can also carry the
  // configuration drawer's section, and saving that would reopen the drawer on
  // the next visit. The time range is left out because the shared one owns it:
  // carried in the tab link, it would override a change made on Metrics.
  const [searchParams] = useSearchParams();
  const savedParams = new URLSearchParams(searchParams);
  savedParams.delete(TIME_PERIOD_SEARCH_PARAM_KEY);
  const search = freshnessControlsSearch(savedParams);
  const setSavedSearch = useSetAtom(freshnessTabSearchAtom);
  React.useEffect(() => setSavedSearch(search), [search, setSavedSearch]);

  return (
    <MainContentContainer mt="10">
      <VStack alignItems="stretch" width="100%" spacing="4">
        <HStack
          spacing="2"
          alignItems="center"
          flexWrap="wrap"
          borderWidth="1px"
          borderColor={colors.border.primary}
          borderRadius="lg"
          padding="3"
        >
          <FreshnessFilterControls
            scope="cluster"
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={setSharedTimePeriodMinutes}
            objectTypes={objectTypes}
            setObjectTypes={setObjectTypes}
          />
        </HStack>
        <AppErrorBoundary message="An error occurred fetching freshness data.">
          <FreshnessContent
            scope="cluster"
            scopeId={clusterId}
            timePeriodMinutes={timePeriodMinutes}
            predicate={predicate}
            setPredicate={setPredicate}
            thresholdControl={thresholdControl}
            typeFilters={objectTypes}
            placementPathname={absoluteFreshnessPath(regionSlug)}
          />
        </AppErrorBoundary>
        <FreshnessNotes />
      </VStack>
    </MainContentContainer>
  );
};
