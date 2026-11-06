// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { useThresholdControl } from "~/components/ThresholdLineGraph/useThresholdControl";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { rangeLabelFor } from "~/platform/freshness/constants";
import {
  FreshnessContent,
  FreshnessFilterControls,
  FreshnessNotes,
} from "~/platform/freshness/FreshnessPage";
import { useFreshnessParams } from "~/platform/freshness/useFreshnessParams";
import { absoluteFreshnessPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

/**
 * The Freshness page fixed to this cluster. Its schema links open the Freshness
 * page, since this tab cannot show a schema.
 */
export const ClusterFreshness = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const regionSlug = useRegionSlug();
  const {
    threshold,
    timePeriodMinutes,
    predicate,
    objectTypes,
    setThreshold,
    setTimePeriodMinutes,
    setPredicate,
    setObjectTypes,
  } = useFreshnessParams();
  const thresholdControl = useThresholdControl(threshold, setThreshold);

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
            setTimePeriodMinutes={setTimePeriodMinutes}
            objectTypes={objectTypes}
            setObjectTypes={setObjectTypes}
          />
        </HStack>
        <AppErrorBoundary message="An error occurred fetching freshness data.">
          <FreshnessContent
            scope="cluster"
            scopeId={clusterId}
            lookbackMs={timePeriodMinutes * 60_000}
            rangeLabel={rangeLabelFor(timePeriodMinutes)}
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
