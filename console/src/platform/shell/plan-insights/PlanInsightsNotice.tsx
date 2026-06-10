// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, useTheme } from "@chakra-ui/react";
import { useSetAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import { useEffect, useMemo, useState } from "react";
import React from "react";

import { useSegment } from "~/analytics/segment";
import { Cluster, Notice } from "~/api/materialize/types";
import CountSignifier from "~/components/CountSignifier";
import { MaterializeTheme } from "~/theme";

import {
  CommandOutput,
  shellStateAtom,
  updateDisplayStateAtomCallback,
} from "../store/shell";
import {
  calculateCommandDuration,
  doesCommandExceedTimeout,
  getCommandResultCompletedTime,
} from "../timings";
import { useShellVirtualizedList } from "../useShellVirtualizedList";
import {
  BlockedDependencies,
  NonRunningSources,
  useBlockedDependencies,
  useNonRunningSources,
} from "./queries";
import {
  countVisiblePlanInsights,
  planInsightsToInstrumentationIds,
} from "./utils";

type ImportType = "storage" | "compute";
type Name = {
  item: string;
  schema: string;
  database?: string;
};
type ImportInsights = {
  [objectId: string]: {
    name: Name;
    type: ImportType;
  };
};

// An object of possible clusters that would lead to fast path queries.
type FastPathClustersInsights = {
  [clusterName: string]: {
    index: Name; // Name of the index
    on: Name; // Name of the view / object
  };
};

type PlanInsightsServerPayload = {
  imports: ImportInsights;
  fast_path_clusters: FastPathClustersInsights;
};

type ExplainContext = {
  plans: {
    raw: string;
    optimized: {
      global: {
        json: string;
        text: string;
      };
    };
  };
  insights: PlanInsightsServerPayload | null;
  cluster: {
    name: string;
    id:
      | {
          System: number;
        }
      | {
          User: number;
        };
  };
  redacted_sql?: string;
};

// Combination of the plan insights state from the notice payload and
// insights we query ourselves
export type PlanInsights = {
  imports: ImportInsights;
  fastPathClusters: FastPathClustersInsights;
  blockedDependencies: BlockedDependencies;
  nonRunningSources: NonRunningSources;
  cluster: Cluster;
  redactedSql?: string;
};

export const PLAN_INSIGHTS_NOTICE_CODE = "MZ001";
const PUBLISH_PLAN_INSIGHTS_TIMEOUT_MS = 3_000;
const PLAN_INSIGHTS_VISIBILITY_POLL_INTERVAL_MS = 100;

/**
 * Notices used to display insights for slow queries. Requires the 'emit_plan_insights_notice' session variable to be set.
 */
const PlanInsightsNotice = ({
  notice,
  commandOutput,
  commandResultIndex,
}: {
  notice: Notice;
  commandOutput: CommandOutput;
  commandResultIndex: number;
}) => {
  const { track } = useSegment();
  const { colors } = useTheme<MaterializeTheme>();

  const setShellState = useSetAtom(shellStateAtom);
  const { scrollToBottom } = useShellVirtualizedList();

  const updateDisplayState = useAtomCallback(updateDisplayStateAtomCallback);

  const timeTakenMs = calculateCommandDuration(
    commandResultIndex,
    commandOutput,
  );

  const commandStartTimeMs =
    getCommandResultCompletedTime(commandOutput, commandResultIndex - 1) ??
    commandOutput.commandSentTimeMs;

  /**
   * We initially check if the notice should be visible based on how long the command took
   * to execute or how long the command has been in progress.
   *
   */
  const [isNoticeVisibleFromTimer, setIsNoticeVisibleFromTimer] =
    useState(false);

  const displayState =
    commandOutput.commandResultsDisplayStates[commandResultIndex];

  const planInsights = displayState.planInsights;

  const isPublished = planInsights !== undefined;

  const explainContext = useMemo(
    () => JSON.parse(notice.message) as ExplainContext,
    [notice.message],
  );

  const importIds = Object.keys(explainContext.insights?.imports ?? {});

  const {
    data: blockedDependenciesData,
    isLoading: isBlockedDependenciesLoading,
  } = useBlockedDependencies({
    objectIds: importIds,
    commandResultIndex,
    historyId: commandOutput.historyId,
    hasSuccessfullyFetchedOnce: isPublished,
  });

  const { data: nonRunningSourcesData, isLoading: isNonRunningSourcesLoading } =
    useNonRunningSources({
      objectIds: importIds,
      commandResultIndex,
      historyId: commandOutput.historyId,
      hasSuccessfullyFetchedOnce: isPublished,
    });

  /**
   * On mount, we set an interval to check if a command exceeds the time threshold to show plan insights. A command can either exceed the time threshold while it is in progress
   * or if it has completed.
   *
   * Due to it being an interval instead of a timeout, we'll show plan insights at an upper bound of PUBLISH_PLAN_INSIGHTS_TIMEOUT_MS + PLAN_INSIGHTS_VISIBILITY_POLL_INTERVAL_MS instead of
   * just PUBLISH_PLAN_INSIGHTS_TIMEOUT_MS. This is acceptable since using an setInterval instead of a setTimeout simplifies the logic.
   *
   * The only variables expected to change in this effect are timeTakenMs and isPublished.
   */
  useEffect(() => {
    // If plan insights is already visible, don't set the interval.
    if (isPublished) {
      return;
    }

    const intervalId = setInterval(() => {
      if (
        doesCommandExceedTimeout(
          PUBLISH_PLAN_INSIGHTS_TIMEOUT_MS,
          commandStartTimeMs,
          timeTakenMs,
        )
      ) {
        setIsNoticeVisibleFromTimer(true);

        clearInterval(intervalId);
      }
    }, PLAN_INSIGHTS_VISIBILITY_POLL_INTERVAL_MS);

    return () => {
      clearInterval(intervalId);
    };
  }, [commandStartTimeMs, timeTakenMs, isPublished]);

  useEffect(() => {
    const publishPlanInsights = (newPlanInsights: PlanInsights) => {
      updateDisplayState(
        commandOutput.historyId,
        commandResultIndex,
        (prevDisplayState) => ({
          ...prevDisplayState,
          planInsights: newPlanInsights,
        }),
      );
    };

    if (
      !isPublished &&
      isNoticeVisibleFromTimer &&
      // As long as each query isn't pending and fetching, we allow plan insights to be shown.
      // Even if they've errored or are disabled, we still want to show plan insights that weren't from fetch calls.
      !isBlockedDependenciesLoading &&
      !isNonRunningSourcesLoading
    ) {
      const { insights } = explainContext;

      const { imports, fast_path_clusters } = insights ?? {};

      const newPlanInsights = {
        imports: imports ?? {},
        fastPathClusters: fast_path_clusters ?? {},
        nonRunningSources: nonRunningSourcesData?.nonRunningSources ?? {},
        blockedDependencies: blockedDependenciesData?.blockedDependencies ?? {},
        cluster: {
          name: explainContext.cluster.name,
          id:
            "System" in explainContext.cluster.id
              ? `s${explainContext.cluster.id.System}`
              : `u${explainContext.cluster.id.User}`,
        },
        redactedSql: explainContext.redacted_sql,
      };

      publishPlanInsights(newPlanInsights);

      if (countVisiblePlanInsights(newPlanInsights) > 0) {
        // The button is shown when at least one insight is published.
        track("Insights Button Shown", {
          redactedSql: explainContext.redacted_sql,
          insights: planInsightsToInstrumentationIds(newPlanInsights),
        });
        // We know that the command exceeding the notice timeout is a new development. We scroll to the bottom then.
        scrollToBottom();
      }
    }
  }, [
    track, // Should not change
    scrollToBottom, // Should not change
    explainContext, // Should not change
    commandOutput.historyId, // Should not change
    commandResultIndex, // Should not change
    updateDisplayState, // Should not change
    isPublished, // Changes only once when publishPlanInsights is called
    isBlockedDependenciesLoading, // Changes during fetch
    blockedDependenciesData, // Changes after fetch success
    isNonRunningSourcesLoading, // Changes during fetch
    nonRunningSourcesData, // Changes after fetch success
    isNoticeVisibleFromTimer, // Changes when time threshold is exceeded to show plan insights
  ]);

  if (!isPublished) {
    return null;
  }

  const visibleInsightsCount = countVisiblePlanInsights(planInsights);

  if (visibleInsightsCount === 0) {
    return null;
  }

  return (
    <Button
      backgroundColor={colors.background.warn}
      borderColor={colors.border.warn}
      borderWidth="1px"
      _hover={{
        backgroundColor: colors.background.warn,
      }}
      _active={{
        backgroundColor: colors.background.warn,
        filter: "brightness(0.9)",
      }}
      size="sm"
      onClick={() => {
        track("Insights Button Clicked", {
          insights: planInsightsToInstrumentationIds(planInsights),
          redactedSql: planInsights.redactedSql,
        });

        setShellState((prevShellState) => ({
          ...prevShellState,
          currentPlanInsights: {
            historyId: commandOutput.historyId,
            commandResultIndex,
          },
        }));
      }}
    >
      Query Insights
      <CountSignifier backgroundColor={colors.accent.purple}>
        {visibleInsightsCount}
      </CountSignifier>
    </Button>
  );
};
export default PlanInsightsNotice;
