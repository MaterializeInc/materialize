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
  GridItem,
  GridItemProps,
  HStack,
  StackProps,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { forwardRef, useRef } from "react";
import { Link } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import ScheduleDemoLink from "~/components/ScheduleDemoLink";
import { newConnectionPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { academyStepsData } from "./academySteps";
import { quickstartStepsData } from "./quickstartSteps";
import { shellStateAtom } from "./store/shell";
import { TUTORIAL_SHORT_LABELS, TutorialId } from "./store/tutorialIds";
import { StepData, StepLayout } from "./tutorialUtils";

/* Semantic version number used for instrumentation */
const QUICKSTART_VERSION = "2.0.0";

const TUTORIALS: Record<TutorialId, StepData[]> = {
  quickstart: quickstartStepsData,
  academy: academyStepsData,
};

/**
 * Since many steps have the same title, this gets the xth step of a given title
 */
const getLocalTutorialStepByTitle = (
  stepsData: StepData[],
  globalTutorialStep: number,
  title: string,
): number => {
  const startIndexByTitle = stepsData.reduce<{ [title: string]: number }>(
    (accum, step, index) => {
      if (accum[step.title] === undefined) {
        accum[step.title] = index;
      }
      return accum;
    },
    {},
  );

  if (Object.keys(startIndexByTitle).length === 0) {
    return 0;
  }

  return globalTutorialStep - startIndexByTitle[title];
};

const Steps = forwardRef(
  (
    {
      stepsData,
      runCommand,
      onChangeStep,
    }: {
      stepsData: StepData[];
      runCommand: (command: string) => void;
      onChangeStep: (desired: number) => void;
    },
    ref,
  ) => {
    const [shellState] = useAtom(shellStateAtom);
    const { currentTutorialStep } = shellState;
    const { colors } = useTheme<MaterializeTheme>();

    const regionSlug = useRegionSlug();

    const atStart = currentTutorialStep === 0;

    const atEnd = currentTutorialStep >= stepsData.length - 1;

    const safeStepIndex = Math.min(currentTutorialStep, stepsData.length - 1);
    const step = stepsData[safeStepIndex];

    return (
      <StepLayout width="100%" ref={ref}>
        <step.render
          runCommand={runCommand}
          title={step.title}
          colors={colors}
        />
        <HStack
          width="100%"
          alignSelf="flex-end"
          alignItems="space-between"
          justifyContent={atStart ? "flex-end" : "space-between"}
          marginTop="4"
        >
          {!atStart && (
            <Button
              onClick={() => onChangeStep(currentTutorialStep - 1)}
              variant="tertiary"
            >
              Back
            </Button>
          )}
          {atEnd ? (
            <HStack gap={4}>
              <ScheduleDemoLink>
                <Button variant="primary">Talk to us</Button>
              </ScheduleDemoLink>
              <Button
                as={Link}
                variant="primary"
                to={`${newConnectionPath(regionSlug)}`}
              >
                Connect data
              </Button>
            </HStack>
          ) : (
            <Button
              onClick={() => onChangeStep(currentTutorialStep + 1)}
              variant="primary"
            >
              Continue
            </Button>
          )}
        </HStack>
      </StepLayout>
    );
  },
);

type ProgressProps = {
  min: number;
  max: number;
  value: number;
  onStepClick: (step: number) => void;
} & StackProps;

const PROGRESS_STEP_SPACING = 1;

const Progress = ({ min, max, value, onStepClick, ...rest }: ProgressProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const steps: boolean[] = [];

  for (let i = min; i < max; i++) {
    steps.push(i <= value);
  }

  return (
    <HStack {...rest} spacing={PROGRESS_STEP_SPACING}>
      {steps.map((filled, idx) => {
        return (
          <Box
            key={idx}
            flexGrow="1"
            height="1"
            borderRadius="2px"
            transition="all 0.2s"
            bgColor={filled ? colors.accent.purple : colors.background.tertiary}
            onClick={() => onStepClick(idx)}
            cursor={value !== idx ? "pointer" : "auto"}
            title={value !== idx ? `Jump to step ${idx + 1}` : ""}
            sx={{
              _hover: {
                backgroundColor: colors.accent.purple,
                boxShadow:
                  value !== idx
                    ? `0px 0px 3px 1px ${colors.accent.purple}`
                    : "none",
              },
            }}
          />
        );
      })}
    </HStack>
  );
};

type TutorialProps = GridItemProps & {
  runCommand: (command: string) => void;
};

const Tutorial = ({ runCommand, ...rest }: TutorialProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [shellState, setShellState] = useAtom(shellStateAtom);
  const { currentTutorialStep: step, activeTutorial } = shellState;
  const { track } = useSegment();
  const stepsScrollContainerRef = useRef<HTMLDivElement>(null);

  const stepsData = TUTORIALS[activeTutorial];

  const changeStep = (desired: number) => {
    if (desired < 0) {
      desired = 0;
    } else if (desired >= stepsData.length) {
      desired = stepsData.length - 1;
    }
    const prevStep = step;

    setShellState((prev) => ({ ...prev, currentTutorialStep: desired }));

    const title = stepsData[desired].title;

    track("Tutorial change page", {
      pageHeader: title,
      pageNumber: desired,
      sectionPageNumber: getLocalTutorialStepByTitle(stepsData, desired, title),
      quickstartVersion: QUICKSTART_VERSION,
      tutorial: activeTutorial,
    });

    const isChangingToSecondStep = prevStep === 0 && desired === 1;
    if (isChangingToSecondStep) {
      // If a user goes from the first page to the next page, we use this as a
      // signal that the user has intended to start the tutorial.
      track("Quickstart Start", {
        quickstartVersion: QUICKSTART_VERSION,
        tutorial: activeTutorial,
      });
    }

    const isChangingToLastStep =
      prevStep === stepsData.length - 2 && desired === stepsData.length - 1;
    if (isChangingToLastStep) {
      // If a user goes from the second last page to the last page, we use this
      // as a signal that the user has completed the tutorial.
      track("Quickstart End", {
        quickstartVersion: QUICKSTART_VERSION,
        tutorial: activeTutorial,
      });
    }

    stepsScrollContainerRef.current?.scrollTo({ top: 0 });
  };

  return (
    <GridItem
      area="tutorial"
      borderLeftWidth="1px"
      borderColor={colors.border.secondary}
      bg={colors.background.shellTutorial}
      borderBottomRightRadius="lg"
      display="flex"
      flexDirection="column"
      {...rest}
    >
      <VStack
        paddingTop="6"
        spacing="0"
        alignItems="flex-start"
        minHeight="0"
        flex="1"
      >
        <VStack spacing="6" alignItems="flex-start" width="100%" paddingX="10">
          <Progress
            min={0}
            max={stepsData.length}
            value={step}
            width="100%"
            onStepClick={changeStep}
          />
          <Text
            textStyle="text-small"
            fontWeight="500"
            color={colors.foreground.secondary}
            marginBottom="4"
          >
            {TUTORIAL_SHORT_LABELS[activeTutorial].toUpperCase()}
          </Text>
        </VStack>
        <Steps
          stepsData={stepsData}
          runCommand={runCommand}
          onChangeStep={changeStep}
          ref={stepsScrollContainerRef}
        />
      </VStack>
    </GridItem>
  );
};

export default Tutorial;
