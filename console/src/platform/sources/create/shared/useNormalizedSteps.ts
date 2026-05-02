// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSteps } from "@chakra-ui/react";
import { useCallback } from "react";

import { WizardStep } from "~/components/formComponentsV2";

export default function useNormalizedSteps({
  initialSteps = [],
  sourceSteps,
  indexOffset,
}: {
  initialSteps?: WizardStep[];
  sourceSteps: WizardStep[];
  indexOffset?: number;
}) {
  const wizardSteps = useSteps({
    index: initialSteps.length + (indexOffset ?? 0),
    count: sourceSteps.length,
  });
  const { setActiveStep } = wizardSteps;

  // Determine if the active step matches the corresponding index of the route-dependent steps.
  const isNormalizedStep = (stepIdx: number): boolean => {
    const activeStep = wizardSteps.activeStep - initialSteps.length;
    return activeStep === stepIdx;
  };

  const setNormalizedActiveStep = useCallback(
    (stepIdx: number): void => {
      const targetIdx = initialSteps.length + stepIdx;
      return setActiveStep(targetIdx);
    },
    [setActiveStep, initialSteps],
  );

  return {
    isNormalizedStep,
    setNormalizedActiveStep,
    steps: [...initialSteps, ...sourceSteps],
    wizardSteps,
  };
}
