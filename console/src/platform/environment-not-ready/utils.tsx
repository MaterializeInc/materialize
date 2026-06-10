// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Spinner } from "@chakra-ui/react";
import React from "react";

import { Environment } from "~/store/environments";
import CheckmarkIcon from "~/svg/CheckmarkIcon";
import { MaterializeTheme } from "~/theme";

import { TutorialKey } from "./types";

export const getBorderColor = (
  colors: MaterializeTheme["colors"],
  environment: Environment,
) => {
  if (
    environment.state === "enabled" &&
    environment.status.health === "healthy"
  ) {
    return "#7EE7AB";
  }
  return colors.border.info;
};

export const getBackgroundColor = (
  colors: MaterializeTheme["colors"],
  environment: Environment,
) => {
  if (
    environment.state === "enabled" &&
    environment.status.health === "healthy"
  ) {
    return "#CEFDE2";
  }
  return colors.background.info;
};

export const getTextColor = (
  colors: MaterializeTheme["colors"],
  environment: Environment,
) => {
  if (
    environment.state === "enabled" &&
    environment.status.health === "healthy"
  ) {
    return colors.gray[900];
  }
  return colors.foreground.primary;
};

export const getIcon = (environment: Environment) => {
  if (environment.state === "disabled") {
    return (
      <Box
        height="4"
        width="4"
        backgroundColor="gray.400"
        borderRadius="10px"
        flexShrink="0"
      />
    );
  }
  if (
    environment.state === "enabled" &&
    environment.status.health === "healthy"
  ) {
    return <CheckmarkIcon color="black" />;
  }
  return <Spinner width="12px" height="12px" speed="0.75s" />;
};

export const getText = (environment: Environment, regionId: string) => {
  if (environment.state === "disabled") {
    return `${regionId} is disabled`;
  }
  if (
    environment.state === "enabled" &&
    environment.status.health === "healthy"
  ) {
    return `${regionId} is ready`;
  }
  return `Starting ${regionId}`;
};

export function onboardingPath(step: TutorialKey) {
  return `../${step}`;
}
