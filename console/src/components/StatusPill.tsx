// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { WarningIcon } from "@chakra-ui/icons";
import { Box, BoxProps, Spinner } from "@chakra-ui/react";
import React from "react";

import { ConnectorStatus } from "~/api/materialize/types";
import { ConnectorStatusInfo, snapshotting } from "~/platform/connectors/utils";

export type StatusPillProps = BoxProps & {
  status: string;
  label?: string;
  colorScheme?: keyof typeof colorSchemeToStyles;
  icon?: React.ReactNode;
};

const getSourceColorScheme = (connector: ConnectorStatusInfo) => {
  if (snapshotting(connector)) {
    return "lightBlue";
  }
  return getConnectorColorScheme(connector.status as ConnectorStatus);
};

/** Gets the color scheme of source/sink status pills. */
const getConnectorColorScheme = (status: ConnectorStatus) => {
  switch (status) {
    case "created":
      return "blue";
    case "starting":
      return "blue";
    case "paused":
      return "blue";
    case "running":
      return "green";
    case "stalled":
      return "yellow";
    case "failed":
      return "red";
    case "dropped":
      return "gray";
  }
};

/** Gets the icon to display for a source/sink status pill. */
const getSourceIcon = (connector: ConnectorStatusInfo) => {
  if (snapshotting(connector)) {
    return <Spinner width="12px" height="12px" speed="0.75s" />;
  }
  switch (connector.status) {
    case "starting":
      return <Spinner width="12px" height="12px" speed="0.75s" />;
    case "failed":
      return <WarningIcon />;
    default:
      return null;
  }
};

const colorSchemeToStyles = {
  blue: {
    borderColor: undefined,
    backgroundColor: "#ECE5FF",
    textColor: "#1C1561",
  },
  lightBlue: {
    borderColor: "#9AC6F2",
    backgroundColor: "#DAEEFD",
    textColor: "#0D1116",
  },
  green: {
    borderColor: undefined,
    backgroundColor: "#DEF7E2",
    textColor: "#00471D",
  },
  yellow: {
    borderColor: undefined,
    backgroundColor: "#F5E8CF",
    textColor: "#8A5B01",
  },
  red: {
    borderColor: undefined,
    backgroundColor: "#F5D4D9",
    textColor: "#B80F25",
  },
  gray: {
    borderColor: undefined,
    backgroundColor: "#F7F7F8",
    textColor: "#736F7B",
  },
};

const getConnectorStatusLabel = (connector: ConnectorStatusInfo) => {
  if (snapshotting(connector)) {
    return "Snapshotting";
  }
  return connector.status.charAt(0).toUpperCase() + connector.status.slice(1);
};

export type ConnectorStatusPillProps = BoxProps & {
  connector: ConnectorStatusInfo;
};

export const ConnectorStatusPill = ({
  connector,
  ...props
}: ConnectorStatusPillProps) => {
  if (!connector.status) return null;

  return (
    <StatusPill
      status={connector.status}
      colorScheme={getSourceColorScheme(connector)}
      icon={getSourceIcon(connector)}
      label={getConnectorStatusLabel(connector)}
      {...props}
    />
  );
};

const StatusPill = ({
  status,
  label,
  backgroundColor,
  textColor,
  icon,
  colorScheme,
  ...boxProps
}: StatusPillProps) => {
  const styles = colorScheme
    ? colorSchemeToStyles[colorScheme]
    : { backgroundColor, borderColor: undefined, textColor };
  return (
    <Box
      display="flex"
      alignItems="center"
      gap="4px"
      borderRadius="40px"
      borderColor={styles.borderColor}
      borderWidth={styles.borderColor ? "1px" : undefined}
      paddingY="2px"
      paddingX="8px"
      textAlign="center"
      textStyle="text-small"
      fontWeight="500"
      backgroundColor={styles.backgroundColor}
      color={styles.textColor}
      width="fit-content"
      {...boxProps}
    >
      {icon}
      {label ? label : status.charAt(0).toUpperCase() + status.slice(1)}
    </Box>
  );
};

export default StatusPill;
