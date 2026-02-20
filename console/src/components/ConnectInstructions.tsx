// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BoxProps, Grid, Spinner, Text } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";
import { useParams } from "react-router-dom";

import { User } from "~/external-library-wrappers/frontegg";
import { ClusterDetailParams } from "~/platform/clusters/ClusterRoutes";
import { currentEnvironmentState } from "~/store/environments";
import { MonitorIcon } from "~/svg/Monitor";
import TerminalIcon from "~/svg/Terminal";

import { CopyableBox, TabbedCodeBlock } from "./copyableComponents";

const ConnectInstructions = ({
  user,
  ...props
}: BoxProps & { userStr: string | undefined; user: User }): JSX.Element => {
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const { clusterName } = useParams<ClusterDetailParams>();

  if (!currentEnvironment || currentEnvironment.state !== "enabled") {
    return <Spinner />;
  }

  const userStr = props.userStr || user.email;

  const environmentdAddress = currentEnvironment.sqlAddress;

  const defaultClusterOptionString = clusterName
    ? `&options=--cluster%3D${clusterName}`
    : "";

  // NOTE(benesch): We'd like to use `sslmode=verify-full` to prevent MITM
  // attacks, but that mode requires specifying `sslrootcert=/path/to/cabundle`,
  // and that path varies by platform. So instead we use `require`, which is
  // at least better than the default of `prefer`.
  const psqlCopyString = `psql "postgres://${encodeURIComponent(
    userStr,
  )}@${environmentdAddress}/materialize?sslmode=require${defaultClusterOptionString}"`;

  const [host, port] = environmentdAddress.split(":");
  return (
    <TabbedCodeBlock
      data-testid="connection-options"
      lineNumbers
      tabs={[
        {
          title: "External tools",
          children: (
            <Grid
              p="6"
              rowGap="6"
              templateColumns="min-content 540px"
              justifyContent="space-between"
              alignItems="center"
              width="100%"
            >
              <Text textStyle="heading-xs">Host</Text>
              <CopyableBox
                variant="compact"
                contents={host}
                aria-label="Host"
              />
              <Text textStyle="heading-xs">Port</Text>
              <CopyableBox
                variant="compact"
                contents={port}
                aria-label="Port"
              />
              <Text textStyle="heading-xs">Database</Text>
              <CopyableBox
                variant="compact"
                contents="materialize"
                aria-label="Database"
              />
              <Text textStyle="heading-xs">User</Text>
              <CopyableBox
                variant="compact"
                contents={userStr}
                aria-label="User"
              />
            </Grid>
          ),
          icon: <MonitorIcon w="4" h="4" />,
        },
        {
          title: "Terminal",
          contents: psqlCopyString,
          icon: <TerminalIcon w="4" h="4" />,
        },
      ]}
      minHeight="208px"
      {...props}
    />
  );
};

export default ConnectInstructions;
