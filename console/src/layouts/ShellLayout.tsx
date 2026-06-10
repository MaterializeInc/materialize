// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex } from "@chakra-ui/react";
import React, { PropsWithChildren } from "react";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";

type ShellLayoutProps = PropsWithChildren;

/**
 * The layout for shell, containing the navigation bar
 * and a sticky footer.
 *
 */
export const ShellLayout = (props: ShellLayoutProps) => {
  return (
    <AppErrorBoundary>
      <Flex id="shell" flex="1" minHeight="0" position="relative">
        {props.children}
      </Flex>
    </AppErrorBoundary>
  );
};
