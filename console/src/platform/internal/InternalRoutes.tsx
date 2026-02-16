// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, VStack } from "@chakra-ui/layout";
import React from "react";
import { Route } from "react-router-dom";

import {
  BaseLayout,
  MainContentContainer,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  NavBarContainer,
  NavBarEnvironmentSelect,
  NavBarHeader,
  NavMenuContainer,
} from "~/layouts/NavBar";
import { NavItem } from "~/layouts/NavBar/NavItem";
import { SentryRoutes } from "~/sentry";

import Notices from "./notices/NoticeRoutes";

const InternalAppIndex = () => {
  return (
    <MainContentContainer>
      <VStack align="start" spacing="4">
        <PageHeading>Internal Console Apps</PageHeading>
        <Text textStyle="text-ui-reg">
          These are internal only features not intendend for customers.
        </Text>
      </VStack>
    </MainContentContainer>
  );
};

const DemoApp = () => {
  return <MainContentContainer>Demo app</MainContentContainer>;
};

const InternalNavBar = () => {
  return (
    <NavBarContainer>
      <NavBarHeader />
      <NavBarEnvironmentSelect />
      <NavMenuContainer>
        <VStack spacing="1">
          <NavItem label="Demo" href="demo" />
          <NavItem label="Notices" href="notices" />
        </VStack>
      </NavMenuContainer>
    </NavBarContainer>
  );
};
const InternalRoutes = () => {
  return (
    <BaseLayout navBarOverride={InternalNavBar}>
      <SentryRoutes>
        <Route index element={<InternalAppIndex />} />
        <Route path="demo" element={<DemoApp />} />
        <Route path="notices" element={<Notices />} />
        <Route path="notices">
          <Route index path="*" element={<Notices />} />
        </Route>
      </SentryRoutes>
    </BaseLayout>
  );
};

export default InternalRoutes;
