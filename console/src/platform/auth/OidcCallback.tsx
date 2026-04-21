// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Heading, HStack, Text, VStack } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { LOGIN_PATH } from "~/api/materialize/auth";
import Alert from "~/components/Alert";
import LoadingScreen from "~/components/LoadingScreen";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import { useAuth } from "~/external-library-wrappers/oidc";
import { AuthContentContainer, AuthLayout } from "~/layouts/AuthLayout";

// Surfaces OIDC callback errors (access_denied, state mismatch, etc.)
export const OidcCallback = () => {
  const auth = useAuth();

  if (!auth.error) return <LoadingScreen />;

  return (
    <AuthLayout>
      <AuthContentContainer>
        <VStack alignItems="stretch" width="100%" mx="12">
          <HStack my={{ base: "8", lg: "0" }} paddingBottom="8">
            <MaterializeLogo height="12" />
          </HStack>
          <VStack alignItems="stretch" spacing="2" paddingBottom="6">
            <Heading size="md">Sign-in failed</Heading>
            <Text fontSize="sm">
              We couldn&apos;t complete your sign-in with your identity
              provider. If this keeps happening, confirm you&apos;ve been
              granted access to Materialize.
            </Text>
          </VStack>
          <Alert
            variant="error"
            message={auth.error.message || "Sign-in failed. Please try again."}
            showButton
            buttonText="Back to sign in"
            buttonProps={{ as: RouterLink, to: LOGIN_PATH, replace: true }}
          />
        </VStack>
      </AuthContentContainer>
    </AuthLayout>
  );
};
