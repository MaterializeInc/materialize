// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, HStack, Spinner, Text, VStack } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";
import { useLocation } from "react-router";

import Alert from "~/components/Alert";
import { User } from "~/external-library-wrappers/frontegg";
import { useCreateApiToken } from "~/queries/frontegg";
import { currentRegionIdAtom } from "~/store/environments";

const DEFAULT_CLI_URL = "http://localhost:8808";

const MzCliAppPasswordPage = ({ user }: { user: User }) => {
  const { search } = useLocation();
  const searchParams = React.useMemo(
    () => new URLSearchParams(search),
    [search],
  );
  const [currentRegionId] = useAtom(currentRegionIdAtom);

  const email = user?.email;
  const tokenDescription =
    searchParams.get("tokenDescription") || "External tool token";
  const redirectUri = searchParams.get("redirectUri") || DEFAULT_CLI_URL;

  const {
    mutate: createAppPassword,
    error,
    isPending: createInProgress,
  } = useCreateApiToken({
    onSuccess: (newPassword) => {
      // Redirect to the CLI server after the token is created.
      const encodedSecret = encodeURIComponent(newPassword.secret);
      const encodedClientId = encodeURIComponent(newPassword.clientId);
      const encodedDescription = encodeURIComponent(tokenDescription);
      const encodedRegion = encodeURIComponent(currentRegionId);
      const url = `${redirectUri}/?secret=${encodedSecret}&clientId=${encodedClientId}&description=${encodedDescription}&email=${email}&region=${encodedRegion}`;

      window.location.assign(url);
    },
  });

  const onYesClick = () => {
    createAppPassword({
      type: "personal",
      description: tokenDescription,
    });
  };

  const onNoClick = () => {
    const url = `${redirectUri}/?secret=&clientId=&description=&email=&region=`;

    window.location.assign(url);
  };

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      height="100%"
      flex="1"
      alignContent="center"
    >
      <VStack textAlign="center" marginX="auto" marginTop="8%">
        {error && <Alert variant="error" message={error.message} mb="10" />}
        {createInProgress ? (
          <Spinner data-testid="loading-spinner" size="xl" />
        ) : (
          <>
            <Text fontSize="3xl">
              <b>You are about to create an app-password.</b>
            </Text>
            <Text fontSize="3xl">Do you wish to continue?</Text>
            <HStack spacing={20} paddingTop={20}>
              <Button colorScheme="purple" size="lg" onClick={onYesClick}>
                Yes
              </Button>
              <Button colorScheme="red" size="lg" onClick={onNoClick}>
                No
              </Button>
            </HStack>
          </>
        )}
      </VStack>
    </VStack>
  );
};

export default MzCliAppPasswordPage;
