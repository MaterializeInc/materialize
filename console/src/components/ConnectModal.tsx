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
  Flex,
  HStack,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import ConnectInstructions from "~/components/ConnectInstructions";
import { Modal } from "~/components/Modal";
import { User } from "~/external-library-wrappers/frontegg";
import docUrls from "~/mz-doc-urls.json";
import { useCreateApiToken } from "~/queries/frontegg";
import { useListApiTokens } from "~/queries/frontegg";
import { MaterializeTheme } from "~/theme";

import { SecretCopyableBox } from "./copyableComponents";
import SupportLink from "./SupportLink";
import TextLink from "./TextLink";

export const NEW_USER_DEFAULT_PASSWORD_NAME = "App password";

/**
 * A modal that displays Materialize connection instructions
 */
const ConnectModal = ({
  onClose,
  isOpen,
  forAppPassword,
  user,
}: {
  onClose: () => void;
  isOpen: boolean;
  forAppPassword?: {
    user: string;
  };
  user: User;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const showCreateAppPassword = !forAppPassword;

  return (
    <Modal size="3xl" isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader fontWeight="500">Connect To Materialize</ModalHeader>
        <ModalCloseButton />
        <ModalBody pt="2" pb="6" alignItems="stretch">
          <Text
            fontSize="sm"
            whiteSpace="normal"
            color={colors.foreground.secondary}
          >
            Below are the details to connect to Materialize
            {forAppPassword && " with this app password"}. If you need more
            information you can{" "}
            <TextLink
              href={docUrls["/docs/integrations/sql-clients/"]}
              target="_blank"
            >
              view the documentation
            </TextLink>{" "}
            or <SupportLink variant="brandColor">Contact Support</SupportLink>.
          </Text>
          <ConnectInstructions
            user={user}
            userStr={forAppPassword?.user}
            mt="4"
          />
          {showCreateAppPassword && (
            <Box mt="6">
              <CreateAppPassword user={user} />
            </Box>
          )}
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

const CreateAppPassword = ({ user }: { user: User }) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <React.Suspense
      fallback={
        <Flex alignItems="center" color={colors.foreground.secondary}>
          <Spinner size="sm" mr={2} /> Loading...
        </Flex>
      }
    >
      <CreateAppPasswordInner user={user} />
    </React.Suspense>
  );
};

const CreateAppPasswordInner = ({ user }: { user: User }) => {
  const { data: appPasswords } = useListApiTokens({ user });
  const { colors } = useTheme<MaterializeTheme>();
  const {
    mutate: createAppPassword,
    isPending: createInProgress,
    data: newPassword,
  } = useCreateApiToken();

  if (createInProgress) {
    return (
      <Flex alignItems="center" color={colors.foreground.secondary}>
        <Spinner size="sm" mr={2} />
        <Text fontSize="sm">Generating new app password...</Text>
      </Flex>
    );
  }

  if (newPassword?.password) {
    return (
      <>
        <VStack alignItems="stretch">
          <Text
            as="span"
            fontSize="sm"
            lineHeight="16px"
            fontWeight={500}
            color={colors.foreground.primary}
          >
            New app password
          </Text>
          <SecretCopyableBox
            label="clientId"
            contents={newPassword.password}
            obfuscatedContent={newPassword.obfuscatedPassword}
          />
        </VStack>
        <Text
          pt={1}
          fontSize="sm"
          lineHeight="20px"
          fontWeight={400}
          color={colors.foreground.secondary}
        >
          Copy this app password to somewhere safe. App passwords cannot be
          displayed after initial creation.
        </Text>
      </>
    );
  }

  return (
    <>
      <HStack justifyContent="space-between">
        <Box>
          <Text fontSize="sm" fontWeight="500">
            Create an app password
          </Text>
          <Text fontSize="sm" color={colors.foreground.secondary}>
            Create a new app password if you don’t have one accessible.
          </Text>
        </Box>
        <Button
          onClick={() =>
            createAppPassword({
              type: "personal",
              description: `${NEW_USER_DEFAULT_PASSWORD_NAME} ${
                appPasswords.length + 1
              }`,
            })
          }
          disabled={!!(newPassword || createInProgress)}
          variant="primary"
          size="sm"
        >
          Create app password
        </Button>
      </HStack>
    </>
  );
};

export default ConnectModal;
