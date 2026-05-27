// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  Image,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { useEffect } from "react";
import { useNavigate } from "react-router-dom";

import { Modal } from "~/components/Modal";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import WelcomeHeaderImg from "~/img/welcome-banner.png";
import { newConnectionPath, shellPath } from "~/platform/routeHelpers";
import {
  setStoredSidebarVisibility,
  shellStateAtom,
} from "~/platform/shell/store/shell";
import {
  Environment,
  useEnvironmentsWithHealth,
  useRegionSlug,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { useWelcomeDialog } from "./useWelcomeDialog";

const WelcomeDialogContent = () => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const regionSlug = useRegionSlug();
  const environments = useEnvironmentsWithHealth();
  const navigate = useNavigate();
  const [showModal, setShowModal] = React.useState(false);
  const [welcomeDialogSeen, setWelcomeDialogSeen] = useWelcomeDialog();

  const [, setShellState] = useAtom(shellStateAtom);

  const hasEnabledRegion = [...environments.values()].some(
    (environment: Environment) =>
      environment.state == "enabled" && environment.status.health == "healthy",
  );

  useEffect(() => {
    if (!hasEnabledRegion) {
      setShowModal(false);
      return;
    }

    if (welcomeDialogSeen) {
      setShowModal(false);
      return;
    }

    setShowModal(true);
    return;
  }, [hasEnabledRegion, welcomeDialogSeen]);

  const dismissWelcomeDialog = () => {
    setWelcomeDialogSeen(true);
  };

  return (
    <>
      <Modal
        size="md"
        isCentered
        autoFocus={false}
        isOpen={showModal}
        onClose={dismissWelcomeDialog}
      >
        <ModalOverlay />
        <ModalContent overflow="hidden" shadow={shadows.level4}>
          <Image />
          <ModalHeader fontWeight="500" hidden>
            Welcome to Materialize
          </ModalHeader>
          <ModalCloseButton
            color="#ffffff"
            sx={{ _hover: { backgroundColor: "transparent" } }}
            data-testid="welcome-dialog-close-button"
          />
          <Image src={WelcomeHeaderImg} />
          <ModalBody p="6" alignItems="stretch">
            <VStack align="flex-start" spacing={4}>
              <Text as="h2" textStyle="heading-md">
                Welcome to Materialize
              </Text>
              <Text textStyle="text-base" color={colors.foreground.secondary}>
                Get started quickly with one of the following options:
              </Text>
              <VStack mt={4} spacing={4} alignItems="flex-start">
                <Text as="h3" textStyle="heading-sm">
                  Quickstart (Recommended)
                </Text>
                <Text textStyle="text-base" color={colors.foreground.secondary}>
                  Learn the basics and get familiar with Materialize.
                </Text>
                <Text as="h3" textStyle="heading-sm">
                  Connect a data source
                </Text>
                <Text textStyle="text-base" color={colors.foreground.secondary}>
                  Start developing with your own data.
                </Text>
              </VStack>
            </VStack>
          </ModalBody>
          <ModalFooter borderTop="none" p="6" gap={4}>
            <Button
              variant="primary"
              onClick={() => {
                navigate(newConnectionPath(regionSlug));
                dismissWelcomeDialog();
              }}
            >
              Connect Data
            </Button>
            <Button
              variant="primary"
              onClick={() => {
                navigate(shellPath(regionSlug));
                dismissWelcomeDialog();
                setShellState((prevState) => ({
                  ...prevState,
                  tutorialVisible: true,
                }));
                setStoredSidebarVisibility(true);
              }}
            >
              Begin Quickstart
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </>
  );
};

export const WelcomeDialog = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) =>
        runtimeConfig.isImpersonating ? null : <WelcomeDialogContent />
      }
    />
  );
};

export default WelcomeDialog;
