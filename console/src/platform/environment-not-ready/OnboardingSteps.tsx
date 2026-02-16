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
  chakra,
  Flex,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import React from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import { User } from "~/external-library-wrappers/frontegg";
import SlackIcon from "~/img/slack.png";
import docUrls from "~/mz-doc-urls.json";
import { regionPath } from "~/platform/routeHelpers";
import { currentEnvironmentState, useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

import { TUTORIAL_KEYS, TUTORIAL_SLIDES } from "./constants";
import ContactSalesCta from "./ContactSalesCta";
import { TutorialKey } from "./types";
import { onboardingPath } from "./utils";

export const OnboardingSteps = ({ user }: { user: User }) => {
  const navigate = useNavigate();
  const { colors } = useTheme<MaterializeTheme>();
  const environment = useAtomValue(currentEnvironmentState);
  const { step } = useParams<{ step: TutorialKey }>();
  const { track } = useSegment();
  const regionSlug = useRegionSlug();
  assert(step);

  const tutorialIndex = TUTORIAL_KEYS.findIndex((k) => step === k);
  if (tutorialIndex === -1) {
    navigate("..");
    return;
  }
  const firstStep = tutorialIndex === 0;
  const lastStep = tutorialIndex === TUTORIAL_KEYS.length - 1;
  const slide = TUTORIAL_SLIDES[step];
  const nextSlide = TUTORIAL_KEYS[tutorialIndex + 1];
  const environmentReady =
    environment?.state === "enabled" && environment.status.health === "healthy";
  return (
    <Box width="100%" height="100%" position="relative" overflowX="hidden">
      <Flex
        alignItems="center"
        flexDir={{ base: "column-reverse", xl: "row" }}
        gap={{ base: 8, xl: 24 }}
        justifyContent={{
          base: "center",
          xl: "space-between",
        }}
        ml={{ base: "0", xl: "144px" }}
        position="absolute"
        top={{ base: "auto", xl: "50%" }}
        right={{ base: "auto", xl: "0" }}
        left={{ base: "auto", xl: "0" }}
        mt={{ base: "auto", xl: "-300px" }}
        width="100%"
      >
        <VStack
          alignItems="flex-start"
          justifyContent="space-between"
          px={{ base: 8, xl: 0 }}
          width="100%"
          spacing="0"
        >
          <Box minW="300px" height="280px">
            <Flex gap="2" marginBottom="10" h="2">
              {TUTORIAL_KEYS.map((key, i) => {
                const isActive = tutorialIndex === i;
                return (
                  <Box
                    key={key}
                    as={Link}
                    to={onboardingPath(TUTORIAL_KEYS[i])}
                    h="2"
                    w="2"
                    rounded="full"
                    background={
                      isActive
                        ? colors.accent.brightPurple
                        : colors.background.tertiary
                    }
                    transition="all .2s"
                  />
                );
              })}
            </Flex>
            <Text as="h2" textStyle="heading-lg">
              {slide.title}
            </Text>
            <Text
              my="4"
              fontSize="16px"
              lineHeight="28px"
              textStyle="text-base"
            >
              {slide.text}
            </Text>
            {firstStep && (
              <HStack spacing="10">
                <Button
                  as="a"
                  target="_blank"
                  rel="noreferrer"
                  variant="card"
                  size="lg"
                  maxWidth="200px"
                  href="https://materialize.com/s/chat"
                  onClick={() => {
                    track("Link Click", {
                      label: "Community Slack",
                      href: "https://materialize.com/s/chat",
                    });
                  }}
                >
                  <chakra.img height="6" width="6" src={SlackIcon} /> Join our
                  Slack community
                </Button>
                <Button
                  as="a"
                  target="_blank"
                  href={docUrls["/docs/"]}
                  variant="card"
                  size="lg"
                  maxWidth="200px"
                  onClick={() => {
                    track("Link Click", {
                      label: "Onboarding Docs CTA",
                      href: docUrls["/docs/"],
                    });
                  }}
                >
                  <MaterializeLogo markOnly />
                  Visit our documentation
                </Button>
              </HStack>
            )}
          </Box>
          <Text fontSize="xs" color="foreground.secondary" mb="2">
            {tutorialIndex === 0 ? "While you wait" : "Next"}:
          </Text>
          <HStack spacing="4" alignItems="center">
            <NextButton
              buttonText={slide.buttonText}
              lastStep={lastStep}
              environmentReady={environmentReady}
              nextSlideKey={nextSlide}
            />
            {environmentReady && !lastStep && (
              <Button
                as={Link}
                to={regionPath(regionSlug)}
                variant="primary"
                size="md"
              >
                Navigate to Console &rarr;
              </Button>
            )}
          </HStack>
          <ContactSalesCta mt="6" />
        </VStack>
        <Box
          as="img"
          src={slide.image.src}
          alt={slide.image.src}
          height="auto"
          maxHeight={{ base: "auto", lg: "400px", xl: "600px" }}
          mr={{ base: undefined, xl: "-44px" }}
          px={{ base: "8", xl: "0" }}
          width={{ base: "100%", lg: "auto", xl: "auto" }}
        />
      </Flex>
    </Box>
  );
};

const NextButton = (props: {
  buttonText: string;
  lastStep: boolean;
  environmentReady: boolean;
  nextSlideKey: TutorialKey;
}) => {
  const regionSlug = useRegionSlug();

  if (!props.lastStep) {
    return (
      <Button
        variant="primary"
        as={Link}
        to={onboardingPath(props.nextSlideKey)}
      >
        {props.buttonText} &rarr;
      </Button>
    );
  }
  if (props.environmentReady) {
    return (
      <Button as={Link} to={regionPath(regionSlug)} variant="primary">
        Open console &rarr;
      </Button>
    );
  }

  return (
    <Button variant="primary" isDisabled>
      <Spinner mr="2" size="sm" /> Open Console &rarr;
    </Button>
  );
};
