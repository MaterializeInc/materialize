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
  Grid,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import * as React from "react";
import { useState } from "react";

import IntegrationCard, {
  IntegrationStatus,
} from "~/components/IntegrationCard";
import SearchInput from "~/components/SearchInput";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";

import { IntegrationCategory } from "./integrationsList";

type Category = {
  label: string;
  slug: string;
};

const categories: Category[] = [
  {
    label: "All Integrations",
    slug: "all",
  },
  {
    label: "Developer Tools & SQL Clients",
    slug: "developer_tooling",
  },
  {
    label: "BI & Data Visualization",
    slug: "bi_data_viz",
  },
  {
    label: "Data Sources",
    slug: "data_sources",
  },
  {
    label: "Data Sinks",
    slug: "data_sinks",
  },
  {
    label: "SQL Drivers",
    slug: "sql_drivers",
  },
  {
    label: "Monitoring",
    slug: "monitoring",
  },
  {
    label: "Security",
    slug: "security",
  },
];

const IntegrationsGallery = ({
  integrations,
}: {
  integrations: IntegrationCategory[];
}) => {
  const { colors, colorMode } = useTheme<MaterializeTheme>();
  const [activeCategory, setActiveCategory] = useState<string>(
    categories[0].slug,
  );

  const [searchString, setSearchString] = useState<string>("");

  const filteredIntegrations = React.useMemo(() => {
    if (!integrations) return null;
    if (activeCategory === "all" && !searchString) return integrations;

    const category = integrations.filter(
      (integration) =>
        integration.key === activeCategory || activeCategory === "all",
    );

    const filtered = category.map((section) => {
      const children = section.children.filter(
        (integration) =>
          integration.name.toLowerCase().includes(searchString.toLowerCase()) ||
          integration.description
            .toLowerCase()
            .includes(searchString.toLowerCase()),
      );

      const results = { ...section, children };

      return results;
    });

    return filtered;
  }, [activeCategory, searchString, integrations]);

  // check if all integrations are filtered out
  const allIntegrationsFiltered = React.useMemo(
    () =>
      filteredIntegrations?.every(
        (section: IntegrationCategory) => section.children.length === 0,
      ),
    [filteredIntegrations],
  );

  const resetFilters = () => {
    setActiveCategory(categories[0].slug);
    setSearchString("");
  };

  return (
    <MainContentContainer height="100vh" isolation="isolate">
      <PageHeader>
        <PageHeading>Integrations</PageHeading>
        <SearchInput
          name="source"
          value={searchString}
          onChange={(e) => setSearchString(e.target.value)}
          placeholder="Search integrations"
          width={{ base: "100%", lg: "320px" }}
        />
      </PageHeader>
      <Grid
        templateColumns={{ base: "1fr", lg: "minmax(100px, 240px) 1fr" }}
        gap={10}
        position="relative"
        mt={6}
        pb={10}
        mr={{ base: 0, lg: 10 }}
      >
        <Flex align="start" flexDir="column" mb={{ base: 2, lg: 0 }}>
          <Text textStyle="heading-xs" mb={4} mt="6px">
            Categories
          </Text>
          <Flex
            align="start"
            flexDir={{ base: "row", lg: "column" }}
            wrap="wrap"
            gap={{ base: 2, lg: 0 }}
            px={0}
            w="100%"
          >
            {categories.map((category, i) => (
              <CategoryButton
                key={i}
                category={category}
                activeCategory={activeCategory}
                onClick={() => setActiveCategory(category.slug)}
              />
            ))}
          </Flex>
        </Flex>
        <Flex flexDir="column" gap={16} minH="100%">
          {allIntegrationsFiltered ? (
            <Flex
              flexDir="column"
              gap={2}
              justify="center"
              align="center"
              w="100%"
              h="100%"
            >
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                No integrations found for this category for the search term:{" "}
                {searchString}
              </Text>
              <Button
                variant="text-only"
                onClick={resetFilters}
                textStyle="text-ui-med"
                color={colors.accent.brightPurple}
              >
                Reset filters
              </Button>
            </Flex>
          ) : (
            filteredIntegrations?.map((section) =>
              section.children.length ? (
                <VStack
                  key={section.key}
                  align="start"
                  flexGrow="grow"
                  w="100%"
                  gap={4}
                  id={section.key}
                >
                  <Text textStyle="heading-md">{section.label}</Text>
                  <Grid
                    templateColumns="repeat(auto-fill, minmax(320px, 1fr))"
                    autoRows="1fr"
                    columnGap={6}
                    rowGap={10}
                    w="100%"
                  >
                    {section.children.map((integration) => {
                      let link = integration.link;
                      if (integration.sourceType) {
                        link = `../../sources/new/${integration.sourceType}`;
                      }
                      return (
                        <IntegrationCard
                          key={integration.name}
                          imagePath={
                            typeof integration.imagePath === "string"
                              ? integration.imagePath
                              : integration.imagePath(colorMode)
                          }
                          status={integration.status as IntegrationStatus}
                          name={integration.name}
                          description={integration.description}
                          link={link}
                        />
                      );
                    })}
                  </Grid>
                </VStack>
              ) : null,
            )
          )}
        </Flex>
      </Grid>
    </MainContentContainer>
  );
};

const CategoryButton = (
  {
    category,
    activeCategory,
    onClick,
  }: {
    category: Category;
    activeCategory: string;
    onClick: () => void;
  },
  props: any,
) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      {...props}
      as="button"
      userSelect="none"
      px={4}
      py={1}
      width={{ base: "fit-content", lg: "100%" }}
      rounded="md"
      textAlign="left"
      backgroundColor={
        activeCategory === category.slug ? colors.background.secondary : "none"
      }
      color={
        activeCategory === category.slug
          ? colors.foreground.primary
          : colors.foreground.secondary
      }
      transition="all 0.1s"
      _hover={{
        cursor: "pointer",
      }}
      onClick={onClick}
    >
      <Text textStyle="text-ui-med" py={1}>
        {category.label}
      </Text>
    </Box>
  );
};

export default IntegrationsGallery;
