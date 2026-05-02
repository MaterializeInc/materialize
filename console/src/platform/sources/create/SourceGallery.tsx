// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Flex,
  Grid,
  GridItem,
  IconProps,
  Stack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useMemo, useState } from "react";

import IconNavLink from "~/components/IconNavLink";
import SearchInput from "~/components/SearchInput";
import { useMaxMySqlConnections } from "~/platform/connections/queries";
import ConfluentLogoIcon from "~/svg/ConfluentLogoIcon";
import KafkaLogoIcon from "~/svg/KafkaLogoIcon";
import MySqlLogoIcon from "~/svg/MySqlLogoIcon";
import PostgresLogoIcon from "~/svg/PostgresLogoIcon";
import SqlServerLogoIcon from "~/svg/SqlServerLogoIcon";
import WebhookIcon from "~/svg/WebhookIcon";
import { MaterializeTheme } from "~/theme";

type SourceEntry = {
  label: string;
  icon: (props: IconProps) => React.JSX.Element;
  to: string;
};

type SourceCategory = {
  label: string;
  description: string;
  sources: SourceEntry[];
};

const DATA_SYSTEMS: SourceCategory = {
  label: "Data systems",
  description:
    "Connect to your data Kafka stream, transactional database, or any other data system compatible with Kafka, PostgreSQL or MySQL.",
  sources: [
    {
      label: "PostgreSQL",
      icon: PostgresLogoIcon,
      to: "postgres",
    },
    {
      label: "SQL Server",
      icon: SqlServerLogoIcon,
      to: "sqlserver",
    },
    {
      label: "Apache Kafka",
      icon: KafkaLogoIcon,
      to: "kafka",
    },
    {
      label: "Confluent Cloud",
      icon: ConfluentLogoIcon,
      to: "kafka?kafkaConnectionType=confluent",
    },
  ],
};

const MYSQL_SOURCE_ENTRY: SourceEntry = {
  label: "MySQL",
  icon: MySqlLogoIcon,
  to: "mysql",
};

const SAAS_PLATFORMS: SourceCategory = {
  label: "SaaS Platforms",
  description:
    "Integrate your operational data from webhook-compatible SaaS platforms.",
  sources: [
    {
      label: "Webhook",
      icon: (props: IconProps) => <WebhookIcon {...props} />,
      to: "webhook",
    },
  ],
};

const FilteredSources = ({
  sources,
  labelFilter,
}: {
  sources: SourceEntry[];
  labelFilter: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const filtered = sources.filter((source) =>
    source.label.toLowerCase().includes(labelFilter.toLowerCase()),
  );
  return (
    <>
      {filtered.length > 0 ? (
        filtered.map((source) => (
          <GridItem key={source.label}>
            <IconNavLink
              icon={source.icon({ width: "6", height: "6" })}
              width="100%"
              to={source.to}
              label={source.label}
            />
          </GridItem>
        ))
      ) : (
        <GridItem gridColumn="1 / -1">
          <Text
            textStyle="text-ui-reg"
            fontStyle="italic"
            color={colors.foreground.secondary}
            textAlign="center"
          >
            No matching sources
          </Text>
        </GridItem>
      )}
    </>
  );
};

const SourceGallery = () => {
  const { data: maxMySqlConnections } = useMaxMySqlConnections();
  const mySqlEnabled = (maxMySqlConnections ?? 0) > 0;
  const [labelFilter, setLabelFilter] = useState("");
  const sourceCategories = useMemo(() => {
    if (!mySqlEnabled) return [DATA_SYSTEMS, SAAS_PLATFORMS];
    const preSQLSource = DATA_SYSTEMS.sources[0];
    // Need to insert MySQL source as a second source in the list. Can refactor to a static list if we add more OLTP sources that will have specific ordering requirements
    const restDataSystemSources = [
      MYSQL_SOURCE_ENTRY,
      ...DATA_SYSTEMS.sources.slice(1),
    ];
    const dataSystemSources = [preSQLSource, ...restDataSystemSources];

    return [{ ...DATA_SYSTEMS, sources: dataSystemSources }, SAAS_PLATFORMS];
  }, [mySqlEnabled]);

  return (
    <Flex justifyContent="center" height="100%" flex="1">
      <VStack
        mt="10"
        width="100%"
        maxWidth="896px"
        spacing="10"
        alignItems="stretch"
      >
        <Stack
          direction={{ base: "column", md: "row" }}
          justifyContent="space-between"
        >
          <Text as="h1" textStyle="heading-lg">
            Choose a source
          </Text>
          <SearchInput
            name="source"
            value={labelFilter}
            onChange={(e) => setLabelFilter(e.target.value)}
          />
        </Stack>
        <Grid
          gridTemplateColumns={{
            base: "1fr",
            sm: "1fr 1fr",
            md: "1fr 1fr 1fr",
          }}
          gridGap="6"
        >
          {sourceCategories.map((category) => (
            <React.Fragment key={category.label}>
              <GridItem gridColumn="1 / -1">
                <Text textStyle="heading-sm">{category.label}</Text>
                <Text textStyle="text-base">{category.description}</Text>
              </GridItem>
              <FilteredSources
                sources={category.sources}
                labelFilter={labelFilter}
              />
            </React.Fragment>
          ))}
        </Grid>
      </VStack>
    </Flex>
  );
};

export default SourceGallery;
