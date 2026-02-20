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
  BoxProps,
  Button,
  ChakraTheme,
  Code,
  GridItem,
  GridItemProps,
  HStack,
  IconButton,
  ListItem,
  OrderedList,
  StackProps,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  UnorderedList,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import React, { forwardRef, PropsWithChildren, useRef } from "react";
import { Link } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import { TabbedCodeBlock } from "~/components/copyableComponents";
import ScheduleDemoLink from "~/components/ScheduleDemoLink";
import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";
import { newConnectionPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import CommandIcon from "~/svg/CommandIcon";
import { MaterializeTheme, ThemeColors } from "~/theme";
import ColorsType from "~/theme/colors";

import { saveClearPrompt, setPromptValue } from "./store/prompt";
import { shellStateAtom } from "./store/shell";
import TutorialInsertionWidget from "./TutorialInsertionWidget";
import TutorialSchemaWidget from "./TutorialSchemaWidget";

/* Semantic version number used for instrumentation */
const QUICKSTART_VERSION = "2.0.0";

type RunnableProps = {
  runCommand: (value: string) => void;
  title: string;
  value: string;
};

const Runnable = ({ runCommand, value, title }: RunnableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const setPrompt = useAtomCallback(setPromptValue);
  const clearPrompt = useAtomCallback(saveClearPrompt);

  return (
    <TabbedCodeBlock
      width="100%"
      flexShrink="0"
      tabs={[{ title, contents: value }]}
      lineNumbers
      headingIcon={
        <IconButton
          aria-label="Run command button"
          title="Run command"
          onClick={() => {
            setPrompt(value);
            runCommand(value);
            clearPrompt();
          }}
          icon={<CommandIcon color={colors.foreground.secondary} />}
          variant="unstyled"
          rounded={0}
          sx={{
            _hover: {
              background: "rgba(255, 255, 255, 0.06)",
            },
          }}
        />
      }
    />
  );
};

const TextContainer = ({ children }: PropsWithChildren) => (
  <VStack spacing="4" alignItems="flex-start">
    {children}
  </VStack>
);

const RunnableContainer = ({
  children,
  ...rest
}: PropsWithChildren & BoxProps) => (
  <VStack spacing="6" {...rest}>
    {children}
  </VStack>
);

const StepLayout = forwardRef(
  ({ children, ...rest }: PropsWithChildren & BoxProps, ref) => (
    <VStack
      spacing="6"
      alignItems="stretch"
      overflow="auto"
      flexGrow="1"
      minHeight="0"
      paddingBottom="6"
      paddingX="10"
      ref={ref}
      {...rest}
    >
      {children}
    </VStack>
  ),
);

type StepProps = {
  runCommand: (value: string) => void;
  title: string;
  colors: ChakraTheme["colors"] & ThemeColors & typeof ColorsType;
};

const stepsData: Array<{
  title: string;
  render: (props: StepProps) => JSX.Element;
}> = [
  {
    title: "Introduction",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            Materialize provides always-fresh results while also providing
            strong consistency guarantees. In Materialize, both{" "}
            <TextLink target="_blank" href={docUrls["/docs/concepts/indexes/"]}>
              indexes on views
            </TextLink>{" "}
            and{" "}
            <TextLink
              target="_blank"
              href={`${docUrls["/docs/concepts/views/"]}#materialized-views`}
            >
              materialized views
            </TextLink>{" "}
            incrementally update results when Materialize ingests new data,
            meaning that work is performed on writes.
          </Text>
          <Text textStyle="text-base">In the quickstart, you will:</Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              Create and query various views on sample auction data. The data is
              continually generated at 1 second intervals to mimic a
              data-intensive workload.
            </ListItem>
            <ListItem textStyle="text-base">
              Create an index on a view to compute and store view results in
              memory. As new auction data arrives, the index incrementally
              updates view results.
            </ListItem>
            <ListItem textStyle="text-base">
              Create and query views to verify that Materialize always serves
              consistent results.
            </ListItem>
          </UnorderedList>
        </TextContainer>
      </>
    ),
  },
  {
    title: "Create a schema.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            By default, you are working in the{" "}
            <Code variant="inline-syntax">materialize.public</Code>{" "}
            <TextLink target="_blank" href={docUrls["/docs/sql/namespaces/"]}>
              namespace
            </TextLink>{" "}
            where:
          </Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <Code variant="inline-syntax">materialize</Code> is the database
              name and
            </ListItem>
            <ListItem textStyle="text-base">
              <Code variant="inline-syntax">public</Code> is the schema name.
            </ListItem>
          </UnorderedList>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            <Text>
              Create a separate schema for this quickstart. For a schema name to
              be valid:
            </Text>
            <UnorderedList>
              <ListItem textStyle="text-base">
                The first character must be either: an ASCII letter (a-z and
                A-Z), an underscore (_), or a non-ASCII character.
              </ListItem>
              <ListItem textStyle="text-base">
                The remaining characters can be: an ASCII letter (a-z and A-Z),
                ASCII numbers (0-9), an underscore (_), dollar signs ($), or a
                non-ASCII character.
              </ListItem>
            </UnorderedList>
            <Text>
              Alternatively, by double-quoting the name, you can bypass the
              aforementioned constraints with the following exception: schema
              names, whether double-quoted or not, cannot contain the dot (
              <Code variant="inline-syntax">.</Code>) See also{" "}
              <TextLink
                target="_blank"
                href={`${docUrls["/docs/sql/identifiers/"]}#naming-restrictions`}
              >
                Identifier: Naming restrictions
              </TextLink>{" "}
            </Text>

            <Box width="100%">
              <TutorialSchemaWidget />
            </Box>
          </ListItem>
          <ListItem textStyle="text-base">
            Switch to the new schema. From the top of the SQL Shell, select your
            schema from the namespace dropdown.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Create the source.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            <TextLink target="_blank" href={docUrls["/docs/concepts/sources/"]}>
              Sources
            </TextLink>{" "}
            are external systems from which Materialize reads in data. This
            tutorial uses Materialize&apos;s sample{" "}
            <TextLink
              target="_blank"
              href={`${docUrls["/docs/sql/create-source/load-generator/"]}#auction`}
            >
              Auction load generator
            </TextLink>{" "}
            to create the sources.
          </Text>
        </TextContainer>

        <OrderedList spacing={3}>
          <ListItem>
            Create the source using the{" "}
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-source/"]}
              >
                CREATE SOURCE
              </TextLink>
            </Code>{" "}
            command. For the sample load generator, the quickstart uses{" "}
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-source/"]}
              >
                CREATE SOURCE
              </TextLink>
            </Code>{" "}
            with the <Code variant="inline-syntax">FROM LOAD GENERATOR</Code>{" "}
            clause that works specifically with Materialize&apos;s sample data
            generators. The tutorial specifies that the generator should emit
            new data every 1s.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '1s', AS OF 100000)
  FOR ALL TABLES;"
                title="Create the sources."
              />
            </RunnableContainer>
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-source/"]}
              >
                CREATE SOURCE
              </TextLink>
            </Code>{" "}
            can create multiple tables (referred to as
            <Code variant="inline-syntax">subsources</Code> in Materialize) when
            ingesting data from multiple upstream tables. For each upstream
            table that is selected for ingestion, Materialize creates a
            subsource.
          </ListItem>
          <ListItem>
            Use{" "}
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/show-sources/"]}
              >
                SHOW SOURCES
              </TextLink>
            </Code>{" "}
            command to see the results of the previous step.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="SHOW SOURCES;"
                title="See the results of the previous step."
              />
            </RunnableContainer>
            A subsource is how Materialize refers to a table that has the
            following properties:
            <UnorderedList>
              <ListItem>
                A subsource can only be written by the source; in this case, the
                load-generator.
              </ListItem>
              <ListItem>Users can read from subsources.</ListItem>
            </UnorderedList>
          </ListItem>

          <ListItem>
            Use the{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/select/"]}>
                SELECT
              </TextLink>
            </Code>{" "}
            statement to query <Code variant="inline-syntax">auctions</Code> and{" "}
            <Code variant="inline-syntax">bids</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="SELECT * FROM auctions LIMIT 1;"
                title="View a sample row in auctions."
              />
              <Runnable
                runCommand={runCommand}
                value="SELECT * FROM bids LIMIT 1;"
                title="View a sample row in bids."
              />
              <Runnable
                runCommand={runCommand}
                value="SELECT a.*, b.*
FROM auctions AS a
JOIN bids AS b
  ON a.id = b.auction_id
LIMIT 3;"
                title="View the relationship between auctions and bids."
              />
            </RunnableContainer>
            <TextContainer>
              <Text textStyle="text-base">
                Subsequent steps in this quickstart uses a query to find winning
                bids for auctions to show how Materialize uses views and indexes
                to provide immediately available up-to-date results for various
                queries.
              </Text>
            </TextContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Create a view to find winning bids.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            A{" "}
            <TextLink target="_blank" href={docUrls["/docs/concepts/views/"]}>
              view
            </TextLink>{" "}
            is a saved name for the underlying{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/select/"]}>
                SELECT{" "}
              </TextLink>
            </Code>{" "}
            statement, providing an alias/shorthand when referencing the query.
            The underlying query is not executed during the view creation;
            instead, the underlying query is executed when the view is
            referenced.
          </Text>
          <Text textStyle="text-base">
            Assume you want to find the winning bids for auctions that have
            ended. The winning bid for an auction is the highest bid entered for
            an auction before the auction ended. As new auction and bid data
            appears, the query must be rerun to get up-to-date results.
          </Text>
        </TextContainer>

        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Use{" "}
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-view/"]}
              >
                CREATE VIEW
              </TextLink>
            </Code>{" "}
            to create a view for the winning bids. Materialize provides an
            idiomatic way to perform{" "}
            <TextLink
              target="_blank"
              href={
                docUrls["/docs/transform-data/idiomatic-materialize-sql/top-k/"]
              }
            >
              Top-K queries
            </TextLink>
            . For K = 1, the idiomatic Materialize SQL uses
            <Code variant="inline-syntax">
              {" "}
              <TextLink
                target="_blank"
                href={`${
                  docUrls[
                    "/docs/transform-data/idiomatic-materialize-sql/top-k/"
                  ]
                }#for-k--1-1`}
              >
                DISTINCT ON (...)
              </TextLink>
            </Code>{" "}
            to return only the first row for each distinct auction id.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW winning_bids AS
  SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
  FROM auctions AS a
   JOIN bids AS b
     ON a.id = b.auction_id
  WHERE b.bid_time < a.end_time
    AND mz_now() >= a.end_time
  ORDER BY a.id,
    b.amount DESC,
    b.bid_time,
    b.buyer;`}
                title="Create a view."
              />
            </RunnableContainer>
          </ListItem>

          <ListItem textStyle="text-base">
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/select/"]}>
                SELECT
              </TextLink>
            </Code>{" "}
            from the view to execute the underlying query. For example:
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SELECT * FROM winning_bids
ORDER BY bid_time DESC
LIMIT 10;`}
                title="Example 1"
              />
              <Runnable
                runCommand={runCommand}
                value={`SELECT * FROM winning_bids
WHERE item = 'Best Pizza in Town'
ORDER BY bid_time DESC
LIMIT 10;`}
                title="Example 2"
              />
            </RunnableContainer>
            <TextContainer>
              <Text>
                Since new data is continually being ingested, you must{" "}
                <span style={{ fontWeight: "600" }}>rerun the query</span> to
                get the up-to-date results. Each time you query the view, you
                are re-running the underlying statement, which becomes less
                performant as the amount of data grows.
              </Text>
              <Text>
                In Materialize, to make the queries more performant even as data
                continues to grow, you can create{" "}
                <TextLink
                  target="_blank"
                  href={docUrls["/docs/concepts/indexes/"]}
                >
                  indexes
                </TextLink>{" "}
                on views. Indexes provide always fresh view results in memory
                within a cluster by performing{" "}
                <span style={{ fontWeight: "600" }}>
                  incremental updates as new data arrives
                </span>
                . Queries can then read from the in-memory, already up-to-date
                results instead of re-running the underlying statement, making
                queries{" "}
                <span style={{ fontWeight: "600" }}>
                  computationally free and more performant
                </span>
                .
              </Text>
              <Text>
                In the next step, you will create an index on{" "}
                <Code variant="inline-syntax">winning_bids</Code>.
              </Text>
            </TextContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Create an index to provide up-to-date results.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            Indexes in Materialize represent query results stored in memory
            within a cluster. In Materialize, you can create{" "}
            <TextLink target="_blank" href={docUrls["/docs/concepts/indexes/"]}>
              indexes
            </TextLink>{" "}
            on views to provide always fresh view results in memory within a
            cluster. Queries can then read from the in-memory, already
            up-to-date results instead of re-running the underlying statement.
          </Text>
          <Text textStyle="text-base">
            To provide the up-to-date results, indexes
            <span style={{ fontWeight: "600" }}>
              {" "}
              perform incremental updates{" "}
            </span>{" "}
            as inputs change instead of recalculating the results from scratch.
            Additionally, indexes can also help{" "}
            <TextLink
              target="_blank"
              href={docUrls["/docs/transform-data/optimization/"]}
            >
              optimize operations
            </TextLink>{" "}
            like point lookups and joins.
          </Text>
        </TextContainer>

        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Use the{" "}
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-index/"]}
              >
                CREATE INDEX
              </TextLink>
            </Code>{" "}
            command to create the following index on the
            <Code variant="inline-syntax">winning_bids</Code> view.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="CREATE INDEX wins_by_item ON winning_bids (item);"
                title="Index winning_bids on the item column"
              />
            </RunnableContainer>
            <TextContainer>
              <Text textStyle="text-base">
                During the index creation, the underlying{" "}
                <Code variant="inline-syntax">winning_bids</Code> query is
                executed, and the view results are stored in memory within the
                cluster.{" "}
                <span style={{ fontWeight: "600" }}>
                  As new data arrives, the index incrementally updates the view
                  results in memory.{" "}
                </span>{" "}
                Because incremental work is performed on writes, reads from
                indexes return up-to-date results and are computationally free.
              </Text>
              <Text textStyle="text-base">
                The index can also help{" "}
                <TextLink
                  target="_blank"
                  href={docUrls["/docs/transform-data/optimization/"]}
                >
                  optimize
                </TextLink>{" "}
                operations like point lookups and{" "}
                <TextLink
                  target="_blank"
                  href={`${docUrls["/docs/transform-data/optimization/"]}#optimize-multi-way-joins-with-delta-joins`}
                >
                  delta joins{" "}
                </TextLink>
                on the index column(s) as well as support ad-hoc queries.
              </Text>
            </TextContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Rerun the previous queries on{" "}
            <Code variant="inline-syntax">winning_bids</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SELECT * FROM winning_bids
ORDER BY bid_time DESC
LIMIT 10;`}
                title="Example 1"
              />
              <Runnable
                runCommand={runCommand}
                value={`SELECT * FROM winning_bids
WHERE item = 'Best Pizza in Town'
ORDER BY bid_time DESC;`}
                title="Example 2"
              />
            </RunnableContainer>
            <Text textStyle="text-base">
              The queries should be faster since they use the in-memory, already
              up-to-date results computed by the index.
            </Text>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Create views and a table to find flippers in real time.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            For this quickstart, auction flipping activities are defined as when
            a user buys an item in one auction and resells the same item at a
            higher price within an 8-day period. This step finds auction
            flippers in real time, based on auction flipping activity data and
            known flippers data. Specifically, this step creates:
          </Text>

          <UnorderedList>
            <ListItem textStyle="text-base">
              A view to find auction flipping activities. Results are updated as
              new data comes in (at 1 second intervals) from the data generator.
            </ListItem>
            <ListItem textStyle="text-base">
              A table that maintains known auction flippers. You will manually
              enter new data to this table.
            </ListItem>
            <ListItem textStyle="text-base">
              A view to immediately see auction flippers based on either the
              flipping activities view or the known auction flippers table.
            </ListItem>
          </UnorderedList>
        </TextContainer>

        <OrderedList spacing={3}>
          <ListItem>
            Create a view to detect auction flipping activities.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW flip_activities AS
SELECT w2.seller as flipper_id,
       w2.item AS item,
       w2.amount AS sold_amount,
       w1.amount AS purchased_amount,
       w2.amount - w1.amount AS diff_amount,
       datediff('days', w2.bid_time, w1.bid_time)
          AS timeframe_days
FROM winning_bids AS w1
JOIN winning_bids AS w2
  ON w1.buyer = w2.seller AND w1.item = w2.item
WHERE w2.amount > w1.amount
  AND datediff('days',w2.bid_time,w1.bid_time) < 8
;`}
                title="Create a view flip_activities"
              />

              <Text textStyle="text-base">
                The <Code variant="inline-syntax"> flip_activities</Code> view
                can use the index created on
                <Code variant="inline-syntax"> winning_bids</Code> view to
                provide up-to-date data.
              </Text>
              <Text textStyle="text-base">
                To view a sample row in{" "}
                <Code variant="inline-syntax">flip_activities</Code>, run the
                following{" "}
                <Code variant="inline-syntax">
                  <TextLink target="_blank" href={docUrls["/docs/sql/select/"]}>
                    SELECT{" "}
                  </TextLink>
                </Code>{" "}
                command:
              </Text>
              <Runnable
                runCommand={runCommand}
                value="SELECT * FROM flip_activities LIMIT 10;"
                title="View a sample row in flip_activities"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem>
            Use{" "}
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-table/"]}
              >
                CREATE TABLE{" "}
              </TextLink>
            </Code>{" "}
            to create a <Code variant="inline-syntax">known_flippers</Code>{" "}
            table that you can manually populate with known flippers. That is,
            assume that separate from your auction activities data, you receive
            independent data specifying users as flippers.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="CREATE TABLE known_flippers (flipper_id bigint);"
                title="Create a table known_flippers"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem>
            Create a view <Code variant="inline-syntax">flippers</Code> to
            immediately see flippers if either:
            <UnorderedList>
              <ListItem>A user has 2 or more flipping activities; or</ListItem>
              <ListItem>
                A user is listed in the
                <Code variant="inline-syntax">known_flippers</Code> table.
              </ListItem>
            </UnorderedList>
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW flippers AS
SELECT flipper_id
FROM (
     SELECT flipper_id
     FROM flip_activities
     GROUP BY flipper_id
     HAVING count(*) >= 2

     UNION ALL

     SELECT flipper_id
     FROM known_flippers
);`}
                title="Create a view flippers"
              />
            </RunnableContainer>
          </ListItem>
        </OrderedList>
        <Text>
          Both the <Code variant="inline-syntax">known_flippers</Code> and{" "}
          <Code variant="inline-syntax">flippers</Code> views can use the index
          created on <Code variant="inline-syntax">winning_bids</Code> view to
          provide up-to-date data. Depending upon your query patterns and usage,
          an existing index may be sufficient, such as in this quickstart. In
          other use cases, creating an index only on the view(s) from which you
          will serve results may be preferred.
        </Text>
      </>
    ),
  },
  {
    title: "Subscribe to see results change.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE{" "}
              </TextLink>
            </Code>{" "}
            to <Code variant="inline-syntax">flippers</Code> to see new flippers
            appear as new data arrives (either from the{" "}
            <Code variant="inline-syntax">known_flippers</Code> table or the{" "}
            <Code variant="inline-syntax">flip_activities</Code> view).
          </Text>
        </TextContainer>

        <OrderedList spacing={3}>
          <ListItem>
            Use{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>{" "}
            to see flippers as new data arrives (either from the{" "}
            <Code variant="inline-syntax">known_flippers</Code> table or the{" "}
            <Code variant="inline-syntax">flip_activities</Code> view).
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SUBSCRIBE TO (
   SELECT *
   FROM flippers
) WITH (snapshot = false);`}
                title="Subscribe to see flippers as new data comes in."
              />
            </RunnableContainer>
            The optional{" "}
            <Code variant="inline-syntax">WITH (snapshot = false)</Code>{" "}
            indicates that the command displays only the new flippers that come
            in after the start of the{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>{" "}
            operation, and not the flippers in the view at the start of the
            operation.
          </ListItem>

          <ListItem>
            Enter a flipper id into the{" "}
            <Code variant="inline-syntax">known_flippers</Code> table. You can
            specify any number for the flipper id.
            <RunnableContainer mt={3} mb={3}>
              <Box width="100%">
                <TutorialInsertionWidget />
              </Box>
            </RunnableContainer>
            The flipper should immediately appear in the{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>{" "}
            results. You should also see flippers who are flagged by their flip
            activities. Because of the randomness of the auction data being
            generated, user activity data that match the definition of a flipper
            may take some time even though auction data is constantly being
            ingested. However, once new matching data comes in, you will see it
            immediately in the{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>{" "}
            results. While waiting, you can enter additional flippers into the{" "}
            <Code variant="inline-syntax">known_flippers</Code> table.
          </ListItem>
          <ListItem>
            To cancel out of the
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE{" "}
              </TextLink>
            </Code>
            , click the{" "}
            <span style={{ fontWeight: "bold" }}>Stop streaming</span> button.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Verify that Materialize returns consistent data.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>

          <Text textStyle="text-base">
            To verify that Materialize serves consistent results, even as new
            data comes in, this step creates the following views for completed
            auctions:
          </Text>

          <UnorderedList>
            <ListItem textStyle="text-base">
              A view to keep track of each seller&apos;s credits.
            </ListItem>
            <ListItem textStyle="text-base">
              A view to keep track of each buyer&apos;s debits.
            </ListItem>
            <ListItem textStyle="text-base">
              A view that sums all sellers&apos; credits, all buyers&apos;
              debits, and calculates the difference, which should be 0.
            </ListItem>
          </UnorderedList>
        </TextContainer>

        <OrderedList spacing={3}>
          <ListItem>
            Create a view to track credited amounts for sellers of completed
            auctions.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW seller_credits AS
SELECT seller, SUM(amount) as credits
FROM winning_bids
GROUP BY seller;`}
                title="Create a view that tracks sellers' credit amount"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem>
            Create a view to track debited amounts for the winning bidders of
            completed auctions.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW buyer_debits AS
SELECT buyer, SUM(amount) as debits
FROM winning_bids
GROUP BY buyer;`}
                title="Create a view that tracks buyers' debit amounts"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem>
            To verify that the total credit and total debit amounts equal for
            completed auctions (i.e., to verify that the results are correct and
            consistent even as new data comes in), create a{" "}
            <Code variant="inline-syntax">funds_movement</Code> view that
            calculates the total credits across sellers, total debits across
            buyers, and the difference between the two.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW funds_movement AS
SELECT SUM(credits) AS total_credits,
       SUM(debits) AS total_debits,
       SUM(credits) - SUM(debits) AS total_difference
FROM (
  SELECT SUM(credits) AS credits, 0 AS debits
  FROM seller_credits

  UNION

  SELECT 0 AS credits, SUM(debits) AS debits
  FROM buyer_debits
);`}
                title="Create a view to verify that the total credits and debits equal"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem>
            To see that the sums always equal even as new data comes in, you can{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>{" "}
            to this query:
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SUBSCRIBE TO (
  SELECT *
  FROM funds_movement
);`}
                title="Subscribe to see results change over time!"
              />
            </RunnableContainer>
            Toggle <span style={{ fontWeight: "bold" }}>Show diffs</span> to see
            changes to <Code variant="inline-syntax">funds_movement</Code>. As
            new data comes in and auctions complete, the{" "}
            <Code variant="inline-syntax">total_credits</Code> and
            <Code variant="inline-syntax">total_debits</Code> values should
            change but the <Code variant="inline-syntax">total_difference</Code>
            should remain 0.
          </ListItem>
          <ListItem>
            To cancel out of the{" "}
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>
            , click the{" "}
            <span style={{ fontWeight: "bold" }}>Stop streaming</span> button.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Clean up.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            To clean up the quickstart environment:
          </Text>

          <UnorderedList>
            <ListItem>
              Use{" "}
              <Code variant="inline-syntax">
                <TextLink
                  target="_blank"
                  href={docUrls["/docs/sql/drop-source/"]}
                >
                  DROP SOURCE
                </TextLink>
              </Code>{" "}
              with the <Code variant="inline-syntax">CASCADE</Code> option to
              drop <Code variant="inline-syntax">auction_house</Code> and its
              dependent objects (e.g., views and indexes).
            </ListItem>

            <ListItem>
              Use{" "}
              <Code variant="inline-syntax">
                {" "}
                <TextLink
                  target="_blank"
                  href={docUrls["/docs/sql/drop-table/"]}
                >
                  DROP TABLE
                </TextLink>
              </Code>{" "}
              to drop the <Code variant="inline-syntax">known_flippers</Code>{" "}
              table
            </ListItem>
          </UnorderedList>

          <Runnable
            runCommand={runCommand}
            value="DROP SOURCE auction_house CASCADE;"
            title="Drop the source and its dependent objects."
          />
          <Runnable
            runCommand={runCommand}
            value="DROP TABLE known_flippers;"
            title="Drop the known_flippers table."
          />
        </TextContainer>
      </>
    ),
  },
  {
    title: "Summary",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            In Materialize,{" "}
            <TextLink target="_blank" href={docUrls["/docs/concepts/indexes/"]}>
              indexes
            </TextLink>{" "}
            represent query results stored in memory within a cluster. When you
            create an index on a view, the index incrementally updates the view
            results (instead of recalculating the results from scratch) as
            Materialize ingests new data. These up-to-date results are then
            immediately available and computationally free for reads within the
            cluster.
          </Text>
          <Text textStyle="heading-sm">General guidelines</Text>
          <Text textStyle="text-base">
            This quickstart created an index on a view to maintain in-memory
            up-to-date results in the cluster. In Materialize, both materialized
            views and indexes on views incrementally update view results.
            Materialized views persist the query results in durable storage and
            is available across clusters while indexes maintain the view results
            in memory within a single cluster. Some general guidelines for using
            indexed views (I) vs. materialized views (M) include:
          </Text>
        </TextContainer>
        {/**
         * Avoid wrapping in TableContainer, which uses immutable overflowY value of hidden.
         */}
        <Table size="sm">
          <Thead>
            <Tr>
              <Th>Usage Pattern</Th>
              <Th></Th>
            </Tr>
          </Thead>
          <Tbody>
            <Tr>
              <Td>View results are accessed from a single cluster only</Td>
              <Td>I</Td>
            </Tr>
            <Tr>
              <Td>View results are accessed across clusters</Td>
              <Td>M</Td>
            </Tr>
            <Tr>
              <Td>
                Final consumer of the view is a{" "}
                <TextLink
                  target="_blank"
                  href={docUrls["/docs/concepts/sinks/"]}
                >
                  sinks
                </TextLink>{" "}
                or a{" "}
                <TextLink
                  target="_blank"
                  href={docUrls["/docs/sql/subscribe/"]}
                >
                  SUBSCRIBE
                </TextLink>{" "}
                operation
              </Td>
              <Td>M</Td>
            </Tr>
            <Tr>
              <Td>
                Use of{" "}
                <TextLink
                  target="_blank"
                  href={
                    docUrls["/docs/transform-data/patterns/temporal-filters/"]
                  }
                >
                  temporal filters
                </TextLink>
              </Td>
              <Td>M</Td>
            </Tr>
          </Tbody>
        </Table>

        <TextContainer>
          <Text textStyle="text-base">The quickstart used an index since:</Text>
          <UnorderedList>
            <ListItem>
              The examples did not need to store the results in durable storage.
            </ListItem>

            <ListItem>
              All activities were limited to the single quickstart cluster.
            </ListItem>
            <ListItem>
              Although used, SUBSCRIBE operations were for
              illustrative/validation purposes and were not the final consumer
              of the views.
            </ListItem>
          </UnorderedList>

          <Text textStyle="heading-sm">Considerations</Text>
          <Text textStyle="text-base">
            Before creating an index (which represent query results stored in
            memory), consider its memory usage as well as its
            <TextLink
              target="_blank"
              href={`${docUrls["/docs/administration/billing/"]}#compute-cost-factors`}
            >
              {" "}
              compute cost{" "}
            </TextLink>
            implications. For best practices when creating indexes, see{" "}
            <TextLink
              target="_blank"
              href={`${docUrls["/docs/concepts/indexes/"]}#best-practices`}
            >
              Index Best Practices
            </TextLink>
            .
          </Text>
        </TextContainer>
      </>
    ),
  },
  {
    title: "What's next?",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            To start developing with your own data, click{" "}
            <span style={{ fontWeight: "600" }}>Connect data</span>.
          </Text>
          <Text textStyle="text-base">
            For help getting started with your data or other questions about
            Materialize, click{" "}
            <span style={{ fontWeight: "600" }}>Talk to us</span>.
          </Text>

          <Text textStyle="heading-sm">Additional resources</Text>
        </TextContainer>

        <UnorderedList>
          <ListItem>
            <TextLink
              target="_blank"
              href={docUrls["/docs/concepts/clusters/"]}
            >
              Clusters
            </TextLink>
          </ListItem>
          <ListItem>
            <TextLink target="_blank" href={docUrls["/docs/concepts/indexes/"]}>
              Indexes
            </TextLink>{" "}
          </ListItem>
          <ListItem>
            <TextLink target="_blank" href={docUrls["/docs/concepts/sources/"]}>
              Sources
            </TextLink>
          </ListItem>
          <ListItem>
            <TextLink target="_blank" href={docUrls["/docs/concepts/views/"]}>
              Views
            </TextLink>
          </ListItem>
          <ListItem>
            <TextLink
              target="_blank"
              href={
                docUrls[
                  "/docs/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart/"
                ]
              }
            >
              Idiomatic Materialize SQL Chart
            </TextLink>
          </ListItem>
          <ListItem>
            <TextLink
              target="_blank"
              href={docUrls["/docs/administration/billing/"]}
            >
              Usage and Billing
            </TextLink>
          </ListItem>
          <ListItem>
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-index/"]}
              >
                CREATE INDEX
              </TextLink>
            </Code>
          </ListItem>
          <ListItem>
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-schema/"]}
              >
                CREATE SCHEMA
              </TextLink>
            </Code>
          </ListItem>
          <ListItem>
            <Code variant="inline-syntax">
              <TextLink
                target="_blank"
                href={docUrls["/docs/sql/create-view/"]}
              >
                CREATE VIEW
              </TextLink>
            </Code>
          </ListItem>
          <ListItem>
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/select/"]}>
                SELECT
              </TextLink>
            </Code>
          </ListItem>
          <ListItem>
            <Code variant="inline-syntax">
              <TextLink target="_blank" href={docUrls["/docs/sql/subscribe/"]}>
                SUBSCRIBE
              </TextLink>
            </Code>
          </ListItem>
        </UnorderedList>
      </>
    ),
  },
];

/**
 * Since many steps have the same title, this gets the xth step of a given title
 */
const getLocalTutorialStepByTitle = (
  globalTutorialStep: number,
  title: string,
): number => {
  const startIndexByTitle = stepsData.reduce<{ [title: string]: number }>(
    (accum, step, index) => {
      if (accum[step.title] === undefined) {
        accum[step.title] = index;
      }
      return accum;
    },
    {},
  );

  if (Object.keys(startIndexByTitle).length === 0) {
    return 0;
  }

  return globalTutorialStep - startIndexByTitle[title];
};

const Steps = forwardRef(
  (
    {
      runCommand,
      onChangeStep,
    }: {
      runCommand: (command: string) => void;
      onChangeStep: (desired: number) => void;
    },
    ref,
  ) => {
    const [shellState] = useAtom(shellStateAtom);
    const { currentTutorialStep } = shellState;
    const { colors } = useTheme<MaterializeTheme>();

    const regionSlug = useRegionSlug();

    const atStart = currentTutorialStep === 0;

    const atEnd = currentTutorialStep >= stepsData.length - 1;

    const step = stepsData[currentTutorialStep];

    return (
      <StepLayout width="100%" ref={ref}>
        <step.render
          runCommand={runCommand}
          title={step.title}
          colors={colors}
        />
        <HStack
          width="100%"
          alignSelf="flex-end"
          alignItems="space-between"
          justifyContent={atStart ? "flex-end" : "space-between"}
          marginTop="4"
        >
          {!atStart && (
            <Button
              onClick={() => onChangeStep(currentTutorialStep - 1)}
              variant="tertiary"
            >
              Back
            </Button>
          )}
          {atEnd ? (
            <HStack gap={4}>
              <ScheduleDemoLink>
                <Button variant="primary">Talk to us</Button>
              </ScheduleDemoLink>
              <Button
                as={Link}
                variant="primary"
                to={`${newConnectionPath(regionSlug)}`}
              >
                Connect data
              </Button>
            </HStack>
          ) : (
            <Button
              onClick={() => onChangeStep(currentTutorialStep + 1)}
              variant="primary"
            >
              Continue
            </Button>
          )}
        </HStack>
      </StepLayout>
    );
  },
);

type ProgressProps = {
  min: number;
  max: number;
  value: number;
  onStepClick: (step: number) => void;
} & StackProps;

const PROGRESS_STEP_SPACING = 1;

const Progress = ({ min, max, value, onStepClick, ...rest }: ProgressProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const steps: boolean[] = [];

  for (let i = min; i < max; i++) {
    steps.push(i <= value);
  }

  return (
    <HStack {...rest} spacing={PROGRESS_STEP_SPACING}>
      {steps.map((filled, idx) => {
        return (
          <Box
            key={idx}
            flexGrow="1"
            height="1"
            borderRadius="2px"
            transition="all 0.2s"
            bgColor={filled ? colors.accent.purple : colors.background.tertiary}
            onClick={() => onStepClick(idx)}
            cursor={value !== idx ? "pointer" : "auto"}
            title={value !== idx ? `Jump to step ${idx + 1}` : ""}
            sx={{
              _hover: {
                backgroundColor: colors.accent.purple,
                boxShadow:
                  value !== idx
                    ? `0px 0px 3px 1px ${colors.accent.purple}`
                    : "none",
              },
            }}
          />
        );
      })}
    </HStack>
  );
};

type TutorialProps = GridItemProps & {
  runCommand: (command: string) => void;
};

const Tutorial = ({ runCommand, ...rest }: TutorialProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [shellState, setShellState] = useAtom(shellStateAtom);
  const { currentTutorialStep: step } = shellState;
  const { track } = useSegment();
  const stepsScrollContainerRef = useRef<HTMLDivElement>(null);

  const changeStep = (desired: number) => {
    if (desired < 0) {
      desired = 0;
    } else if (desired >= stepsData.length) {
      desired = stepsData.length - 1;
    }
    const prevStep = step;

    setShellState((prev) => ({ ...prev, currentTutorialStep: desired }));

    const title = stepsData[desired].title;

    track("Tutorial change page", {
      pageHeader: title,
      pageNumber: desired,
      sectionPageNumber: getLocalTutorialStepByTitle(desired, title),
      quickstartVersion: QUICKSTART_VERSION,
    });

    const isChangingToSecondStep = prevStep === 0 && desired === 1;
    if (isChangingToSecondStep) {
      // If a user goes from the first page to the next page, we use this as a
      // signal that the user has intended to start the quickstart
      track("Quickstart Start", {
        quickstartVersion: QUICKSTART_VERSION,
      });
    }

    const isChangingToLastStep =
      prevStep === stepsData.length - 2 && desired === stepsData.length - 1;
    if (isChangingToLastStep) {
      // If a user goes from the second last page to the last page, we use this
      // as a signal that the user has completed the quickstart.
      track("Quickstart End", {
        quickstartVersion: QUICKSTART_VERSION,
      });
    }

    stepsScrollContainerRef.current?.scrollTo({ top: 0 });
  };

  return (
    <GridItem
      area="tutorial"
      borderLeftWidth="1px"
      borderColor={colors.border.secondary}
      bg={colors.background.shellTutorial}
      borderBottomRightRadius="lg"
      display="flex"
      flexDirection="column"
      {...rest}
    >
      <VStack
        paddingTop="6"
        spacing="0"
        alignItems="flex-start"
        minHeight="0"
        flex="1"
      >
        <VStack spacing="6" alignItems="flex-start" width="100%" paddingX="10">
          <Progress
            min={0}
            max={stepsData.length}
            value={step}
            width="100%"
            onStepClick={changeStep}
          />
          <Text
            textStyle="text-small"
            fontWeight="500"
            color={colors.foreground.secondary}
            marginBottom="4"
          >
            QUICKSTART
          </Text>
        </VStack>
        <Steps
          runCommand={runCommand}
          onChangeStep={changeStep}
          ref={stepsScrollContainerRef}
        />
      </VStack>
    </GridItem>
  );
};

export default Tutorial;
