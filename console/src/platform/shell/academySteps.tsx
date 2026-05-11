// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Code,
  ListItem,
  OrderedList,
  Text,
  UnorderedList,
} from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";

import {
  Runnable,
  RunnableContainer,
  StepData,
  TextContainer,
} from "./tutorialUtils";

/**
 * MZ Academy: intro-to-Materialize tutorial steps.
 *
 * Assumes the user has a Materialize emulator with a PostgreSQL source feeding
 * an e-commerce schema (users, products, orders, order_items, shopping_carts,
 * shopping_cart_items, categories, featured_products), a Kafka source feeding
 * a clickstream topic, and three user clusters: source_cluster,
 * transform_cluster, serving_cluster.
 *
 * The SQL here references those tables; if the environment isn't set up the
 * Runnable will surface a "relation does not exist" error from Materialize and
 * the learner can go back and complete the environment setup.
 */
export const academyStepsData: StepData[] = [
  {
    title: "Welcome to MZ Academy",
    render: ({ title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            This tutorial is an in-console rendition of the MZ Academy
            intro-to-Materialize course. It walks through views, materialized
            views, indexes, idiomatic Materialize SQL, and temporal filters
            against a small e-commerce dataset replicated from PostgreSQL plus a
            Kafka clickstream.
          </Text>
          <Text textStyle="heading-sm">Before you start</Text>
          <Text textStyle="text-base">
            The tutorial assumes you have a Materialize emulator with a
            PostgreSQL source feeding an e-commerce schema (
            <Code variant="inline-syntax">users</Code>,{" "}
            <Code variant="inline-syntax">products</Code>,{" "}
            <Code variant="inline-syntax">orders</Code>,{" "}
            <Code variant="inline-syntax">order_items</Code>,{" "}
            <Code variant="inline-syntax">shopping_carts</Code>,{" "}
            <Code variant="inline-syntax">shopping_cart_items</Code>,{" "}
            <Code variant="inline-syntax">categories</Code>,{" "}
            <Code variant="inline-syntax">featured_products</Code>), a Kafka
            source feeding a <Code variant="inline-syntax">clickstream</Code>{" "}
            topic, and three user clusters:{" "}
            <Code variant="inline-syntax">source_cluster</Code>,{" "}
            <Code variant="inline-syntax">transform_cluster</Code>, and{" "}
            <Code variant="inline-syntax">serving_cluster</Code>.
          </Text>
          <Text textStyle="text-base">
            If you&apos;re working from a stock emulator, the next step will
            walk you through verifying which pieces are present and which still
            need to be set up.
          </Text>
        </TextContainer>
      </>
    ),
  },
  {
    title: "Verify the environment.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            Run the queries below. If any return an empty result set or an
            error, complete the environment setup and come back to this step.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Confirm the three user clusters are present.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="SHOW CLUSTERS;"
                title="List clusters"
              />
            </RunnableContainer>
            You should see <Code variant="inline-syntax">source_cluster</Code>,{" "}
            <Code variant="inline-syntax">transform_cluster</Code>, and{" "}
            <Code variant="inline-syntax">serving_cluster</Code> alongside the
            built-in system clusters.
          </ListItem>
          <ListItem textStyle="text-base">
            Confirm the PostgreSQL and Kafka sources are running.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="SHOW SOURCES;"
                title="List sources"
              />
            </RunnableContainer>
            Expected: <Code variant="inline-syntax">commerce_pg_source</Code>,{" "}
            <Code variant="inline-syntax">marketing_pg_source</Code>, and{" "}
            <Code variant="inline-syntax">clickstream_source</Code>.
          </ListItem>
          <ListItem textStyle="text-base">
            Confirm the source tables and the clickstream parsing view exist.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="SHOW TABLES;"
                title="List tables"
              />
              <Runnable
                runCommand={runCommand}
                value="SHOW VIEWS;"
                title="List views"
              />
            </RunnableContainer>
            Expect tables <Code variant="inline-syntax">users</Code>,{" "}
            <Code variant="inline-syntax">products</Code>,{" "}
            <Code variant="inline-syntax">orders</Code>,{" "}
            <Code variant="inline-syntax">order_items</Code>,{" "}
            <Code variant="inline-syntax">shopping_carts</Code>,{" "}
            <Code variant="inline-syntax">shopping_cart_items</Code>,{" "}
            <Code variant="inline-syntax">categories</Code>,{" "}
            <Code variant="inline-syntax">featured_products</Code>, and a{" "}
            <Code variant="inline-syntax">clickstream</Code> view that parses
            the Kafka topic.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Clusters: compute resources, isolated.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            A{" "}
            <TextLink
              target="_blank"
              href={docUrls["/docs/concepts/clusters/"]}
            >
              cluster
            </TextLink>{" "}
            in Materialize provides the CPU and memory needed by sources,
            indexes, materialized views, and ad-hoc queries. Clusters are
            isolated from each other — memory in one cluster (e.g., an index) is
            not visible to another. For production deployments we recommend
            separating workloads into three clusters:
          </Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <Code variant="inline-syntax">source_cluster</Code> for source
              ingestion.
            </ListItem>
            <ListItem textStyle="text-base">
              <Code variant="inline-syntax">transform_cluster</Code> for
              materialized views and shared transform indexes.
            </ListItem>
            <ListItem textStyle="text-base">
              <Code variant="inline-syntax">serving_cluster</Code> for
              query-time indexes and end-user reads.
            </ListItem>
          </UnorderedList>
          <Text textStyle="text-base">
            Tables, views, and materialized views are reachable from any
            cluster. Indexes are not — they live in one cluster&apos;s memory
            and are only used by queries that run on that cluster.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Switch to <Code variant="inline-syntax">transform_cluster</Code> for
            the upcoming view and materialized-view exercises.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';`}
                title="Use the transform cluster"
              />
            </RunnableContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Views: define-once, recomputed-on-query.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            A{" "}
            <TextLink target="_blank" href={docUrls["/docs/concepts/views/"]}>
              view
            </TextLink>{" "}
            is a query definition saved under a name. Unindexed regular views
            don&apos;t store results — each query against the view recomputes
            them from scratch. They&apos;re great for readability and
            composition; pick a materialized view or an index later only when
            usage patterns justify the storage or memory.
          </Text>
          <Text textStyle="text-base">
            In this step we stack four shopping-cart views on top of the
            replicated <Code variant="inline-syntax">shopping_carts</Code>,{" "}
            <Code variant="inline-syntax">shopping_cart_items</Code>, and{" "}
            <Code variant="inline-syntax">products</Code> tables.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Per-line-item subtotals, with an out-of-stock alert.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW shopping_cart_line_item_subtotals AS
  SELECT
      sci.cart_id,
      sci.id AS cart_item_id,
      sci.product_id,
      p.name AS product_name,
      sci.quantity,
      p.price,
      sci.quantity * p.price AS line_total,
      CASE
          WHEN p.stock_quantity < sci.quantity
            THEN 'Notice: ' || p.stock_quantity || ' in stock.'
          ELSE NULL
      END AS alert,
      sc.user_id
  FROM shopping_cart_items sci
  JOIN shopping_carts sc ON sci.cart_id = sc.id
  JOIN products p ON sci.product_id = p.id;`}
                title="Create shopping_cart_line_item_subtotals"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Per-cart total, built on top of the previous view.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW shopping_cart_totals AS
  SELECT cart_id, SUM(line_total) AS cart_total
  FROM shopping_cart_line_item_subtotals
  GROUP BY cart_id;`}
                title="Create shopping_cart_totals"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Checkout view that joins line items with their cart totals.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW shopping_cart_checkout AS
SELECT sclis.*, sct.cart_total
FROM shopping_cart_line_item_subtotals sclis
JOIN shopping_cart_totals sct
  ON sclis.cart_id = sct.cart_id;`}
                title="Create shopping_cart_checkout"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW current_totals_all_shopping_carts AS
SELECT SUM(cart_total) AS current_carts_total
FROM shopping_cart_totals;`}
                title="Create current_totals_all_shopping_carts"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Query the views from a different cluster — tables and views work
            cross-cluster.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'serving_cluster';
SELECT * FROM shopping_cart_checkout LIMIT 5;
SELECT * FROM current_totals_all_shopping_carts;`}
                title="Read from serving_cluster"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN SELECT * FROM shopping_cart_checkout;"
                title="EXPLAIN — sees every join/aggregate"
              />
            </RunnableContainer>
            The <Code variant="inline-syntax">EXPLAIN</Code> plan lists the base
            tables and the join/aggregate operations that must run on every
            query — there&apos;s no precomputed state yet.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Materialized views: incremental, persisted, cross-cluster.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            A{" "}
            <TextLink
              target="_blank"
              href={`${docUrls["/docs/concepts/views/"]}#materialized-views`}
            >
              materialized view
            </TextLink>{" "}
            persists its result set in durable storage and{" "}
            <strong>incrementally updates</strong> it as the input tables
            change. Queries against the materialized view read the already-up-
            to-date results — no recomputation needed. The persisted state is
            visible to every cluster.
          </Text>
          <Text textStyle="text-base">
            Tip: when stacking views, you typically only materialize the
            top-level one. Leave the underlying views as regular views unless
            you have other consumers.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Build a per-product sales view, then a materialized view on top of
            it that includes every product (including ones with no sales yet).
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
CREATE VIEW product_sales AS
SELECT
    oi.product_id,
    p.name AS product_name,
    COUNT(DISTINCT oi.order_id) AS order_count,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    AVG(oi.unit_price) AS avg_selling_price
FROM order_items oi
JOIN products p ON oi.product_id = p.id
GROUP BY oi.product_id, p.name;`}
                title="Create product_sales view"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE MATERIALIZED VIEW product_performance AS
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.price AS current_price,
    p.stock_quantity,
    COALESCE(ps.order_count, 0)       AS order_count,
    COALESCE(ps.units_sold, 0)        AS units_sold,
    COALESCE(ps.total_revenue, 0)     AS total_revenue,
    COALESCE(ps.avg_selling_price, 0) AS avg_selling_price,
    CASE
        WHEN p.stock_quantity = 0  THEN 'out_of_stock'
        WHEN p.stock_quantity < 20 THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status
FROM products p
LEFT JOIN product_sales ps ON p.id = ps.product_id;`}
                title="Create product_performance materialized view"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Read it from <Code variant="inline-syntax">serving_cluster</Code>{" "}
            and compare <Code variant="inline-syntax">EXPLAIN</Code> output to
            the regular-view plan.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'serving_cluster';
SELECT * FROM product_performance ORDER BY total_revenue DESC LIMIT 10;`}
                title="Query product_performance"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN SELECT * FROM product_performance;"
                title="EXPLAIN — streams from the materialized view"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN SELECT * FROM product_sales;"
                title="EXPLAIN — recomputes from base tables"
              />
            </RunnableContainer>
            The materialized view&apos;s plan reads from the stored result — no
            join, no aggregate. The plain view&apos;s plan still has the join +
            aggregate operations.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Indexes: in-memory, cluster-local.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            An{" "}
            <TextLink target="_blank" href={docUrls["/docs/concepts/indexes/"]}>
              index
            </TextLink>{" "}
            in Materialize keeps a view&apos;s (or materialized view&apos;s)
            full result set in a specific cluster&apos;s memory, organized by a
            hashed key, and updates it incrementally. Two properties to
            internalize:
          </Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <strong>Cluster-local.</strong> Only queries running on the
              index&apos;s cluster can use it.
            </ListItem>
            <ListItem textStyle="text-base">
              <strong>Hashed key.</strong> Equality on the full key is a point
              lookup. Anything else (prefix, range, OR with non-key clauses)
              still runs against the index but does a full scan — fast, but not
              as fast.
            </ListItem>
          </UnorderedList>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Index the <Code variant="inline-syntax">product_sales</Code> view
            and the <Code variant="inline-syntax">product_performance</Code>{" "}
            materialized view in{" "}
            <Code variant="inline-syntax">serving_cluster</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'serving_cluster';
CREATE INDEX product_sales_idx
  IN CLUSTER serving_cluster
  ON product_sales (product_id);`}
                title="Index product_sales by product_id"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE INDEX product_performance_idx
  IN CLUSTER serving_cluster
  ON product_performance (stock_status, product_id);`}
                title="Compound index on product_performance"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Verify the cluster-local property: the same query plan from
            <Code variant="inline-syntax">transform_cluster</Code> does{" "}
            <em>not</em> use the index.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'serving_cluster';
EXPLAIN SELECT * FROM product_performance
WHERE stock_status = 'low_stock' AND product_id = 5;`}
                title="Point lookup on serving_cluster — uses the index"
              />
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
EXPLAIN SELECT * FROM product_performance
WHERE stock_status = 'low_stock' AND product_id = 5;`}
                title="Same query on transform_cluster — no index"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Point lookup vs full scan. The first query specifies an equality on
            the full key; the second only matches the prefix.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'serving_cluster';
EXPLAIN SELECT * FROM product_performance
WHERE stock_status = 'low_stock'
  AND (product_id = 5 OR product_id = 6);`}
                title="Two point lookups"
              />
              <Runnable
                runCommand={runCommand}
                value={`EXPLAIN SELECT * FROM product_performance
WHERE stock_status = 'low_stock';`}
                title="Prefix only — full scan + notice"
              />
            </RunnableContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Index reuse and creation order.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            When you create an index or a materialized view, Materialize binds
            the plan once and looks for an existing index in the{" "}
            <strong>same cluster</strong> to reuse. Ad-hoc queries are
            re-planned at query time and can use any index that exists when they
            run. The upshot: <strong>creation order matters</strong> for indexes
            and materialized views. An existing materialized view will not
            retroactively pick up an index created after it.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Index <Code variant="inline-syntax">product_sales</Code> in{" "}
            <Code variant="inline-syntax">transform_cluster</Code>, then add a
            second materialized view that will use it.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE INDEX product_sales_idx_transform_cluster
  IN CLUSTER transform_cluster
  ON product_sales (product_id);`}
                title="Index product_sales in transform_cluster"
              />
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
CREATE MATERIALIZED VIEW category_sales_summary AS
SELECT
  c.id   AS category_id,
  c.name AS category_name,
  COUNT(*)                  AS products_with_sales,
  SUM(ps.units_sold)        AS category_units_sold,
  SUM(ps.total_revenue)     AS category_revenue,
  AVG(ps.avg_selling_price) AS avg_selling_price
FROM categories c
JOIN products p       ON p.category_id = c.id
JOIN product_sales ps ON ps.product_id = p.id
GROUP BY c.id, c.name;`}
                title="Create category_sales_summary"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Check what depends on the new index. Only{" "}
            <Code variant="inline-syntax">category_sales_summary</Code> (created
            after the index) uses it.{" "}
            <Code variant="inline-syntax">product_performance</Code>, which was
            created earlier, still reads the base tables.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SELECT
    dep.name AS index_name,
    o.name AS dependent_object,
    o.type AS dependent_type
 FROM mz_internal.mz_materialization_dependencies d
 JOIN mz_catalog.mz_objects o   ON d.object_id     = o.id
 JOIN mz_catalog.mz_objects dep ON d.dependency_id = dep.id
 WHERE dep.name = 'product_sales_idx_transform_cluster'
 ORDER BY o.name;`}
                title="Who uses the new index?"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Recreate <Code variant="inline-syntax">product_performance</Code> so
            it picks up the index. The{" "}
            <Code variant="inline-syntax">CASCADE</Code> is needed because the
            earlier <Code variant="inline-syntax">product_performance_idx</Code>{" "}
            depends on it; we&apos;ll add it back afterwards.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="DROP MATERIALIZED VIEW product_performance CASCADE;"
                title="Drop product_performance (+ its index)"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE MATERIALIZED VIEW product_performance AS
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.price AS current_price,
    p.stock_quantity,
    COALESCE(ps.order_count, 0)       AS order_count,
    COALESCE(ps.units_sold, 0)        AS units_sold,
    COALESCE(ps.total_revenue, 0)     AS total_revenue,
    COALESCE(ps.avg_selling_price, 0) AS avg_selling_price,
    CASE
        WHEN p.stock_quantity = 0  THEN 'out_of_stock'
        WHEN p.stock_quantity < 20 THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status
FROM products p
LEFT JOIN product_sales ps ON p.id = ps.product_id;`}
                title="Recreate product_performance"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE INDEX product_performance_idx
  IN CLUSTER serving_cluster
  ON product_performance (stock_status, product_id);`}
                title="Recreate product_performance_idx"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Verify <Code variant="inline-syntax">product_performance</Code> now
            depends on the index instead of{" "}
            <Code variant="inline-syntax">order_items</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SELECT
    o.name AS object,
    dep.name AS depends_on,
    dep.type AS depends_on_type
FROM mz_internal.mz_materialization_dependencies d
JOIN mz_catalog.mz_objects o   ON o.id   = d.object_id
JOIN mz_catalog.mz_objects dep ON dep.id = d.dependency_id
WHERE o.name = 'product_performance'
ORDER BY dep.name;`}
                title="What does product_performance depend on?"
              />
            </RunnableContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Idiomatic SQL: top-K with LATERAL + LIMIT.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            When you maintain a view that uses a window function (e.g.,{" "}
            <Code variant="inline-syntax">RANK() OVER (PARTITION BY …)</Code>),
            any change to a record in a partition forces Materialize to
            recompute that whole partition from scratch. For Top-K-per-group
            workloads, express the intent as a{" "}
            <TextLink
              target="_blank"
              href={
                docUrls["/docs/transform-data/idiomatic-materialize-sql/top-k/"]
              }
            >
              <Code variant="inline-syntax">LATERAL</Code> subquery with{" "}
              <Code variant="inline-syntax">ORDER BY ... LIMIT</Code>
            </TextLink>{" "}
            and let Materialize compile to an incremental TopK operator.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Build a reusable per-product revenue view, then a top-3-per-category
            view via <Code variant="inline-syntax">LATERAL</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
CREATE VIEW product_revenue AS
SELECT
    p.id          AS product_id,
    p.name        AS product_name,
    p.category_id,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category_id;`}
                title="Reusable product_revenue view"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW top_3_products_per_category AS
SELECT
    c.id   AS category_id,
    c.name AS category_name,
    r.product_id,
    r.product_name,
    r.revenue
FROM categories c,
LATERAL (
  SELECT product_id, product_name, revenue
  FROM product_revenue
  WHERE category_id = c.id
  ORDER BY revenue DESC
  LIMIT 3
) r;`}
                title="Top 3 products per category"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Inspect the plan — look for{" "}
            <Code variant="inline-syntax">Monotonic TopK</Code>, not{" "}
            <Code variant="inline-syntax">Non-incremental GroupAggregate</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN SELECT * FROM top_3_products_per_category;"
                title="EXPLAIN top_3_products_per_category"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            For K=1, <Code variant="inline-syntax">DISTINCT ON</Code> is the
            equivalent idiom and compiles to a{" "}
            <Code variant="inline-syntax">Monotonic Top1</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW IF NOT EXISTS top_product_per_category AS
SELECT DISTINCT ON (pr.category_id)
       pr.category_id,
       c.name        AS category_name,
       pr.product_id,
       pr.product_name,
       pr.revenue
FROM product_revenue pr
JOIN categories c ON c.id = pr.category_id
ORDER BY pr.category_id, pr.revenue DESC;`}
                title="Top 1 per category"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN SELECT * FROM top_product_per_category;"
                title="EXPLAIN top_product_per_category"
              />
            </RunnableContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Idiomatic SQL: rewrite aggregate-as-window.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            <Code variant="inline-syntax">COUNT(*) OVER (PARTITION BY …)</Code>{" "}
            and friends are window functions even when the aggregate isn&apos;t
            obvious. In a maintained view, any insert/update/delete forces a
            full per-partition recompute. Rewrite as a separate{" "}
            <Code variant="inline-syntax">GROUP BY</Code> view (or CTE) and
            join.
          </Text>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Per-order line-item count, then joined back into{" "}
            <Code variant="inline-syntax">order_items</Code>.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
CREATE VIEW order_item_counts AS
SELECT order_id, COUNT(*) AS items_in_order
FROM order_items
GROUP BY order_id;`}
                title="Create order_item_counts"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW order_items_with_count AS
SELECT oi.*, oic.items_in_order
FROM order_items oi
JOIN order_item_counts oic ON oic.order_id = oi.order_id;`}
                title="Join in the per-order count"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN SELECT * FROM order_items_with_count;"
                title="EXPLAIN — should show Accumulable GroupAggregate"
              />
            </RunnableContainer>
            The plan shows{" "}
            <Code variant="inline-syntax">Accumulable GroupAggregate</Code>{" "}
            (incremental) instead of{" "}
            <Code variant="inline-syntax">Non-incremental GroupAggregate</Code>.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Temporal filters with mz_now().",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            A{" "}
            <TextLink
              target="_blank"
              href={docUrls["/docs/transform-data/patterns/temporal-filters/"]}
            >
              temporal filter
            </TextLink>{" "}
            uses <Code variant="inline-syntax">mz_now()</Code> in a{" "}
            <Code variant="inline-syntax">WHERE</Code> clause to age records out
            of a sliding window. Two constraints to remember when using
            <Code variant="inline-syntax"> mz_now()</Code> in a materialized
            view, indexed view, or{" "}
            <Code variant="inline-syntax">SUBSCRIBE</Code>:
          </Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <strong>Comparison only.</strong>{" "}
              <Code variant="inline-syntax">mz_now()</Code> only participates in
              comparison operators. Move any arithmetic to the other side of the
              comparison.
            </ListItem>
            <ListItem textStyle="text-base">
              <strong>AND only.</strong> Top-level{" "}
              <Code variant="inline-syntax">WHERE</Code> conditions including{" "}
              <Code variant="inline-syntax">mz_now()</Code> must be{" "}
              <Code variant="inline-syntax">AND</Code>-combined. Rewrite{" "}
              <Code variant="inline-syntax">OR</Code> with{" "}
              <Code variant="inline-syntax">UNION</Code>.
            </ListItem>
          </UnorderedList>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Per-user spending in the last 1 minute, plus a top-100 view on top.
            The artificial 1-minute window lets you watch records enter and
            leave the result set live.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
CREATE VIEW user_spending_1min AS
SELECT
    o.user_id,
    u.name AS user_name,
    u.email AS user_email,
    COUNT(*) AS total_orders,
    SUM(o.total) AS spend_1min,
    AVG(o.total) AS avg_order_value_1min,
    MAX(o.created_at) AS last_order_date
FROM orders o
JOIN users u ON u.id = o.user_id
WHERE o.created_at + INTERVAL '1 min' > mz_now()
GROUP BY o.user_id, u.name, u.email;`}
                title="user_spending_1min — uses a temporal filter"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW top_100_customers_1min AS
SELECT user_id, user_name, user_email, total_orders,
       spend_1min, avg_order_value_1min, last_order_date
FROM user_spending_1min
ORDER BY spend_1min DESC
LIMIT 100;`}
                title="top_100_customers_1min"
              />
              <Runnable
                runCommand={runCommand}
                value="SUBSCRIBE TO top_100_customers_1min;"
                title="Subscribe — watch rows enter/leave"
              />
            </RunnableContainer>
            To exit the subscribe, click <strong>Stop streaming</strong>.
          </ListItem>
          <ListItem textStyle="text-base">
            Combine the top-K idiom with a temporal filter: top-5 most-viewed
            products per category in the last 24 hours, from the{" "}
            <Code variant="inline-syntax">clickstream</Code> view.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW IF NOT EXISTS product_view_counts_24h AS
SELECT
    p.id          AS product_id,
    p.name        AS product_name,
    p.category_id,
    COUNT(*) AS view_count
FROM clickstream ce
JOIN products p ON p.id = ce.product_id
WHERE ce.event_type = 'product_view'
  AND ce.event_time + INTERVAL '24 hours' > mz_now()
GROUP BY p.id, p.name, p.category_id;`}
                title="product_view_counts_24h"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE VIEW IF NOT EXISTS top_5_viewed_products_per_category_24h AS
SELECT
    c.id   AS category_id,
    c.name AS category_name,
    r.product_id,
    r.product_name,
    r.view_count
FROM categories c,
LATERAL (
    SELECT product_id, product_name, view_count
    FROM product_view_counts_24h
    WHERE category_id = c.id
    ORDER BY view_count DESC
    LIMIT 5
) r;`}
                title="top_5_viewed_products_per_category_24h"
              />
            </RunnableContainer>
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "Filter pushdown: when to materialize first.",
    render: ({ runCommand, title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            Two patterns for combining a temporal filter with a join:
          </Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <strong>Filter-then-materialize.</strong> Filter is part of the
              materialized view; the storage layer pushes the temporal predicate
              down so only currently-active rows enter the join. Bounded by the
              active set. Good when records activate at a relatively steady
              rate.
            </ListItem>
            <ListItem textStyle="text-base">
              <strong>Materialize-then-filter.</strong> First MV joins
              everything without the temporal filter; a second MV applies the
              filter on top. Steady-state memory holds all rows; activation
              bursts (e.g. a big promotion going live) reveal already-joined
              rows instead of triggering a join surge.
            </ListItem>
          </UnorderedList>
        </TextContainer>
        <OrderedList spacing={3}>
          <ListItem textStyle="text-base">
            Filter-then-materialize: a single MV with the temporal predicate
            inline.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`SET CLUSTER = 'transform_cluster';
CREATE MATERIALIZED VIEW currently_featured_all_in_one AS
SELECT
    f.id    AS feature_id,
    f.name  AS feature_name,
    p.id    AS product_id,
    p.name  AS product_name,
    p.price
FROM featured_products f
JOIN products p ON p.id = f.product_id
WHERE mz_now() >= f.start_date
  AND mz_now() <  f.end_date;`}
                title="Filter-then-materialize MV"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN MATERIALIZED VIEW currently_featured_all_in_one;"
                title="EXPLAIN — look for pushdown= annotation"
              />
            </RunnableContainer>
          </ListItem>
          <ListItem textStyle="text-base">
            Materialize-then-filter: two stacked MVs.
            <RunnableContainer mt={3} mb={3}>
              <Runnable
                runCommand={runCommand}
                value={`CREATE MATERIALIZED VIEW featured_product_details AS
SELECT
    f.id    AS feature_id,
    f.name  AS feature_name,
    f.start_date,
    f.end_date,
    p.id    AS product_id,
    p.name  AS product_name,
    p.price
FROM featured_products f
JOIN products p ON p.id = f.product_id;`}
                title="Step 1: precompute every feature × product pair"
              />
              <Runnable
                runCommand={runCommand}
                value={`CREATE MATERIALIZED VIEW currently_featured AS
SELECT feature_id, feature_name, product_id, product_name, price
FROM featured_product_details
WHERE mz_now() >= start_date
  AND mz_now() <  end_date;`}
                title="Step 2: apply the temporal filter on top"
              />
              <Runnable
                runCommand={runCommand}
                value="EXPLAIN MATERIALIZED VIEW currently_featured;"
                title="EXPLAIN — filter pushdown on stored state"
              />
            </RunnableContainer>
            Memory profile: filter-then-materialize ≈ active set.
            Materialize-then-filter ≈ all rows + active set. Trade memory for
            burst-resilience.
          </ListItem>
        </OrderedList>
      </>
    ),
  },
  {
    title: "MCP server for developer analysis.",
    render: ({ title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">
            The Materialize emulator exposes a built-in{" "}
            <TextLink target="_blank" href="https://modelcontextprotocol.io/">
              Model Context Protocol
            </TextLink>{" "}
            server at{" "}
            <Code variant="inline-syntax">
              http://localhost:6876/api/mcp/developer
            </Code>
            . Connect an MCP-compatible client (Claude Code, Claude Desktop,
            Cursor, etc.) and ask introspection questions like &quot;what
            sources are running and have they snapshotted?&quot;, &quot;is any
            cluster running out of memory?&quot;, or &quot;suggest
            optimizations&quot;.
          </Text>
          <Text textStyle="text-base">
            Materialize publishes two complementary agent skills on GitHub —{" "}
            <Code variant="inline-syntax">mcp-developer-analysis</Code> (for
            introspection workflows) and{" "}
            <Code variant="inline-syntax">materialize-docs</Code> (for syntax
            and concepts). Both are installable via{" "}
            <Code variant="inline-syntax">
              npx skills add MaterializeInc/agent-skills
            </Code>
            .
          </Text>
          <Text textStyle="text-base">
            Setup steps run in your terminal (see the{" "}
            <TextLink
              target="_blank"
              href="https://materialize.com/docs/integrations/mcp-server/mcp-developer/"
            >
              MCP Developer
            </TextLink>{" "}
            docs), but the in-console shortcut is to create a narrowly-scoped
            login role for the agent:
          </Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <Code variant="inline-syntax">CREATE ROLE my_dev_agent;</Code>
            </ListItem>
          </UnorderedList>
          <Text textStyle="text-base">
            The role inherits the default{" "}
            <Code variant="inline-syntax">PUBLIC</Code> privileges, which let it
            read the system catalog through the MCP server&apos;s
            <Code variant="inline-syntax">mz_catalog_server</Code> system
            cluster. From your terminal, point your MCP client at{" "}
            <Code variant="inline-syntax">
              http://localhost:6876/api/mcp/developer
            </Code>{" "}
            with Basic auth header{" "}
            <Code variant="inline-syntax">my_dev_agent:</Code> (no password) and
            you&apos;re wired up.
          </Text>
        </TextContainer>
      </>
    ),
  },
  {
    title: "Summary and what to explore next.",
    render: ({ title }) => (
      <>
        <TextContainer>
          <Text textStyle="heading-md">{title}</Text>
          <Text textStyle="text-base">In this tutorial you covered:</Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              The three-cluster architecture (source / transform / serving) and
              cluster isolation.
            </ListItem>
            <ListItem textStyle="text-base">
              Views (re-computed per query), materialized views (persisted,
              incremental, cross-cluster), and indexes (in-memory,
              cluster-local, hashed-key).
            </ListItem>
            <ListItem textStyle="text-base">
              Index reuse and why creation order matters for maintained objects.
            </ListItem>
            <ListItem textStyle="text-base">
              Idiomatic Materialize SQL for top-K (
              <Code variant="inline-syntax">LATERAL</Code> +{" "}
              <Code variant="inline-syntax">LIMIT</Code>,{" "}
              <Code variant="inline-syntax">DISTINCT ON</Code>) and aggregate
              rewrites.
            </ListItem>
            <ListItem textStyle="text-base">
              Temporal filters with{" "}
              <Code variant="inline-syntax">mz_now()</Code> and the
              filter-pushdown / pre-materialize trade-off.
            </ListItem>
            <ListItem textStyle="text-base">
              The built-in MCP server for connecting AI agents to your
              environment&apos;s system catalog.
            </ListItem>
          </UnorderedList>
          <Text textStyle="heading-sm">Where to go next</Text>
          <UnorderedList>
            <ListItem textStyle="text-base">
              <TextLink
                target="_blank"
                href={docUrls["/docs/concepts/clusters/"]}
              >
                Clusters concept docs
              </TextLink>
            </ListItem>
            <ListItem textStyle="text-base">
              <TextLink
                target="_blank"
                href={docUrls["/docs/concepts/indexes/"]}
              >
                Indexes concept docs
              </TextLink>
            </ListItem>
            <ListItem textStyle="text-base">
              <TextLink
                target="_blank"
                href={
                  docUrls[
                    "/docs/transform-data/idiomatic-materialize-sql/appendix/idiomatic-sql-chart/"
                  ]
                }
              >
                Idiomatic Materialize SQL chart
              </TextLink>
            </ListItem>
            <ListItem textStyle="text-base">
              <TextLink
                target="_blank"
                href={
                  docUrls["/docs/transform-data/patterns/temporal-filters/"]
                }
              >
                Temporal-filter patterns
              </TextLink>
            </ListItem>
          </UnorderedList>
        </TextContainer>
      </>
    ),
  },
];
