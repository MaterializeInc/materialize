// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import awsAuroraLogo from "~/img/integrations/aws_aurora.svg";
import awsMSKLogo from "~/img/integrations/aws_msk.svg";
import awsPrivateLinkLogo from "~/img/integrations/aws_privatelink.svg";
import awsRDSLogo from "~/img/integrations/aws_rds.svg";
import azureDbMysqlLogo from "~/img/integrations/azure_db_mysql.svg";
import censusLogo from "~/img/integrations/census.svg";
import confluentCloudLogo from "~/img/integrations/confluent_cloud.svg";
import cubeLogo from "~/img/integrations/cube.svg";
import datadogLogo from "~/img/integrations/datadog.svg";
import dbeaverLogo from "~/img/integrations/dbeaver.svg";
import dbtLogo from "~/img/integrations/dbt.svg";
import deepnoteLogo from "~/img/integrations/deepnote.svg";
import golangLogo from "~/img/integrations/golang.svg";
import googleCloudSqlLogo from "~/img/integrations/google_cloud_sql.svg";
import grafanaLogo from "~/img/integrations/grafana.svg";
import hexLogo from "~/img/integrations/hex.svg";
import javaLogo from "~/img/integrations/java.svg";
import kafkaLogo from "~/img/integrations/kafka.svg";
import lookerLogo from "~/img/integrations/looker.svg";
import metabaseLogo from "~/img/integrations/metabase.svg";
import mysqlLogo from "~/img/integrations/mysql.svg";
import neonLogo from "~/img/integrations/neon.svg";
import nodeLogo from "~/img/integrations/nodejs.svg";
import phpLogo from "~/img/integrations/php.svg";
import postgresLogo from "~/img/integrations/postgres.svg";
import powerbiLogo from "~/img/integrations/powerbi.svg";
import pythonLogo from "~/img/integrations/python.svg";
import redpandaLogo from "~/img/integrations/redpanda.svg";
import rubyLogo from "~/img/integrations/ruby.svg";
import rudderstackLogo from "~/img/integrations/rudderstack.svg";
import rustLogo from "~/img/integrations/rust.svg";
import segmentLogo from "~/img/integrations/segment.svg";
import sshTunnelLogo from "~/img/integrations/sshtunnel.svg";
import tableauLogo from "~/img/integrations/tableau.svg";
import terraformLogo from "~/img/integrations/terraform.svg";
import vscodeLogo from "~/img/integrations/vscode.svg";
import warpstreamLogo from "~/img/integrations/warpstream.svg";
import webhookLogo from "~/img/integrations/webhook.svg";
import materializeLogoLight from "~/img/materialize-mark-grayscale.svg";
import materializeLogoDark from "~/img/materialize-mark-white.svg";
import docUrls from "~/mz-doc-urls.json";

export interface Integration {
  imagePath: string | ((mode: "light" | "dark") => string);
  name: string;
  description: string;
  link: string;
  status: string;
  external: boolean;
  sourceType?: "webhook" | "postgresql" | "mysql" | "kafka";
}

export interface IntegrationCategory {
  label: string;
  key: string;
  children: Integration[];
}

const integrationsList: IntegrationCategory[] = [
  {
    label: "Developer Tools & SQL Clients",
    key: "developer_tooling",
    children: [
      {
        imagePath: dbeaverLogo,
        name: "DBeaver",
        description: "Use DBeaver as your SQL IDE.",
        link: `${docUrls["/docs/integrations/sql-clients/"]}#dbeaver`,
        status: "Native",
        external: true,
      },
      {
        imagePath: vscodeLogo,
        name: "VS Code",
        description: "Use VS Code as your SQL IDE.",
        link: "https://materialize.com/blog/vs-code-integration/",
        status: "Native",
        external: true,
      },
      {
        imagePath: dbtLogo,
        name: "dbt",
        description:
          "Manage your SQL transformations as code using best practices like documentation, testing and version-control.",
        link: docUrls["/docs/manage/dbt/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: terraformLogo,
        name: "Terraform",
        description:
          "Safely and predictably provision and manage clusters, connections, and other Materialize resources as code.",
        link: docUrls["/docs/manage/terraform/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: (mode) =>
          mode === "dark" ? materializeLogoDark : materializeLogoLight,
        name: "mz CLI",
        description:
          "Manage and connect to your Materialize regions from your terminal.",
        link: "https://materialize.com/docs/integrations/cli/",
        status: "Native",
        external: false,
      },
    ],
  },
  {
    label: "BI & Data Visualization",
    key: "bi_data_viz",
    children: [
      {
        imagePath: cubeLogo,
        name: "Cube",
        description:
          "Cube is a headless BI platform that makes data accessible and consistent across every application.",
        link: docUrls["/docs/integrations/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: metabaseLogo,
        name: "Metabase",
        description:
          "Create real-time dashboards based on the data maintained in Materialize.",
        link: docUrls["/docs/serve-results/bi-tools/metabase/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: lookerLogo,
        name: "Looker",
        description:
          "Create dashboards based on the data maintained in Materialize using Looker.",
        link: docUrls["/docs/serve-results/bi-tools/looker/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: tableauLogo,
        name: "Tableau",
        description:
          "Create dashboards based on the data maintained in Materialize using Tableau Cloud or Tableau Desktop.",
        link: docUrls["/docs/serve-results/bi-tools/tableau/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: powerbiLogo,
        name: "Power BI",
        description:
          "Create dashboards based on the data maintained in Materialize using Power BI.",
        link: docUrls["/docs/serve-results/bi-tools/power-bi/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: deepnoteLogo,
        name: "Deepnote",
        description:
          "Explore Materialize data in SQL and Python using a collaborative data notebook.",
        link: docUrls["/docs/serve-results/bi-tools/deepnote/"],
        status: "Partner",
        external: true,
      },
      {
        imagePath: hexLogo,
        name: "Hex",
        description:
          "Explore Materialize data in SQL and Python using a collaborative data notebook.",
        link: docUrls["/docs/serve-results/bi-tools/hex/"],
        status: "Compatible",
        external: true,
      },
    ],
  },
  {
    label: "Data Sources",
    key: "data_sources",
    children: [
      {
        imagePath: awsRDSLogo,
        name: "Amazon RDS (PostgreSQL)",
        description:
          "Ingest data in real time from Amazon RDS for PostgreSQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/postgres/amazon-rds/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: awsAuroraLogo,
        name: "Amazon Aurora (PostgreSQL)",
        description:
          "Ingest data in real time from Amazon Aurora for PostgreSQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/postgres/amazon-aurora/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: postgresLogo,
        name: "Self-hosted PostgreSQL",
        description:
          "Ingest data in real time from a self-hosted PostgreSQL instance using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/postgres/self-hosted/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: neonLogo,
        name: "Neon (PostgreSQL)",
        description:
          "Ingest data in real time from Neon PostgreSQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/postgres/neon/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: awsRDSLogo,
        name: "Amazon RDS (MySQL)",
        description:
          "Ingest data in real time from Amazon RDS for MySQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/mysql/amazon-rds/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: awsAuroraLogo,
        name: "Amazon Aurora (MySQL)",
        description:
          "Ingest data in real time from Amazon Aurora for MySQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/mysql/amazon-aurora/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: azureDbMysqlLogo,
        name: "Azure DB (MySQL)",
        description:
          "Ingest data in real time from Azure DB for MySQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/mysql/azure-db/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: googleCloudSqlLogo,
        name: "Google Cloud SQL (MySQL)",
        description:
          "Ingest data in real time from Google Cloud SQL for MySQL using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/mysql/google-cloud-sql/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: mysqlLogo,
        name: "Self-hosted MySQL",
        description:
          "Ingest data in real time from a self-hosted MySQL instance using Change Data Capture (CDC).",
        link: docUrls["/docs/ingest-data/mysql/self-hosted/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: awsMSKLogo,
        name: "Amazon MSK",
        description:
          "Ingest data in real time from Amazon MSK, a fully managed Apache Kafka service.",
        link: docUrls["/docs/ingest-data/kafka/amazon-msk/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: confluentCloudLogo,
        name: "Confluent Cloud",
        description:
          "Ingest data in real time from Confluent Cloud, an event streaming platform built on Apache Kafka.",
        link: docUrls["/docs/ingest-data/kafka/confluent-cloud/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: redpandaLogo,
        name: "Redpanda",
        description:
          "Ingest data in real time from Redpanda (Cloud), a Kafka API-compatible event streaming platform.",
        link: docUrls["/docs/ingest-data/redpanda/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: kafkaLogo,
        name: "Self-hosted Kafka",
        description:
          "Ingest data in real time from a self-hosted Kafka broker.",
        link: docUrls["/docs/ingest-data/kafka/kafka-self-hosted/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: warpstreamLogo,
        name: "WarpStream",
        description:
          "Ingest data in real time from WarpStream, a Kafka API-compatible event streaming platform.",
        link: docUrls["/docs/ingest-data/kafka/warpstream/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: segmentLogo,
        name: "Segment",
        description:
          "Ingest event data in real time from Segment, a Customer Data Platform (CDP).",
        link: docUrls["/docs/ingest-data/webhooks/segment/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: rudderstackLogo,
        name: "Rudderstack",
        description:
          "Ingest event data in real time from Rudderstack, a warehouse-native Customer Data Platform (CDP).",
        link: docUrls["/docs/ingest-data/webhooks/rudderstack/"],
        status: "Partner",
        external: true,
      },
      {
        imagePath: webhookLogo,
        name: "Webhooks",
        description:
          "Ingest webhook events in real time when specific actions happen in your SaaS applications.",
        link: docUrls["/docs/sql/create-source/webhook/"],
        status: "Native",
        external: true,
        sourceType: "webhook",
      },
    ],
  },
  {
    label: "Data Sinks",
    key: "data_sinks",
    children: [
      {
        imagePath: censusLogo,
        name: "Census",
        description:
          "Sync data maintained in Materialize in real time to downstream SaaS applications.",
        link: docUrls["/docs/serve-results/sink/census/"],
        status: "Partner",
        external: true,
      },
      {
        imagePath: awsMSKLogo,
        name: "Amazon MSK",
        description:
          "Sink data out in real time to Amazon MSK, a fully managed Apache Kafka service.",
        link: docUrls["/docs/sql/create-sink/kafka/"],
        status: "Compatible",
        external: true,
      },
      {
        imagePath: confluentCloudLogo,
        name: "Confluent Cloud",
        description:
          "Sink data out in real time to Confluent Cloud, an event streaming platform built on Apache Kafka.",
        link: docUrls["/docs/ingest-data/kafka/confluent-cloud/"],
        status: "Native",
        external: false,
      },
      {
        imagePath: redpandaLogo,
        name: "Redpanda",
        description:
          "Sink data out in real time to Redpanda (Cloud), a Kafka API-compatible event streaming platform.",
        link: docUrls["/docs/sql/create-sink/kafka/"],
        status: "Native",
        external: true,
      },
    ],
  },
  {
    label: "SQL Drivers",
    key: "sql_drivers",
    children: [
      {
        imagePath: golangLogo,
        name: "Go",
        description:
          "Interact with Materialize using the Go programming language.",
        link: docUrls["/docs/integrations/client-libraries/golang/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: javaLogo,
        name: "Java",
        description:
          "Interact with Materialize using the Java programming language.",
        link: docUrls["/docs/integrations/client-libraries/java-jdbc/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: nodeLogo,
        name: "Node.js",
        description: "Interact with Materialize using Node.js.",
        link: docUrls["/docs/integrations/client-libraries/node-js/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: phpLogo,
        name: "PHP",
        description:
          "Interact with Materialize using the PHP programming language.",
        link: docUrls["/docs/integrations/client-libraries/php/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: pythonLogo,
        name: "Python",
        description:
          "Interact with Materialize using the Python programming language.",
        link: docUrls["/docs/integrations/client-libraries/python/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: rubyLogo,
        name: "Ruby",
        description:
          "Interact with Materialize using the Ruby programming language.",
        link: docUrls["/docs/integrations/client-libraries/ruby/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: rustLogo,
        name: "Rust",
        description:
          "Interact with Materialize using the Rust programming language.",
        link: docUrls["/docs/integrations/client-libraries/rust/"],
        status: "Native",
        external: true,
      },
    ],
  },
  {
    label: "Monitoring",
    key: "monitoring",
    children: [
      {
        imagePath: grafanaLogo,
        name: "Grafana",
        description:
          "Monitor the performance and overall health of your Materialize region using Grafana.",
        link: docUrls["/docs/manage/monitor/cloud/grafana/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: datadogLogo,
        name: "Datadog",
        description:
          "Monitor the performance and overall health of your Materialize region using Datadog.",
        link: docUrls["/docs/manage/monitor/cloud/datadog/"],
        status: "Native",
        external: true,
      },
    ],
  },
  {
    label: "Security",
    key: "security",
    children: [
      {
        imagePath: awsPrivateLinkLogo,
        name: "AWS PrivateLink",
        description:
          "Tunnel connections to your sources and sinks through an AWS PrivateLink service.",
        link: docUrls["/docs/ingest-data/network-security/privatelink/"],
        status: "Native",
        external: true,
      },
      {
        imagePath: sshTunnelLogo,
        name: "SSH Tunnels",
        description:
          "Tunnel connections to your sources and sinks through an SSH tunnel.",
        link: docUrls["/docs/ingest-data/network-security/ssh-tunnel/"],
        status: "Native",
        external: true,
      },
    ],
  },
];

export default integrationsList;
