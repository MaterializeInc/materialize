// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Form values for the onboarding survey that's submitted via Hubspot

export const OTHER_OPTION = { label: "Other", value: "Other" } as const;

export const ROLE_DESCRIPTION_OPTIONS = [
  { label: "Data Engineer", value: "Data Engineer" },
  { label: "Data Analyst", value: "Data Analyst" },
  { label: "ML Engineer", value: "ML Engineer" },
  { label: "Software Engineer", value: "Software Engineer" },
  { label: "Data Architect", value: "Data Architect" },
  { label: "Database Administrator", value: "Database Administrator" },
  { label: "Technical Executive", value: "Technical Executive" },
  { label: "Student", value: "Student" },
  OTHER_OPTION,
] as const;

export type RoleDescription =
  (typeof ROLE_DESCRIPTION_OPTIONS)[number]["value"];

export const MATERIALIZE_USE_CASE_OPTIONS = [
  {
    label: "My company is evaluating Materialize",
    value: "My company is evaluating Materialize",
  },
  {
    label: "I’m evaluating Materialize for a client",
    value: "I’m evaluating Materialize for a client",
  },
  {
    label: "I’m exploring new technologies",
    value: "I’m exploring new technologies",
  },
  OTHER_OPTION,
] as const;

export type MaterializeUseCase =
  (typeof MATERIALIZE_USE_CASE_OPTIONS)[number]["value"];

export const PROJECT_DESCRIPTION_OPTIONS = [
  {
    label: "Improve the performance of complex reads",
    value: "Improve the performance of complex reads",
  },
  {
    label: "Join and act on data from multiple sources",
    value: "Join and act on data from multiple sources",
  },
  {
    label: "Share operational data across teams and services",
    value: "Share operational data across teams and services",
  },
  OTHER_OPTION,
] as const;

export type ProjectDescription =
  (typeof PROJECT_DESCRIPTION_OPTIONS)[number]["value"];

export const PRIMARY_DATA_SOURCE_OPTIONS = [
  {
    label:
      "PostgreSQL (Amazon RDS, Amazon Aurora, Azure DB, Google Cloud SQL, AlloyDB, Self-hosted PostgresSQL)",
    value: "PostgreSQL",
  },
  {
    label:
      "MySQL (Amazon RDS, Amazon Aurora, Azure DB, Google Cloud SQL, Self-hosted MySQL)",
    value: "MySQL",
  },
  {
    label:
      "Kafka (Apache Kafka, Confluent Cloud, Amazon MSK, WarpStream, Redpanda (Cloud))",
    value: "Kafka",
  },
  {
    label:
      "Cloud Data Warehouse (Amazon Redshift, Google BigQuery, Microsoft Azure, Snowflake)",
    value: "Cloud Data Warehouse",
  },
  {
    label: "Object storage (AWS S3, GCP Cloud Storage, Azure Blob Storage)",
    value: "Object storage / S3",
  },
  { label: "Webhooks", value: "Webhooks" },
  OTHER_OPTION,
] as const;

export type PrimaryDataSource =
  (typeof PRIMARY_DATA_SOURCE_OPTIONS)[number]["value"];
