+++
title = "Bring Your Own Cloud (BYOC) on GCP"
description = "Run Materialize inside your own GCP project while Materialize manages provisioning, upgrades, and operations."

[menu.main]
parent = "byoc"
name = "GCP"
identifier = "byoc-gcp"
weight = 60
+++

{{< warning >}}

Early draft, work in progress. Shared for feedback on the BYOC on GCP MVP design. Not official documentation; flows and details will change.

{{< /warning >}}

Materialize Bring Your Own Cloud (BYOC) runs a full Materialize environment inside your own GCP project. Your data never leaves your project: the VPC network, compute (GKE), metadata database (Cloud SQL), and object storage (Cloud Storage) are all created in your project. Materialize manages provisioning, upgrades, and day-to-day operations from its control plane.

BYOC is a good fit if you need:

- **Data residency**: your data stays in your own GCP project and VPC.
- **Network isolation**: no data path through Materialize-managed infrastructure.
- **Compliance**: SOC 2 / HIPAA controls that require infrastructure ownership.
- **Cost visibility**: compute and storage are billed directly to your GCP project.

{{< note >}}
BYOC is set up together with the Materialize team. This guide describes the steps; your Materialize contact coordinates the handoff. BYOC on GCP requires an active BYOC subscription.
{{< /note >}}

## How it works

Your environment runs entirely in a dedicated GCP project that you create for Materialize. Materialize provisions and operates it using an identity you grant admin on that project through Workload Identity Federation; the project itself is the isolation boundary. Only operational telemetry (logs and metrics, with sensitive values redacted) leaves your project so Materialize can monitor and support the deployment.

![BYOC on GCP architecture](/images/byoc-gcp-architecture.svg)

## Prerequisites

- A dedicated GCP project for Materialize, and the region you want to run in.
- Permission to configure Workload Identity Federation and grant IAM roles on that project.
- An active Materialize BYOC subscription.

## Step 1: Share setup details

Materialize provides its OIDC issuer details and a setup script. You confirm your target project ID, region, and network details with your Materialize contact for provisioning.

## Step 2: Grant access

Run the provided setup in your dedicated project. It configures Workload Identity Federation so your project trusts Materialize's OIDC issuer, and grants the federated identity an admin role on the project, scoped to that project only. Materialize's deployer federates in from its own infrastructure to obtain short-lived credentials; no service-account keys are created or shared, and Materialize runs nothing inside your project's control plane to authenticate.

{{< note >}}
The project should be dedicated to Materialize and contain no other resources. The project is the isolation boundary: Materialize's access does not extend to any of your other projects. (Draft: the federation setup tooling and whether the granted role is Owner or a custom admin role are still being finalized.)
{{< /note >}}

## Step 3: Materialize provisions your environment

Using the access you granted, Materialize provisions your environment in your project: networking (VPC and subnets), compute (GKE with autoscaling), the metadata database (Cloud SQL), object storage (Cloud Storage), and the Materialize instance.

## Step 4: Connect

Once provisioning completes, Materialize shares your connection details. Establish a Private Service Connect connection from your VPC to reach the environment privately.

## Security model

- **No standing keys.** Materialize authenticates through Workload Identity Federation: your project trusts Materialize's OIDC issuer and issues short-lived credentials on demand. No service-account keys are created or stored.
- **A dedicated, isolated project.** Materialize's admin is scoped to the dedicated project you create; it does not extend to your other projects, your organization, or billing.
- **Single-tenant isolation.** Dedicated GKE, VPC, Cloud SQL, and Cloud Storage in your project. Data at rest is encrypted with your own Cloud KMS keys.
- **You hold the kill switch.** Remove the Workload Identity Federation trust or the granted IAM role, or delete the project, at any time to cut access. While access is revoked, your environment keeps serving queries but cannot be upgraded, scaled, or repaired until access is restored.
- **Audited support access.** Materialize support uses scoped, time-bound, audited access. Every action the federated identity takes is recorded in your project's Cloud Audit Logs.

## Observability

Your data stays in your project. Operational telemetry is collected so Materialize can monitor and support your deployment, and sensitive values in logs are redacted before anything leaves your project. A copy of your metrics and logs is also kept in your project so you can query it with your own tools.

## Upgrades

Materialize keeps your environment current, applying version upgrades the same way as Materialize Cloud.

## Other clouds

BYOC is available on AWS today; GCP is in development and Azure is on the roadmap. See [BYOC on AWS](/byoc/byoc-aws-draft/).

## Need help?

Contact your Materialize representative, or reach the [Materialize support team](https://materialize.com/contact).
