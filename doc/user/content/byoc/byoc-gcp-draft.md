+++
title = "Bring Your Own Cloud (BYOC) on GCP"
description = "Run Materialize inside your own GCP project while Materialize manages provisioning, upgrades, and operations."

[menu.main]
parent = "byoc"
name = "GCP"
identifier = "byoc-gcp"
weight = 50
+++

{{< warning >}}

Early draft, work in progress. Shared for feedback on the BYOC on GCP MVP design. Not official documentation; flows and details will change.

{{< /warning >}}

Materialize Bring Your Own Cloud (BYOC) runs a full Materialize environment inside your own GCP project. Your data never leaves your project: the VPC network, compute (GKE), metadata database (Cloud SQL), and object storage (Cloud Storage) are all created in your project. Materialize manages provisioning, upgrades, and day-to-day operations from its control plane.

BYOC is a good fit if you need:

- **Data residency**: your data stays in your own GCP project and VPC.
- **Network isolation**: your query traffic never traverses Materialize-managed infrastructure.
- **Compliance**: when direct control of the underlying infrastructure is necessary or more suitable for your requirements.
- **Cost visibility**: compute and storage are billed directly to your GCP project.

{{< note >}}
BYOC is set up together with the Materialize team. This guide describes the steps; your Materialize contact coordinates the handoff. BYOC on GCP requires an active BYOC subscription.
{{< /note >}}

## How it works

Your environment runs entirely in a dedicated GCP project that you create for Materialize. The project is the isolation boundary. Materialize provisions and operates the environment using an identity to which you grant admin on that project, accessed through Workload Identity Federation. Operational telemetry (logs and metrics) leaves your project so Materialize can monitor and support the deployment. Sensitive data is excluded at the application level, so it is never written into a log or a metric in the first place.
![BYOC on GCP architecture](/images/byoc-gcp-architecture.svg)

## Prerequisites

- A dedicated GCP project for Materialize, and the region you want to run in.
- Permission to configure Workload Identity Federation and grant IAM roles on that project.
- Quota in that project and region for the machine types, local SSDs, and IP addresses your environment needs. Your Materialize contact will size this with you.
- No inherited organization or folder policy that blocks the permissions Materialize needs. Materialize cannot detect these in advance, so this is worth checking before you start.

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

Once provisioning completes, Materialize shares your connection details.

Both private and public access are supported, and you choose which you want at provisioning time: a private endpoint reachable only from inside your own network, or a public endpoint with IP allowlisting. Reaching your upstream sources from Materialize is ordinary GCP networking, using VPC peering, Shared VPC, Cloud Interconnect, Cloud VPN, or a Private Service Connect endpoint you create.

{{< note >}}
Draft: the access model is chosen when your environment is provisioned and is not straightforward to change afterwards, so raise your preference with your Materialize contact early.
{{< /note >}}

## Security model

- **No standing keys.** Materialize authenticates through Workload Identity Federation: your project trusts Materialize's OIDC issuer and issues short-lived credentials on demand. No service-account keys are created or stored.
- **A dedicated, isolated project.** Materialize's admin is scoped to the dedicated project you create; it does not extend to your other projects, your organization, or billing.
- **Single-tenant isolation.** Dedicated GKE, VPC, Cloud SQL, and Cloud Storage in your project. Data at rest is encrypted with your own Cloud KMS keys.
- **You hold the kill switch.** Remove the Workload Identity Federation trust or the granted IAM role, or delete the project, at any time to cut access. While access is revoked, your environment keeps serving queries but cannot be upgraded, scaled, or repaired until access is restored.
- **Audited support access.** Materialize support uses scoped, time-bound, audited access. Every action the federated identity takes is recorded in your project's Cloud Audit Logs.

## Observability

A full monitoring stack is deployed in your project as part of provisioning: Loki for logs, a Prometheus-compatible store for metrics, and Grafana at a real hostname with TLS, along with dashboards and alert rules. It is yours, under your retention, and you can point your own tools at it.

The same set of logs and metrics is emitted both to your stack and to Materialize, so that Materialize can monitor and support the deployment. Sensitive data is excluded at the application level rather than filtered on the way out, so row-level data from your tables and views is never written into a log or a metric.

## Upgrades

Materialize keeps your environment current, applying version upgrades the same way as Materialize Cloud: about weekly, driven from the Materialize control plane. Upgrades are rolling, so a new instance comes up alongside the old one before the old one is removed, and your project needs enough headroom for both. Major versions are not skipped, and downgrades are not supported.

## Frequently asked questions

### How do I get started, and what do I need ready?

You need an empty GCP project, rights to configure Workload Identity Federation on it, quota for machines plus local SSDs and IPs, and no inherited org policy blocking the permissions we need. You grant us federated access, hand back a couple of resource names, and the console provisions the rest. Expect roughly an hour for the stack and 15 minutes for an environment.

### What access does Materialize have to my environment?

Admin scoped to that one project via Workload Identity Federation, with no service account keys and every action landing in your own Cloud Audit Logs. We manage the environment through that grant; telemetry and usage flow outbound-only from your account over mutual TLS, with a per-stack certificate we can revoke. Revoke the trust at any time and the environment keeps serving queries but stops being upgradeable or repairable.

### What data leaves my account?

Logs and metrics, with sensitive data excluded at the application level. The same set is emitted to your stack and to ours. Your copy stays in your account under your retention.

### What can Materialize support actually see and do?

Access is session bound, time boxed, fully recorded, and used for support reasons only; Kubernetes and cloud audit logs land in your own account. `mz_support` reaches usage data and can create or resize replicas; `mz_system` is denied. Usage data includes catalog metadata and redacted query text, so schema and column names are visible to support.

### How do people authenticate?

Ory runs inside your own cluster and federates to your IdP, so your identity data never reaches Materialize and your groups map to Materialize roles. A sealed local admin credential stays available as break-glass. Machine and service account auth for tools like dbt, sinks and CI is still being designed, so this answer covers people only.

### How do my applications and my sources connect?

Both private and public access are supported, and you choose at provisioning: a private endpoint inside your own network, or a public one with IP allowlisting. Reaching your sources is ordinary GCP networking: peering, Shared VPC, Interconnect, Cloud VPN, or a PSC endpoint you create.

### How do upgrades work?

About weekly (could be more frequent), similar to our cloud service, driven from our control plane.

## Other clouds

BYOC is in development, on GCP first with AWS to follow; Azure is on the roadmap. See [BYOC on AWS](/byoc/byoc-aws-draft/).

## Need help?

Contact your Materialize representative, or reach the [Materialize support team](https://materialize.com/contact).
