---
title: "Bring Your Own Cloud (BYOC) on AWS"
description: "Run Materialize inside your own AWS account while Materialize manages provisioning, upgrades, and operations."
menu:
  main:
    parent: "installation"
    weight: 50
---

{{< warning >}}

Early draft, work in progress. Shared for feedback on the BYOC on AWS MVP design. Not official documentation; flows and details will change.

{{< /warning >}}

Materialize Bring Your Own Cloud (BYOC) runs a full Materialize environment
inside your own AWS account. Your data never leaves your account: the VPC,
compute (EKS), metadata database (RDS), and object storage (S3) are all created
in your account. Materialize manages provisioning, upgrades, and day-to-day
operations from its control plane.

BYOC is a good fit if you need:

- **Data residency**: your data stays in your own cloud account and VPC.
- **Network isolation**: no data path through Materialize-managed infrastructure.
- **Compliance**: SOC 2 / HIPAA controls that require infrastructure ownership.
- **Cost visibility**: compute and storage are billed directly to your AWS account.

{{< note >}}
BYOC is set up together with the Materialize team. This guide describes the
steps; your Materialize contact coordinates the handoff. BYOC on AWS requires an
active BYOC subscription.
{{< /note >}}

## How it works

Your environment runs entirely in your AWS account. Materialize provisions and
operates it across your account boundary using an IAM role you create, scoped by
a permission boundary you control. Only operational telemetry (logs and metrics,
with sensitive values redacted) leaves your account so Materialize can monitor
and support the deployment.

## Prerequisites

- An AWS account, and the AWS region you want to run in.
- Permission to run CloudFormation and create IAM roles in that account.
- An active Materialize BYOC subscription.

## Step 1: Share setup details

Materialize sends you a **pre-filled CloudFormation quick-create link**. Opening
it loads the CloudFormation console in your own AWS account with the required
values (Materialize's AWS account ID, an external ID, a deployer role ARN, and a name prefix)
already populated, so there is nothing to copy by hand. You confirm your target
region, VPC CIDR, and availability zones with your Materialize contact for
provisioning.

## Step 2: Launch the stack

Open the quick-create link while signed in to the AWS account and region where
you want Materialize to run. The CloudFormation form is already populated, so you
review it and launch the stack (about 5 to 10 minutes). It creates exactly two
objects:

- **An IAM role** (`MaterializeBYOCRole`) that Materialize assumes to manage your
  environment.
- **A permission boundary** (`MaterializeBYOCBoundary`) that caps what that role
  can ever do.

When the stack finishes, return the role ARN and permission boundary ARN to
Materialize.

{{< note >}}
The quick-create link only works when you are signed in to the intended AWS
account and region. Switch to the correct account before opening it.
{{< /note >}}

## Step 3: Materialize provisions your environment

Using the role you created, Materialize provisions your environment in your
account: networking (VPC and subnets), compute (EKS with autoscaling), the
metadata database (RDS), object storage (S3), and the Materialize instance.
Provisioning takes roughly one hour.

## Step 4: Connect

Once provisioning completes, Materialize shares your connection details.
Establish an AWS PrivateLink connection from your VPC to reach the environment
privately.

## Security model

- **No standing credentials.** Materialize assumes the role you create using
  short-lived STS credentials with an external ID for confused-deputy
  protection. No long-lived keys are stored.
- **A permission ceiling.** A transitive permission boundary prevents Materialize
  from acting beyond the agreed scope, creating roles outside it, or removing its
  own boundary.
- **Single-tenant isolation.** Dedicated EKS, VPC, RDS, and S3 in your account.
  Data at rest is encrypted with your own KMS keys.
- **You hold the kill switch.** Revoke the role's trust policy at any time to cut
  access. While access is revoked, your environment keeps serving queries but
  cannot be upgraded, scaled, or repaired until access is restored.
- **Audited support access.** Materialize support uses scoped, time-bound,
  audited access. Every action the role takes is recorded in your own AWS
  CloudTrail.

## Observability

Your data stays in your account. Operational telemetry is collected so
Materialize can monitor and support your deployment, and sensitive values in logs
are redacted before anything leaves your account. A copy of your metrics and logs
is also kept in your account (Loki for logs and a Prometheus-compatible
store for metrics) so you can query it with your own tools.

## Upgrades

Materialize keeps your environment current, applying version upgrades the same
way as Materialize Cloud.

## Other clouds

BYOC launches on AWS first. Support for GCP and Azure is on the roadmap. If you
run on GCP or Azure and are interested in BYOC, let your Materialize contact know
so we can factor your needs into sequencing.

## Need help?

Contact your Materialize representative, or reach the
[Materialize support team](https://materialize.com/contact).
