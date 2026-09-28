---
title: "Add to an existing installation"
description: "Add the advanced SSO stack to a Materialize deployment you already run with the Terraform modules."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-existing"
    weight: 15
---

If Materialize is already running, you can add the advanced SSO stack
without redeploying it. This guide assumes you manage Materialize with the
[Materialize Terraform modules](https://github.com/MaterializeInc/materialize-terraform-self-managed),
including the `materialize-instance` module.

The change happens in two applies, so you can check the stack before any
user signs in through it:

1. Add the `ory-stack` module and check that it's healthy. Materialize keeps
   using its current authentication.
2. Point Materialize at Hydra.

## Before you begin

- Complete the [prerequisites](/self-managed-deployments/sso/advanced/prerequisites/).
- Create PostgreSQL databases for Kratos and Hydra, plus one for Polis if
  you plan to enable SAML. They can live on your existing PostgreSQL server
  or on a dedicated instance.

## Step 1: Add the Ory stack

1. Add the `ory-stack` module next to your existing modules, substituting
   your own hostnames, database connection strings, and `ClusterIssuer`:

   ```hcl
   module "ory" {
     source = "github.com/MaterializeInc/materialize-terraform-self-managed//kubernetes/modules/ory-stack?ref=<RELEASE_TAG>"

     namespace = "ory"

     hydra_fqdn  = "hydra.example.com"
     kratos_fqdn = "kratos.example.com"
     ui_fqdn     = "auth.example.com"

     kratos_dsn = "postgres://<user>:<password>@<host>:5432/kratos?sslmode=require"
     hydra_dsn  = "postgres://<user>:<password>@<host>:5432/hydra?sslmode=require"

     # Use the ory_oel_image_tag default from the enterprise example at the same release.
     oel_image_tag   = "<ORY_IMAGE_TAG>"
     license_key_jwt = var.license_key

     cert_issuer_ref = {
       name = "letsencrypt-prod"
       kind = "ClusterIssuer"
     }
     # true for an in-cluster self-signed issuer, false for a public ACME issuer
     cert_issuer_signs_cluster_local = false

     # Registers the Materialize Console as an OAuth2 client in Hydra.
     materialize_namespace    = "materialize-environment"
     materialize_console_fqdn = "console.example.com"
   }

   output "ory_lb_addresses" {
     value = module.ory.lb_addresses
   }
   ```

   The load balancer settings differ by cloud. Copy `lb_annotations`, and on
   AWS `lb_load_balancer_class` and `lb_external_traffic_policy`, from the
   `module "ory"` block in the enterprise example for your cloud
   ([AWS](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/enterprise),
   [Azure](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure/examples/enterprise),
   [GCP](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp/examples/enterprise)).
   To enable SAML, also set `enable_polis`, `polis_fqdn`, and `polis_dsn`;
   see [Configure identity providers](/self-managed-deployments/sso/advanced/identity-providers/).

1. Apply:

   ```bash
   terraform init -upgrade
   terraform apply
   ```

1. Create DNS records pointing the Hydra, Kratos, and selfservice UI
   hostnames at the addresses in `terraform output ory_lb_addresses`: an A
   record for an IP (Azure, GCP) or a CNAME for a hostname (AWS).

1. Check that Hydra serves its discovery document, with an `issuer` that
   matches `hydra_fqdn`:

   ```bash
   curl -fsSL https://hydra.example.com/.well-known/openid-configuration | jq .issuer
   ```

## Step 2: Point Materialize at Hydra

1. In your existing `materialize-instance` module, switch authentication to
   OIDC and set the OIDC parameters from the `ory-stack` outputs:

   ```hcl
   module "materialize_instance" {
     # ... your existing settings ...

     authenticator_kind = "Oidc"
     # Password for the mz_system admin user, kept as a fallback under OIDC.
     external_login_password_mz_system = var.external_login_password_mz_system

     # With network policies enabled, lets Materialize reach Hydra for its signing keys.
     ory_namespace = "ory"

     system_parameters = {
       oidc_issuer                  = module.ory.hydra_external_url
       oidc_audience                = jsonencode([module.ory.oauth2_client_id])
       oidc_authentication_claim    = "email"
       console_oidc_client_id       = module.ory.oauth2_client_id
       console_oidc_scopes          = "openid email"
       # Optional: grant roles from IdP groups (see Operations).
       oidc_group_role_sync_enabled = "true"
     }

     # Set a new UUID so environmentd restarts with the new parameters.
     force_rollout = "<NEW_UUID>"
   }
   ```

   Users keep their existing SQL roles as long as the `email` claim matches
   the role names. If your users currently sign in with passwords, see
   [Migrate to SSO](/security/self-managed/sso-migration/).

1. Apply:

   ```bash
   terraform apply
   ```

## Step 3: Verify sign-in

{{% include-headless "/headless/self-managed-deployments/enterprise-sso/verify" %}}

## Next steps

- [Configure identity providers](/self-managed-deployments/sso/advanced/identity-providers/)
- [Enable role mapping](/self-managed-deployments/sso/advanced/operations/#enable-role-mapping)
