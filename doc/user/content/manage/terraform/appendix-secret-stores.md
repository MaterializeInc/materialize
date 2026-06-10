---
title: "Appendix: External secret stores"
description: ""
disable_toc: true
menu:
  main:
    parent: "manage-terraform"
    weight: 50
---

Materialize does not directly integrate with external secret stores, but it's possible to manage this integration via Terraform.

The [secret stores demo](https://github.com/MaterializeInc/demos/tree/main/integrations/terraform/secret-stores) shows how to handle [secrets](/sql/create-secret) and sensitive data with some popular secret stores. By utilizing Terraform's infrastructure-as-code model, you can automate and simplify both the initial setup and ongoing management of secret stores with Materialize.

A popular secret store is [HashiCorp Vault](https://www.vaultproject.io/). To use Vault with Materialize, you'll need to install the Terraform Vault provider:

```hcl
terraform {
  required_providers {
    vault = {
      source  = "hashicorp/vault"
      version = "~> 3.15"
    }
  }
}

provider "vault" {
  address = "https://vault.example.com"
  token   = "your-vault-token"
}
```

Next, fetch a secret from Vault and use it to create a new Materialize secret:

```hcl
data "vault_generic_secret" "materialize_password" {
  path = "secret/materialize"
}

resource "materialize_secret" "example_secret" {
  name  = "pgpass"
  value = data.vault_generic_secret.materialize_password.data["pgpass"]
}
```

In this example, the `vault_generic_secret` data source retrieves a secret from Vault, which is then used as the value for a new `materialize_secret` resource.

You can find examples of using other popular secret stores providers in the
[secret stores
demo](https://github.com/MaterializeInc/demos/tree/main/integrations/terraform/secret-stores).
