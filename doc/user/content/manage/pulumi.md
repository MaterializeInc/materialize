---
title: "Use Pulumi to manage Materialize"
description: "Create and manage Materialize resources with Pulumi"
menu:
  main:
    parent: "manage"
    weight: 11
    name: "Use Pulumi to manage Materialize"
---

[Pulumi](https://www.pulumi.com/) is an infrastructure-as-code tool that enables you to manage your resources on any cloud, using a number of different programming languages. Materialize maintains a
[Pulumi provider](https://github.com/MaterializeInc/pulumi-materialize) to help you safely and predictably provision and manage
connections, sources, and other database objects.

## Pulumi provider

Using the Materialize provider will depend on the SDK you select:

{{< tabs >}}
{{< tab "Python">}}
```shell
pip install pulumi-materialize
```
{{< /tab >}}
{{< /tabs >}}

### Authentication

To configure the provider to communicate with your Materialize region, you
need to authenticate with a Materialize username, app password, and other
specifics from your account.

{{< note >}}
Materialize recommends creating a new app password for each application you use. To create a new app password, navigate to [https://console.materialize.com/access](https://console.materialize.com/access).
{{</ note >}}

Materialize recommends saving sensitive configuration variables as secrets within Pulumi..

```shell
pulumi config set --secret materialize:password <app_password>
```

In the `Pulumi.dev.yaml` file, add the provider configuration and any variable
references:

```yaml
config:
  materialize:host: <host>
  materialize:user: <user>
  materialize:password:
    secure: <hash from pulumi config set>
  materialize:port: "6875"
  materialize:database: materialize
```

### Pulumi resources

The Pulumi provider allows you to create several resource types in your
region. Resources correspond to Materialize objects and are configured
using the various SDKs.

For example, to create a new cluster, you would use the `Cluster` resource:

{{< tabs >}}
{{< tab "Python">}}
```python
import pulumi_materialize

pulumi_materialize.Cluster(
    name="example-cluster",
    size="cluster",
)
```
{{< /tab >}}
{{< /tabs >}}

### Create Materialize data sources

The Materialize provider supports several data source types to retrieve
information about your existing Materialize resources. Data sources can return
details about objects defined outside of Pulumi that can still be used within 
your project.

For example, to retrieve your current clusters:

{{< tabs >}}
{{< tab "Python">}}
```python
import pulumi_materialize

clusters = pulumi_materialize.get_clusters()
```
{{< /tab >}}
{{< /tabs >}}

This data source returns all cluster names and IDs which you can use as
variables for new resources.

## Contributing

If you want to help develop the Materialize provider, check out the [contribution guidelines](https://github.com/MaterializeInc/pulumi-materialize/blob/main/CONTRIBUTING.md).

{{< note >}}
The Pulumi provider is a bridge for the [Terraform provider](https://github.com/MaterializeInc/terraform-provider-materialize). New features and enhancements to resources should first be added to Terraform.
{{< /note >}}
