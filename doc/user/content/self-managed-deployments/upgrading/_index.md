---
title: "Upgrading"
description: "Upgrading Self-Managed Materialize."
disable_list: true
menu:
  main:
    parent: "sm-deployments"
    weight: 30
    identifier: "upgrading"
---

Materialize releases new Self-Managed versions per the schedule outlined in [Release schedule](/releases/schedule/#self-managed-release-schedule).

## General rules for upgrading

{{< include-from-yaml data="self_managed/upgrades"
name="upgrades-general-rules" >}}

{{< note >}}

{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-major-version-restriction" >}}

{{< /note >}}


## Upgrade guides

The following upgrade guides are available as examples:

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-helm" %}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-unified" %}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-landing-guides-legacy" %}}

## Upgrading the Helm Chart and Materialize Operator

{{< important >}}

When upgrading Materialize, always upgrade the Helm Chart and Materialize
Operator first.

{{</ important >}}

### Update the Helm Chart repository

To update your Materialize Helm Chart repository:

```shell
helm repo update materialize
```

View the available chart versions:

```shell
helm search repo materialize/materialize-operator --versions
```

### Upgrade your Materialize Operator

The Materialize Kubernetes Operator is deployed via Helm and can be updated
through standard `helm upgrade` command:

{{% include-syntax file="self_managed/upgrades"
example="syntax-helm-upgrade-operator" %}}

You can use `helm list` to find your release name. For example, if your Operator
is running in the namespace `materialize`, run `helm list`:

```shell
helm list -n materialize
```

Retrieve the name associated with the `materialize-operator` **CHART**; for
example, `my-demo` in the following helm list:

```none
NAME    	  NAMESPACE  	REVISION	UPDATED                             	STATUS  	CHART                                          APP VERSION
my-demo	materialize	1      2025-12-08 11:39:50.185976 -0500 EST	deployed	materialize-operator-v26.1.0    v26.1.0
```

Then, to upgrade:

```shell
helm upgrade -n materialize my-demo materialize/operator \
  -f my-values.yaml \
  --version {{< self-managed/versions/get-latest-version >}}
```

## Upgrading Materialize Instances

Select the instructions for your CRD API version:

- [v1alpha2 (v26.17+)](/self-managed-deployments/upgrading/materialize-instances/v1alpha2/)
- [v1alpha1 (before v26.17)](/self-managed-deployments/upgrading/materialize-instances/v1alpha1/)

## Version Specific Upgrade Notes

### Upgrading to `v26.17` and later versions

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.17.md" >}}

### Upgrading to `v26.1` and later versions

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.1.md" >}}

### Upgrading to `v26.0`

{{< include-md file="shared-content/self-managed/upgrade-notes/v26.0.md" >}}

### Upgrading between minor versions less than `v26`
 - Prior to `v26`, you must upgrade at most one minor version at a time. For
   example, upgrading from `v25.1.5` to `v25.2.16` is permitted.

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)

- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

- [Troubleshooting](/self-managed-deployments/troubleshooting/)
