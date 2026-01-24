---
title: "Appendix: Privileges"
description: "List of available  privileges in Materialize."
menu:
  main:
    parent: security-appendix
    weight: 5
aliases:
  - /manage/access-control/appendix-privileges/
  - /security/appendix-privileges/
---

{{< note >}}
{{% include-headless "/headless/rbac-cloud/privileges-related-objects" %}}
{{</ note >}}

The following privileges are available in Materialize:

{{< tabs >}}
{{< tab "By Privilege" >}}
{{< yaml-table data="rbac/privileges_objects" >}}
{{</ tab >}}
{{< tab "By Object" >}}
{{< yaml-table data="rbac/object_privileges" >}}
{{</ tab >}}
{{</ tabs >}}
