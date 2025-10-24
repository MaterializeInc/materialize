---
title: "Appendix: Privileges"
description: "List of available  privileges in Materialize."
menu:
  main:
    parent: security
    weight: 95
aliases:
  - /manage/access-control/appendix-privileges/
---

{{< note >}}
{{< include-md file="shared-content/rbac/privileges-related-objects.md" >}}
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
