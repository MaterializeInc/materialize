---
headless: true
---

{{< warning >}}
Use a **custom authorization server** (e.g., `.../oauth2/default`), not the
Okta **org authorization server** (the bare org URL, without an
`/oauth2/...` path). Access tokens issued by the org authorization server
have a fixed `aud` claim and cannot carry custom claims or scopes, which
breaks the [Client Credentials flow](#client-credentials-flow) and
[MCP clients](#connecting-mcp-clients). Custom authorization servers,
including `default`, require Okta's API Access Management feature.
{{< /warning >}}
