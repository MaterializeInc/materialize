---
headless: true
---

Update the `<baseURL>` and `<mcp-token>` placeholders with your values:

| Deployment   |  `<baseURL>`                                                     |  `<mcp-token>`              |
|--------------| ------------------------------------------------------------------| -------------------------------|
| **Cloud**        | Replace with your value (format: `https://<region-id>.materialize.cloud`)  | Replace with your value       |
| **Self-Managed** | Replace with your value (format: `http://<host>:6876`) | Replace with your value       |
| **Emulator**     | `http://localhost:6876` | Leave the placeholder as-is |

{{< tip >}}

For **Cloud**, you can get the MCP URL directly from the Console's **Connect**
modal. The modal displays the correct `<baseURL>`.

{{< /tip >}}
