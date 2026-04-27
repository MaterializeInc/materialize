---
headless: true
---

Update the `<baseURL>` and `<base64-token>` placeholders with your values:

| Deployment   |  `<baseURL>`                                                     |  `<base64-token>`              |
|--------------| ------------------------------------------------------------------| -------------------------------|
| **Cloud**        | Replace with your value (format: `https://<region-id>.materialize.cloud`)  | Replace with your value       |
| **Self-Managed** | Replace with your value (format: `http://<host>:6876`) | Replace with your value       |
| **Emulator**     | `http://localhost:6876` | Leave the placeholder as-is |

{{< tip >}}

For **Cloud**, you can copy the `.json` content from the **MCP Server** tab in
the Console's **Connect** modal. The `.json` copied from the Console already
includes the correct `<baseURL>`.

{{< /tip >}}
