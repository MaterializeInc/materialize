---
title: "Download and run Materialize Emulator"
description: "The Materialize Emulator is an all-in-one Docker image available on Docker Hub, offering the fastest way to get hands-on experience with Materialize in a local environment."
menu:
  main:
    parent: "get-started"
    weight: 15
    name: "Download and run Materialize Emulator"

---

The Materialize Emulator is an all-in-one Docker image available on Docker Hub
for testing and evaluation purposes. The Materialize Emulator is not
representative of Materialize's performance and full feature set.

{{< important >}}

The Materialize Emulator is <redb> not suitable for production workloads.</redb>.

{{</ important >}}

### Materialize Emulator

Materialize Emulator is the easiest way to get started with Materialize, but is
not suitable for full feature set evaluations or production workloads.

| Materialize Emulator              | Details    |
|-----------------------------------|------------|
| **What is it**                    | A single Docker container version of Materialize. |
| **Best For**                       | Prototyping and CI jobs. |
| **Known Limitations**     | Not indicative of true Materialize performance. <br>Services are bundled in a single container. <br>No fault tolerance. <br>No data persistence. <br>No support for version upgrades. |
| **Evaluation Experience**          | Download from Docker Hub. |
| **Support**                        | [Materialize Community Slack channel](https://materialize.com/s/chat).|
| **License/legal arrangement**      | [BSL/Materialize's privacy policy](#license-and-privacy-policy) |

### Prerequisites

- Docker. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install.

### Install and run the Materialize Emulator

{{< note >}}

- Use of the Docker image is subject to Materialize's [BSL License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).

- By downloading the Docker image, you are agreeing to Materialize's [privacy policy](https://materialize.com/privacy-policy/).

{{</ note >}}

1. In a terminal, issue the following command to run a Docker container from the
   Materialize Emulator image. The command downloads the image, if one has not
   been already downloaded.

   ```sh
   docker run -d -p 127.0.0.1:6874:6874 -p 127.0.0.1:6875:6875 -p 127.0.0.1:6876:6876 -p 127.0.0.1:6877:6877 materialize/materialized:{{< version >}}
   ```

   When running locally:

   - The Docker container binds exclusively to localhost for security reasons.
   - The [Materialize Console](/console/) is available on port `6874`.
   - The SQL interface is available on port `6875`.
   - Logs are available via `docker logs <container-id>`.
   - A default user `materialize` is created.
   - A default database `materialize` is created.

1. <a name="materialize-emulator-connect-client"></a>

   Open the Materialize Console in your browser at [http://localhost:6874](http://localhost:6874).

   To streamline development and troubleshooting, we recommend [setting up
   your coding agents](#setup-your-coding-agents).

   You can also connect to the Materialize Emulator using your
   preferred SQL client, using the following connection field values:

   | Field    | Value         |
   |----------|---------------|
   | Server   | `localhost`   |
   | Database | `materialize` |
   | Port     | `6875`        |
   | Username | `materialize` |

   For example, if using [`psql`](/integrations/sql-clients/#psql):

   ```sh
   psql postgres://materialize@localhost:6875/materialize
   ```

1. Once connected, you can get started with the
   [Quickstart](/get-started/quickstart).

### Setup your coding agents

To streamline development and troubleshooting, you can set up coding agents
like [Claude Code](https://docs.anthropic.com/en/docs/claude-code),
[Codex](https://openai.com/index/codex/), and [Cursor](https://www.cursor.com/)
to work with your Materialize Emulator.

#### Install the Materialize agent skills

Materialize provides open-source [agent
skills](/integrations/coding-agent-skills/) that give your coding agent access
to Materialize documentation and reference material, so it can provide more
accurate assistance when writing queries, setting up sources, creating
materialized views, and more.

1. If [Node.js](https://nodejs.org/) (v16 or later) is not installed, refer to
   its [official documentation](https://nodejs.org/en/download) to install.

1. In a terminal, issue the following command to install the Materialize agent
   skills:

   ```bash
   npx skills add MaterializeInc/agent-skills
   ```

For more details on the available skills, see [Agent
Skills](/integrations/coding-agent-skills/).

#### Connect to the MCP server

The Materialize Emulator includes a built-in `materialize-developer` [MCP
server](/integrations/mcp-server/mcp-developer/) (port `6876`) for
troubleshooting and observability. To connect, your MCP client needs an MCP
token and the MCP server URL.

1. Generate the MCP token by Base64-encoding the credentials of the default
   `materialize` user (the Emulator does not support passwords):

   ```sh
   printf 'materialize:' | base64
   ```

1. Configure your MCP client with the MCP token and the Emulator's MCP server
   URL `http://localhost:6876/api/mcp/developer`. For example, if using [Claude
   Code](https://docs.anthropic.com/en/docs/claude-code):

   ```sh
   claude mcp add --transport http materialize-developer \
     http://localhost:6876/api/mcp/developer \
     --header "Authorization: Basic <mcp-token>"
   ```

   Replace `<mcp-token>` with the token generated in the previous step.

1. Restart your MCP client to pick up the new setting. Once connected, you can
   ask questions like *Why is my materialized view stale?* or *How much memory
   is my cluster using?*

For more details, including instructions for other MCP clients, see [MCP Server
for Developers](/integrations/mcp-server/mcp-developer/).

### Next steps

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, check the documentation for [sources](/sql/create-source/).

- Join the [Materialize Community on Slack](https://materialize.com/s/chat).

- To fully evaluate Materialize Cloud, sign up for a [free trial Materialize
  Cloud
  account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).
  The full experience of Materialize is also available as a self-managed
  offering. See [Self-managed Materialize](/self-managed-deployments/).


### Technical Support

For questions, discussions, or general technical support, join the [Materialize
Community on Slack](https://materialize.com/s/chat).

#### `mz-debug`

Materialize provides a [`mz-debug`]command-line debug tool called  that helps collect diagnostic information from your emulator environment. This tool can gather:
- Docker logs and resource information
- Snapshots of system catalog tables from your Materialize instance

To debug your emulator instance, you can use the following command:

```console
mz-debug emulator --docker-container-id <your-container-id>
```

This debug information can be particularly helpful when troubleshooting issues or when working with the Materialize support team.

For more detailed information about the debug tool, see the [`mz-debug` documentation](/integrations/mz-debug/).


### License and privacy policy

- Use of the Docker image is subject to Materialize's [BSL
  License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).

- By downloading the Docker image, you are agreeing to Materialize's
  [privacy policy](https://materialize.com/privacy-policy/).

#### Materialize Self-Managed Community Edition or the Materialize Emulator Privacy FAQ

When you use the Materialize Self-Managed Community Edition or the Materialize Emulator, we may collect the following information from the machine that runs the Materialize Self-Managed Community Edition or the Materialize Emulator software:

- The IP address of the machine running Materialize.

- If available, the cloud provider and region of the machine running
  Materialize.

- Usage data about your use of the product such as the types or quantity of
  commands you run, the number of clusters or indexes you are running, and
  similar feature usage information.

The collection of this data is subject to the [Materialize Privacy Policy](https://materialize.com/privacy-policy/).

Please note that if you visit our website or otherwise engage with us outside of
downloading the Materialize Self-Managed Community Edition or the Materialize
Emulator, we may collect additional information about you as further described
in our [Privacy Policy](https://materialize.com/privacy-policy/).

<style>
red { color: #d33902 }
redb { color: #d33902; font-weight: 500; }
</style>
