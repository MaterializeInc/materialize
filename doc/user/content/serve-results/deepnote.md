---
title: "Deepnote"
description: "How to create collaborative data notebooks with Deepnote"
aliases:
  - /third-party/deepnote/
  - /integrations/deepnote/
menu:
  main:
    parent: "bi-tools"
    name: "Deepnote"
    weight: 5
---

This guide walks you through the steps required to use the collaborative data notebook [Deepnote](https://deepnote.com/) with Materialize.

## Step 1. Create an integration

1. Log in to **Deepnote**.

2. On the left sidebar, search for the integrations section and click the **plus (+) button** to initiate a new integration.

3. From the current integrations list, select the **Create a new integration** option.

4. In the search field, type and choose the **Materialize** option.

5. Populate the connection fields as follows:
   - Integration name: Choose any name you prefer.
   - Host name: Enter the host available in the console.
   - Port: Provide the port available in the console.
   - Authentication:
      - Username: Your console login name.
      - Password: The app password generated within the console.
      - Database: `materialize`
      - Cluster: Your designated cluster.

1. Click the **Create Integration** button.

## Step 2. Execute a query