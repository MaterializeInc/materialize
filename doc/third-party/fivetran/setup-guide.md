---
name: Setup Guide
title: Materialize Setup Guide
description: Follow the guide to set up Materialize as a destination.
---

# Materialize Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

Follow the setup guide to connect Materialize to Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to Materialize destination and its documentation, contact [Materialize Support](mailto:support@materialize.com).

---

## Prerequisites

To connect Materialize to Fivetran, you need the following:

- A Materialize account with `CREATE` privileges on the database you're connecting to.
- A Fivetran account with [permission to add destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#legacyandnewrbacmodel).

---

## Setup instructions

### <span class="step-item"> Get Materialize credentials </span> 

You must have a Materialize account with `CREATE` privileges on the database you're connecting to.

1. Login to the [Materialize Console](https://console.materialize.com) with your account.
2. Click "Connect externally" at the bottom of the sidebar.
3. On the "External tools" tab, click "Create app password". 
4. Copy the generated **password**, **Host**, and **User** somewhere safe, we'll use these soon!

### <span class="step-item">Finish Fivetran configuration </span>

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), and then click **+ Add Destination**.
3. Enter a **Destination name** of your choice.
4. Click **Add**.
5. Select **Materialize** as the destination type.
6. Enter the **Host**, **User**, and **App password** you copied in Step 1.
7. Enter the **Database** you would like to connect to and have `CREATE` permissions for.
7. Choose the **Data processing location**.
8. Choose your **Time zone**.
9. Click **Save & Test**.

Fivetran [tests and validates](/docs/destinations/materialize/setup-guide#setuptest) the Materialize connection. On successful completion of the setup test, you can sync your data using Fivetran connectors to the Materialize destination.

### Setup test

Fivetran performs the following Materialize connection test:

- The Connection test checks if we can connect to your Materialize account through the API using the Host, User, and App password you provided in the setup form.
