---
name: Setup Guide
title: Materialize Setup Guide
description: Follow the guide to set up Materialize as a destination.
---

# Materialize Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="materialize" /%}

Follow the setup guide to connect Materialize to Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to the Materialize destination and its documentation, contact the [Materialize team](mailto:support@materialize.com).

---

## Prerequisites

To connect Materialize to Fivetran, you need the following:

- A Materialize account. If you don't have one, [sign up for a free trial account](https://materialize.com/register/) first.
- A Fivetran account with [permission to add destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#legacyandnewrbacmodel).

---

## Setup instructions

### <span class="step-item"> Get Materialize credentials </span>

The user you're using to connect to Fivetran must have [`CREATE`](https://materialize.com/docs/manage/access-control/rbac/#privileges) privileges on the target database in Materialize.

1. Log in to your [Materialize account](https://console.materialize.com).
2. In the bottom left corner, click on your user, then select **App passwords**.
3. On the App passwords page, click **New app password** in the top right corner.
4. Create a new app password with a name related to Fivetran, for example, "John's Fivetran App Password".
5. Make a note of the password. You won't be able to see it again once you navigate away from this page. You will need it to [Finish Fivetran configuration](#finishfivetranconfiguration).
6. On the navigation menu, click **Connect externally**.
7. Make a note of the **Host** and **User** fields. You will need it to [Finish Fivetran configuration](#finishfivetranconfiguration).

### <span class="step-item">Finish Fivetran configuration </span>

1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and then click **Add destination**.
3. Enter a **Destination name** of your choice.
4. Click **Add**.
5. Select **Materialize** as the destination type.
6. Enter the **Host**, **User**, and **App password** you previously copied.
7. Enter the **Database** you would like to connect to. As a reminder, the user specified in the previous step must have the `CREATE` privileges on this database.
7. Choose the **Data processing location**.
8. Choose your **Time zone**.
9. Click **Save & Test**.

Fivetran will test and validate that we can connect to your Materialize account with the Host, User, and App password you provided in the setup form. On successful completion of the setup test, you can sync your data using Fivetran connectors to the Materialize destination.

In addition, Fivetran automatically configures a [Fivetran Platform Connector](/docs/logs/fivetran-platform) to transfer the connection logs and account metadata to a schema in this destination. The Fivetran Platform Connector enables you to monitor your connections, track your usage, and audit changes. The Fivetran Platform Connector sends all these details at the destination level.

> IMPORTANT: If you are an Account Administrator, you can manually add the Fivetran Platform Connector on an account level so that it syncs all the metadata and logs for all the destinations in your account to a single destination. If an account-level Fivetran Platform Connector is already configured in a destination in your Fivetran account, then we don't add destination-level Fivetran Platform Connectors to the new destinations you create.
