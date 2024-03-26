---
name: Setup Guide
title: Materialize Setup Guide
description: Follow the guide to set up Materialize as a destination.
---

# Materialize Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

Follow the setup guide to connect Materialize to Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to the Materialize destination and its documentation, contact the [Materialize team](mailto:support@materialize.com).

---

## Prerequisites

To connect Materialize to Fivetran, you need the following:

- A Materialize account. If you already have one - great! If not, [sign up for a playground account](https://materialize.com/register/) first.
- A Fivetran account with [permission to add destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#legacyandnewrbacmodel).

---

## Setup instructions

### <span class="step-item"> Get Materialize credentials </span>

The user you're using to connect to Fivetran must have [`CREATE`](https://materialize.com/docs/manage/access-control/rbac/#privileges) privileges on the target database in Materialize.

1. Log in to your [Materialize account](https://console.materialize.com).
2. In the bottom left corner click on your user, then select "App passwords".
3. From the "App passwords" page click "New app password" in the top right corner
4. Create a new app password with a name related to Fivetran, for example "John's Fivetran App Password".
5. Copy the newly created app password somewhere safe, you won't be able to see it again once you navigate away from this page!
6. Click "Connect externally" at the bottom of the sidebar.
7. Copy the **Host** and **User** fields somewhere safe, we'll use these soon!

### <span class="step-item">Finish Fivetran configuration </span>

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), and then click **Add Destination**.
3. Enter a **Destination name** of your choice.
4. Click **Add**.
5. Select **Materialize** as the destination type.
6. Enter the **Host**, **User**, and **App password** you previously copied.
7. Enter the **Database** you would like to connect to. As a reminder, the user specified in the previous step must have `CREATE` privileges on this database.
7. Choose the **Data processing location**.
8. Choose your **Time zone**.
9. Click **Save & Test**.

Fivetran will [test and validate](/docs/destinations/materialize/setup-guide#setuptest) that we can
connect to your Materialize account with the Host, User, and App password you provided in the setup
form. On successful completion of the setup test, you can sync your data using Fivetran connectors
to the Materialize destination.
