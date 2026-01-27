# Materialize console

Introduction to the Materialize Console, user interface for Materialize



The Materialize Console is a graphical user
interface for working with Materialize. From the Console, you can create and
manage your clusters and sources, issue SQL queries, explore your objects, and
view billing information.

![Image of the Materialize Console](/images/console/console.png
"Materialize Console")

- **Create New**: Shortcut menu to the following screens:

  - [**New cluster**](/console/create-new/#create-new-cluster) screens

  - [**New source**](/console/create-new/#create-new-source) screens

  - [**New app
    password**](/console/create-new/#create-new-app-password-cloud-only) screens
    (***Cloud-only***)

- [SQL Shell](/console/sql-shell/): Issue SQL queries.

- [Database object explorer](/console/data/): Explore your objects.

- [Clusters](/console/clusters/): Manage your Materialize clusters.

- [Integrations](/console/integrations/): Learn about the supported integrations.

- [Monitoring](/console/monitoring/): Monitor your environment as well as access your query history.

- [Admin](/console/admin/): Manage client credentials and billing information.
  (***Cloud-only***)

- [Connect](/console/connect/): View information needed to connect using service
  accounts. (***Cloud-only***)

- [User Profile](/console/user-profile/): Manage your user profile
  (***Cloud-only***), find links to links to the documentation, Materialize
  Community slack, and Help Center.



---

## Admin (Cloud-only)


The Materialize Console provides an **Admin** section where you can manage
client credentials and, for administrators, review your usage and billing
information.

The **Admin** section contains the following screens:

| Feature | Description |
|---------|-------------|
| **App Passwords** | View or delete client credentials that allow your applications and services to connect to Materialize. |
| **Billing** | Access detailed usage and billing information as well as your invoices. <br>**Available for administrators only.** |

### App Passwords

![Image of the Client Passwords](/images/console/console-passwords.png "Client passwords")

The **App Passwords** screen displays the various [client credentials
created](/console/create-new/#create-new-app-password-cloud-only) to allow your
applications and services to connect to Materialize.

- The [**Connect**](/console/connect/) button provides details needed to connect
to Materialize.

- The **trash can** button deletes the password.

See also [+Create New > App
Password](/console/create-new/#create-new-app-password-cloud-only) for steps to
create a new app password.

### Billing page

**Available for administrators only.**

![Image of the Billing](/images/console/console-billing.png "Usage and Billing")


---

## Clusters


The Materialize Console provides a
**Clusters** section where you can manage your clusters.

![Image of Clusters page](/images/console/console-clusters.png "Clusters page
listing your clusters")

From the **Clusters** starting page, you can:

- **Alter or drop a cluster**

![Image of the menu to alter or drop the cluster](/images/console/console-clusters-menu.png "Open menu to alter or drop the cluster")

- **Select a cluster** to display cluster details:

![Image of the `quickstart` cluster details](/images/console/console-clusters-view.png "Details about the `quickstart` cluster")


---

## Connect (Cloud-only)


The **Connect** modal provides details needed to connect your [applications](/console/admin/) to Materialize.

![Image of the Connect modal](/images/console/console-connect-modal.png
"Materialize Connect modal")


---

## Create new


From the Console, you can create new [clusters](/concepts/clusters/ "Isolated
pools of compute resources (CPU, memory, and scratch disk space)"),
[sources](/concepts/sources/ "Upstream (i.e., external) systems you want
Materialize to read data from"), and, for Materialize Cloud, application passwords.

### Create new cluster

![Image of the Create New Cluster flow](/images/console/console-create-new/postgresql/create-new-cluster-flow.png "Create New Cluster flow")

From the Materialize Console:

1. Click **+ Create New** and select **Cluster** to open the **New cluster**
   screen.

1. In the **New cluster** screen,

   1. Specify the following cluster information:

      | Field | Description |
      | ----- | ----------- |
      | **Name** | A name for the cluster. | `
      | **Size** | The [size](/sql/create-cluster/#size) of the cluster. |
      | **Replica** | The [replication factor](/sql/create-cluster/#replication-factor) of the cluster. Default: `1` <br>Clusters that contain sources or sinks cannot have a replication factor greater than 1.|

   1. Click **Create cluster** to create the cluster.

1. Upon successful creation, you'll be redirected to the **Overview** page of
    the newly created cluster.


### Create new source

> **Tip:** - For PostgreSQL and MySQL, you must configure your upstream database first.
>   Refer to the [Ingest data](/ingest-data/) section for your data source.
> - For information about the snapshotting process that occurs when a new source
>   is created as well as some best practice guidelines, see [Ingest
>   data](/ingest-data/).


![Image of the Create New Source start for
PostgreSQL](/images/console/console-create-new/postgresql/create-new-source-start.png
"Create New Source start for PostgreSQL")

From the Materialize Console:

1. Click **+ Create New** and select **Source** to open the **New source**
   screen.

1. Choose the source type and follow the instructions to configure a new source.

    > **Tip:** For PostgreSQL and MySQL, you must configure your upstream database first. Refer
>     to the [Ingest data](/ingest-data/) section for your data source.



### Create new app password (Cloud-only)

![Image of the Create application
password](/images/console/console-create-new/create-app-password.png "Create
application password")


1. Click **+ Create New** and select **App Password** to open the **New app
   password** modal.

1. In the  **New app password** modal, specify the **Type** (either **Personal**
   or **Service**) and the associated details:

   > **Note:** - Only **Organization admins** can create a service account.
> - **Personal** apps are run under your user account.
> - **Service** apps are run under a Service account user. If the specified
>    Service account user does not exist, it will be automatically created the
>    **first time** the app password is used.




   **Personal:**

   For a personal app that you will run under your user account, specify the
   type and required field(s):

   | Type | Details |
   | ---- | ----------- |
   | **Type** | Select **Personal** |
   | **Name** | Specify a descriptive name. |


   **Service account:**

   For an app that you will run under a Service account, specify the
   type and required field(s):


   | Field | Details |
   | --- | --- |
   | <strong>Type</strong> | Select <strong>Service</strong> |
   | <strong>Name</strong> | Specify a descriptive name. |
   | <strong>User</strong> | Specify a service account user name. If the specified account user does not exist, it will be automatically created the <strong>first time</strong> the application connects with the user name and password. |
   | <strong>Roles</strong> | <p>Select the organization role:</p> <table>   <thead>       <tr>           <th>Organization role</th>           <th>Description</th>       </tr>   </thead>   <tbody>       <tr>           <td><strong>Organization Admin</strong></td>           <td><ul> <li> <p><strong>Console access</strong>: Has access to all Materialize console features, including administrative features (e.g., invite users, create service accounts, manage billing, and organization settings).</p> </li> <li> <p><strong>Database access</strong>: Has <red><strong>superuser</strong></red> privileges in the database.</p> </li> </ul></td>       </tr>       <tr>           <td><strong>Organization Member</strong></td>           <td><ul> <li> <p><strong>Console access</strong>: Has no access to Materialize console administrative features.</p> </li> <li> <p><strong>Database access</strong>: Inherits role-level privileges defined by the <code>PUBLIC</code> role; may also have additional privileges via grants or default privileges. See <a href="/security/cloud/access-control/#roles-and-privileges" >Access control control</a>.</p> </li> </ul></td>       </tr>   </tbody> </table> <blockquote> <p><strong>Note:</strong> - The first user for an organization is automatically assigned the <strong>Organization Admin</strong> role.</p> <ul> <li>An <a href="/security/cloud/users-service-accounts/#organization-roles" >Organization Admin</a> has <red><strong>superuser</strong></red> privileges in the database. Following the principle of least privilege, only assign <strong>Organization Admin</strong> role to those users who require superuser privileges.</li> <li>Users/service accounts can be granted additional database roles and privileges as needed.</li> </ul> </blockquote>  |


   See also [Create service
   accounts](/security/cloud/users-service-accounts/create-service-accounts/)
   for creating service accounts via Terraform.





1. Click **Create password** to generate the app password.

1. Store the new password securely.

   > **Note:** Do not reload or navigate away from the screen before storing the
>    password. This information is not displayed again.


1. **For a new service account only**.

   For a new service account, after creating the new app password, you must
   connect with the service account to complete the account creation. The first time the account connects, a database role with the same name as the
specified service account **User** is created, and the service account creation is complete.

   To connect:

   1. Find your new service account in the **App Passwords** table.

   1. Click on the **Connect** button to get details on connecting with the new
      account.


      **psql:**
If you have `psql` installed:

1. Click on the **Terminal** tab.
1. From a terminal, connect using the psql command displayed.
1. When prompted for the password, enter the app's password.

Once connected, the service account creation is complete and you can grant roles
to the new service account.


      **Other clients:**
To use a non-psql client to connect,

1. Click on the **External tools** tab to get the connection details.

1. Update the client to use these details and connect.

Once connected, the service account creation is complete and you can grant roles
to the new service account.



To view the created app accounts, go to [Admin > App
Passwords](/console/admin/).


---

## Database object explorer


Under **Data**, the Materialize Console
provides a database object explorer.

![Image of the Materialize Console Database Object
Explorer](/images/console/console-data-explorer.png "Materialize Console Database Object Explorer")

<span class="caption">
When you select <em>Data</em>, the left panel collapses to reveal the database
object explorer.
</span>

You can inspect the objects in your databases by navigating to the object.

|Object|Available information|
|---|---|
|Connections|<li>Details: The [`CREATE CONNECTION`](/sql/create-connection/) SQL statement.</li>|
|Indexes|<li>Details: The [`CREATE INDEX`](/sql/create-index/) SQL statement.</li><li>Workflow: Details about the index (e.g., status), freshness, upstream and downstream objects. </li><li>Visualize: Dataflow visualization.</li>|
|Materialized Views|<li>Details: The [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/) SQL statement.</li><li>Workflow: Details about the materialized view (e.g., status), freshness, upstream and downstream objects.</li><li>Visualize: Dataflow visualization.</li>|
|Sinks|<li>Overview: View the sink metrics (e.g., messages/bytes produced) and details (e.g., Kafka topic).</li><li>Details: The [`CREATE SINK`](/sql/create-sink/) SQL statement.</li><li>Errors: Errors associated with the sink.</li><li>Workflow: Details about the sink (e.g., status), freshness, upstream and downstream objects.</li>|
|Sources|<li>Overview: View the ingestion metrics (e.g., Ingestion lag, messages/bytes received, Ingestion rate), Memory/CPU/Disk usage</li><li>Details: The [`CREATE SOURCE`](/sql/create-source/) SQL statement.</li><li>Errors: Errors associated with the source.</li><li>Subsources: List of associated subsources and their status.</li><li>Workflow: Details about the source (e.g.,status), freshness, upstream and downstream objects.</li><li>Indexes: Indexes on the source.</li>|
|Subsources|<li>Details: The `CREATE SUBSOURCE` SQL statement.</li><li>Columns: Column details.</li><li>Workflow: Details about the subsource (e.g.,status), freshness, upstream and downstream objects.</li><li>Indexes: Indexes on the subsource.</li>|
|Tables|<li>Details: The [`CREATE TABLE`](/sql/create-table/) SQL statement.</li><li>Workflow: Details about the table (e.g., status), freshness, upstream and downstream objects.</li><li>Columns: Column details.</li><li>Indexes: Indexes on the table.</li>|
|Views|<li>Details: The [`CREATE VIEW`](/sql/create-view/) SQL statement.</li><li>Columns: Column details.</li><li>Indexes: Indexes on the view. </li>|

#### Sample source overview

![Image of the Source Overview for auction_house
index](/images/console/console-data-explorer-source-overview.png "Source Overview for auction_house")

#### Sample index workflow

![Image of the Index Workflow for wins_by_item
index](/images/console/console-data-explorer-index-workflow.png "Index Workflow for wins_by_item index")


---

## Integrations


The Materialize Console provides an
**Integrations** page that lists the supported 3rd party integrations,
specifying:

- The level of support (Partner/Native/Compatible), and
- A link to the associated documentation in Materialize.

![Image of the Materialize Console Integrations page](/images/console/console-integrations.png "Materialize Console Integrations")


---

## Monitoring


The Materialize Console provides a
**Monitoring** section where you can review the health of your environment and
access your query history.

![Image of the Environment Overview](/images/console/console-environment-overview.png "Environment overview")

The **Monitoring** section contains the following screens:

| Feature | Description |
|---------|-------------|
| **Environment Overview** | Review the health of your environment. |
| **Query History** | Access your query history. |
| **Sources** | Review your sources. You can select a source to go to its [Database object explorer page](/console/data/). |
| **Sinks** | Review your sinks. You can select a sink to go to its [Database object explorer page](/console/data/). |


---

## SQL Shell


The Materialize Console provides a **SQL
Shell**, where you can issue your queries. Materialize follows the SQL standard
(SQL-92) implementation, and strives for compatibility with the PostgreSQL
dialect. If your query takes too long to complete, the SQL Shell provides
**Query Insights** listing some possible causes.

![Image of the Materialize Console SQL Shell](/images/console/console.png "Materialize Console SQL Shell")

The SQL Shell also includes:

- A top navigation panel, where you can select your cluster and database and
  schema.

- A [Quickstart](/get-started/quickstart/) tutorial. You can close the
  Quickstart by clicking the **Close Quickstart** button in the top-right
  corner.


---

## User profile


## Materialize Cloud

For Materialize Cloud, you can manage your user profile and account settings from the [Materialize
Console](/console/).

![Image of the user profile menu](/images/console/console-user-profile.png
"User profile menu")

## Materialize Self-Managed

For Materialize Self-Managed, you can find links to the documentation,
Materialize Community slack, and Help Center from the profile menu.

![Image of the user profile menu](/images/console/sm-console-user-profile.png
"User profile menu")
