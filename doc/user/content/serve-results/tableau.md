---
title: "Connect to Materialize with Tableau"
description: "How to connect to Materialize with Tableau"
aliases:
  - /third-party/tableau/
  - /integrations/tableau/
menu:
  main:
    parent: "serve"
    name: "Connect with Tableau"
    weight: 25
---

[Tableau](https://www.tableau.com/) is a data visualization platform that provides tools for the interpretation and visualization of data, enabling users to create interactive graphical representations and dashboards from raw data sources.

### Installation and Setup

Before starting, there are some necessary steps you need to follow:

1. [Sign up for a Tableau account](https://www.tableau.com/products/trial)
2. [Download the Tableau Desktop](https://www.tableau.com/products/desktop)
3. [Download the PostgreSQL driver](https://www.tableau.com/support/drivers?edition=pro&lang=en-us&platform=mac&cpu=64&version=2023.2&__full-version=20232.23.0611.2007#postgres)

    Complete the following steps to install the PostgresSQL driver.

    1. Download the Java 8 JDBC driver from https://jdbc.postgresql.org/download/
    2. Copy the .jar file to the following folder (you may have to create the folder manually).

    ```
    ~/Library/Tableau/Drivers
    ```

After installation, open Tableau Desktop.

### Connect to Materialize

Once you're in the Tableau Desktop interface, follow these steps:

1. On the left side, find the "Connect to a Server" section
2. Click on "More" and select "PostgreSQL"
3. Use the following details to configure the connection:

    Field          | Value
    -------------- | ----------------------
    Server         | Materialize host name
    Port           | **6875**
    Database       | **materialize**
    Authentication | Username and Password
    Username       | Materialize user
    Password       | App-specific password
    Require SSL    | Checked

4. Click the "Sign In" button to connect to Materialize

By following these steps, you can configure a connection from your Tableau Desktop to Materialize and start creating dashboards based on your data.

For more detailed information and troubleshooting, you may want to check out the official [Tableau documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm).
