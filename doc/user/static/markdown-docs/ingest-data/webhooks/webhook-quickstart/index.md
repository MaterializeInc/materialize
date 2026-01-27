# Webhooks quickstart
Learn and prototype with the webhook source without external deoendencies
Webhook sources let your applications push webhook events into Materialize. This
quickstart uses an embedded **webhook event generator** that makes it easier for
you to learn and prototype with no external dependencies.

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

All you need is a Materialize account. If you already have one —
great! If not, [sign up for a free trial account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) first.

When you're ready, head over to the [Materialize console](/console/),
and pop open the SQL Shell.

## Step 1. Create a secret

To validate requests between the webhook event generator and Materialize, you
need a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET demo_webhook AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in
a secure location.

## Step 2. Set up a webhook source

Using the secret from the previous step, create a webhook source to ingest data
from the webhook event generator. By default, the source will be created in the
current cluster.

```mzsql
CREATE SOURCE webhook_demo FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS,
      BODY AS request_body,
      SECRET demo_webhook AS validation_secret
    )
    -- The constant_time_eq validation function **does not support** fully
    -- qualified secret names. We recommend always aliasing the secret name
    -- for ease of use.
    constant_time_eq(headers->'x-api-key', validation_secret)
  );
```

After a successful run, the command returns a `NOTICE` message containing the
unique [webhook URL](/sql/create-source/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You will need
it for the next step.

## Step 3. Generate webhook events

The webhook event generator uses [Faker.js](https://fakerjs.dev/) under the
covers, which means you can use any of the [supported modules](https://fakerjs.dev/api/)
to shape the events.

<div id="webhooks-datagen" class="json_widget">
  <div class="input_container">
    <div class="input_container-text">
      <input type="text" placeholder="Webhook URL" id="webhookURL" class="text-input" />
    </div>
  </div>
  <div class="input_container">
    <div class="input_container-text">
      <input type="password" placeholder="Secret (Step 1.)" id="authPassword" class="text-input" />
    </div>
  </div>
  <div class="input_container">
    <div class="input_container-text">
      <button id="startButton" class="btn">Generate webhook events!</button>
      <button id="stopButton" class="btn" style="display: none">Stop</button>
    </div>
  </div>
  <div class="input_container">
    <div class="input_container-text">
      <select id="useCaseSelect" class="text-input">
        <option value="">Select a Faker.js module</option>
        <option value="sensorData">Sensor data</option>
        <option value="userActivity">User activity</option>
        <option value="ecommerceTransaction">E-commerce transaction</option>
      </select>
    </div>
  </div>
  <div>
    <div class="json">
      <textarea placeholder="JSON schema" id="jsonSchema" rows="5" cols="30" class="json-textarea">
{
  "sensor_id": "faker.datatype.number({ max: 100, min: 1})",
  "timestamp": "faker.date.between('2020-01-01T00:00:00.000Z', '2030-01-01T00:00:00.000Z')",
  "location": {
      "latitude": "faker.datatype.number({ max: 90, min: -90})",
      "longitude": "faker.datatype.number({ max: 180, min: -180})"
  },
  "temperature": "faker.datatype.float({ min: 20, max: 95 })"
}
      </textarea>
      <div id="jsonError" class="error" style="display: none"></div>
    </div>
    <div class="log_output" id="logOutput" style="display: none"></div>
  </div>
</div>

<script type="module">
  import { faker } from 'https://esm.sh/@faker-js/faker@8.4.0';

  const webhookURLInput = document.getElementById("webhookURL");
  const authPasswordInput = document.getElementById("authPassword");
  const jsonSchemaTextarea = document.getElementById("jsonSchema");
  const startButton = document.getElementById("startButton");
  const stopButton = document.getElementById("stopButton");
  const jsonErrorDiv = document.getElementById("jsonError");
  const logOutputDiv = document.getElementById("logOutput");

  let logs = [];
  let isGenerating = false;
  let generationInterval;

  document.addEventListener("DOMContentLoaded", function () {
    formatJSON();
  });

  function validateJson(jsonString) {
    try {
      JSON.parse(jsonString);
      jsonErrorDiv.style.display = "none";
      return true;
    } catch (error) {
      jsonErrorDiv.style.display = "block";
      jsonErrorDiv.textContent = "Invalid JSON format";
      return false;
    }
  }

  function isFakerPathValid(path) {
    return path
      .split(".")
      .reduce((acc, curr) => (acc && acc[curr] ? acc[curr] : null), faker);
  }

  function generateDataAndSend() {
    const generatedData = JSON.parse(
      jsonSchemaTextarea.value.replace(/"faker\.(.+?)"/g, (_, match) => {
        try {
          const fakerValue = eval(`faker.${match}`);
          return JSON.stringify(fakerValue);
        } catch (error) {
          console.error("Error while generating faker data:", error);
          return '"Error"';
        }
      })
    );
    logs.unshift(`Sent: ${JSON.stringify(generatedData)}`);
    logOutputDiv.innerHTML = logs
      .map((log, index) => `<p key="${index}">${log}</p>`)
      .join("");


    updateLogDisplay();

    console.log("URL:", webhookURLInput.value);
    console.log("Headers:", {
      "Content-Type": "application/json",
      "X-API-KEY": authPasswordInput.value,
    });
    console.log("Body:", JSON.stringify(generatedData));

    fetch(webhookURLInput.value, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-KEY": authPasswordInput.value,
      },
      body: JSON.stringify(generatedData),
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP error! Status: ${response.status}`);
        }
        return response;
      })
      .then((data) => {

      })
      .catch((error) => {
        console.error("There was a problem with the fetch operation:", error);
        logs.unshift(`Error: ${error.message}`);
        logOutputDiv.innerHTML = logs
          .map((log, index) => `<p key="${index}">${log}</p>`)
          .join("");

        updateLogDisplay();
      });
  }

  function startGeneration() {
    if (!webhookURLInput.value || !authPasswordInput.value) {
      alert("Webhook URL and Auth Password cannot be empty!");
      return;
    }
    if (validateJson(jsonSchemaTextarea.value)) {
      isGenerating = true;
      generationInterval = setInterval(generateDataAndSend, 1000);
      startButton.style.display = "none";
      stopButton.style.display = "inline-block";
    }
  }

  function stopGeneration() {
    isGenerating = false;
    clearInterval(generationInterval);
    startButton.style.display = "inline-block";
    stopButton.style.display = "none";
    generationInterval = null;
  }


  function updateLogDisplay() {
    logOutputDiv.style.display = logs.length > 0 ? "block" : "none";
  }

  startButton.addEventListener("click", startGeneration);
  stopButton.addEventListener("click", stopGeneration);
  updateLogDisplay();


  jsonSchemaTextarea.addEventListener("blur", formatJSON);

  function formatJSON() {
    const jsonString = jsonSchemaTextarea.value.trim();
    try {
      const formattedJSON = JSON.stringify(JSON.parse(jsonString), null, 2);
      jsonSchemaTextarea.value = formattedJSON;
    } catch (error) {

      console.error("Error while formatting JSON:", error);
    }
  }

  document.getElementById('useCaseSelect').addEventListener('change', function() {
    const selectedUseCase = this.value;
    const schema = schemas[selectedUseCase];
    if (schema) {
      jsonSchemaTextarea.value = schema;
      formatJSON();
    }
  });

  const schemas = {
    sensorData: `{
      "sensor_id": "faker.datatype.number({ max: 100, min: 1})",
      "timestamp": "faker.date.between('2020-01-01T00:00:00.000Z', '2030-01-01T00:00:00.000Z')",
      "location": {
          "latitude": "faker.datatype.number({ max: 90, min: -90})",
          "longitude": "faker.datatype.number({ max: 180, min: -180})"
      },
      "temperature": "faker.datatype.float({ min: 20, max: 95 })"
    }`,
    userActivity: `{
      "user_id": "faker.datatype.uuid()",
      "event": "faker.helpers.arrayElement(['page_view', 'click', 'scroll', 'navigation'])",
      "timestamp": "faker.date.recent()",
      "page": {
        "url": "faker.internet.url()",
        "title": "faker.lorem.words()"
      },
      "interaction": {
        "element": "faker.lorem.word()",
        "time_spent": "faker.datatype.number({ min: 1, max: 120 })"
      }
    }`,
    ecommerceTransaction: `{
      "user_id": "faker.datatype.uuid()",
      "event": "faker.helpers.arrayElement(['add_to_cart', 'remove_from_cart', 'purchase', 'view_product'])",
      "timestamp": "faker.date.recent()",
      "product": {
        "id": "faker.datatype.uuid()",
        "name": "faker.commerce.productName()",
        "category": "faker.commerce.department()",
        "price": "faker.commerce.price()"
      }
    }`
  };
</script>


In the SQL Shell, validate that the source is ingesting data:

```mzsql
SELECT jsonb_pretty(body) AS body FROM webhook_demo LIMIT 1;
```

As an example, if you use the `Sensor data` module of the webhook event
generator, the data will look like:
```json
{
  "location": {
    "latitude": 6,
    "longitude": 0
  },
  "sensor_id": 48,
  "temperature": 89.38,
  "timestamp": "2029-10-07T10:44:13.456Z"
}
```

## Step 4. Parse JSON

Manually parsing JSON-formatted data in SQL can be tedious. You can use the [interactive JSON parser widget](https://materialize.com/docs/sql/types/jsonb/#parsing) to automatically turn a sample JSON payload into a parsing view with the individual fields mapped to columns.


Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [jsonb operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types. Using the
previous example:

```mzsql
CREATE VIEW webhook_demo_parsed AS SELECT
    (body->'location'->>'latitude')::numeric AS location_latitude,
    (body->'location'->>'longitude')::numeric AS location_longitude,
    (body->>'sensor_id')::numeric AS sensor_id,
    (body->>'temperature')::numeric AS temperature,
    try_parse_monotonic_iso8601_timestamp(body->>'timestamp') AS timestamp
FROM webhook_demo;
```

## Step 5. Subscribe to see the output

To see results change over time, let’s [`SUBSCRIBE`](/sql/subscribe/) to the
`webhook_demo_parsed ` view:

```mzsql
SUBSCRIBE(SELECT * FROM webhook_demo_parsed) WITH (SNAPSHOT = FALSE);
```

You'll see results change as new webhook events are ingested. When you’re done,
cancel out of the `SUBSCRIBE` using **Stop streaming**.

## Step 6. Clean up

Once you’re done exploring the generated webhook data, remember to clean up your
environment:

```mzsql
DROP SOURCE webhook_demo CASCADE;

DROP SECRET demo_webhook;
```

## Next steps

To get started with your own data, check out the [reference documentation](/sql/create-source/webhook/)
for the webhook source.
