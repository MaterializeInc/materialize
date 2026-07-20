---
headless: true
---
{{< tabs >}}
{{< tab "Legacy Syntax" >}}

With the legacy syntax, the source decodes the topic directly and is itself
queryable. Picking up an upstream schema change requires dropping and recreating
the source, which incurs downtime.

```mzsql
CREATE SOURCE __SOURCE__
    FROM KAFKA CONNECTION __CONNECTION__ (TOPIC '__TOPIC__')
    __FORMAT__;
```

{{< /tab >}}
{{< tab "New Syntax" >}}

With the new syntax, create a source for the topic and then a table that decodes
it. Each table pins its own reader schema, which lets you pick up upstream schema
changes without downtime. For details, see [Handle upstream schema changes with
zero downtime](/ingest-data/kafka/source-versioning/).

```mzsql
CREATE SOURCE __SOURCE__
    FROM KAFKA CONNECTION __CONNECTION__ (TOPIC '__TOPIC__');

CREATE TABLE __TABLE__
    FROM SOURCE __SOURCE__
    __FORMAT__;
```

{{< /tab >}}
{{< /tabs >}}
