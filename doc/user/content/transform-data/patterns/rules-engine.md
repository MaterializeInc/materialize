---
title: "Rules execution engine"
description: "Encode rules as data and apply them using lateral joins."
aliases:
  - /guides/rules-engine/
  - /sql/patterns/rules-engine/
menu:
  main:
    parent: 'sql-patterns'
---

SQL queries are often used to represent "rules" and evaluate which data in a dataset adhere to those rules.
In Materialize, the results of these queries can be kept automatically up-to-date, enabling use cases like real-time, rules-based alerting.
However, some organizations have a lot of rules.
This can lead users to ask questions like "Can Materialize support 100,000 materialized views"?

Fortunately there is a pattern for addressing this need without creating zillions of separate views.
If your rules are simple enough to be expressed as data (i.e. not arbitrary SQL), then you can use lateral joins to implement a rules execution engine.

## Hands-on Example

Suppose we have a dataset about birds, and we'd like to subscribe to all the birds who satisfy a set of rules.
Instead of creating a separate view for each rule, we will encode the rules **as data** and use a `LATERAL` join to execute them.

A `LATERAL` join is essentially a `for` loop; for each element of one dataset, do something with another dataset.
In our example, for each rule in a `bird_rules` dataset, we filter the `birds` dataset according to the rule.

### Create Resources

1. Create the `birds` table and insert some birds.
    ```sql
    CREATE TABLE birds (
    id INT,                           
    name VARCHAR(50),
    wingspan_cm FLOAT,
    colors jsonb                            
    );

    INSERT INTO birds (id, name, wingspan_cm, colors) VALUES
    (1, 'Sparrow', 15.5, '["Brown"]'),
    (2, 'Blue Jay', 20.2, '["Blue"]'),
    (3, 'Cardinal', 22.1, '["Red"]'),
    (4, 'Robin', 18.7, '["Red","Brown"]'),
    (5, 'Hummingbird', 8.2, '["Green"]'),
    (6, 'Penguin', 99.5, '["Black", "White"]'),
    (7, 'Eagle', 200.8, '["Brown"]'),
    (8, 'Owl', 105.3, '["Gray"]'),
    (9, 'Flamingo', 150.6, '["Pink"]'),
    (10, 'Pelican', 180.4, '["White"]');
    ```
1. Create the `bird_rules` table and create a few rules.
    ```sql
    CREATE TABLE bird_rules (
    id INT,
    starts_with CHAR(1),
    wingspan_operator VARCHAR(3),
    wingspan_cm FLOAT,
    colors JSONB
    );

    INSERT INTO bird_rules (id, starts_with, wingspan_operator, wingspan_cm, colors)
    VALUES
    (1, 'P', 'GTE', 50.0, '["Blue"]'),
    (2, 'P', 'LTE', 100.0, '["Black","White"]'),
    (3, 'R', 'GTE', 20.0, '["Red"]');
    ```
    Each rule has a unique `id` and encodes filters on wingspan and color, with `'GTE'` meaning "greater than or equal" and `'LTE'` meaning "less than or equal". For more complicated rules with varying schemas, you may consider using the [`jsonb` type](/sql/types/jsonb) and adjusting the logic in the upcoming view to suit your needs.

### Create the View

Here is the view that will execute our bird rules:

```sql
CREATE VIEW birds_filtered AS
SELECT r.id AS rule_id, b.name, b.colors, b.wingspan_cm
FROM
-- retrieve bird rules
(SELECT id, starts_with, wingspan_operator, wingspan_cm, colors FROM bird_rules) r,
-- for each bird rule, find the birds who satisfy it
LATERAL (
    SELECT *
    FROM birds
    WHERE r.starts_with = SUBSTRING(birds.name, 1, 1)
        AND (
            (r.wingspan_operator = 'GTE' AND birds.wingspan_cm >= r.wingspan_cm)
            OR
            (r.wingspan_operator = 'LTE' AND birds.wingspan_cm <= r.wingspan_cm)
        )
        AND r.colors <@ birds.colors 
) AS b;
```

Create an index to trigger incremental computation.

```sql
CREATE DEFAULT INDEX ON birds_filtered;
```

### Subscribe to Changes

1. Subscribe to the changes of `birds_filtered`.
    ```sql
    COPY(SUBSCRIBE birds_filtered) TO STDOUT;
    ```
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |   name   |      colors         | wingspan_cm
    --------------|---------|---------|----------|---------------------|------------
    1688673701670      1         2       Penguin     ["Black","White"]       99.5
    ```
    Notice that only the Penguin satisfies a rule, and in fact it's rule 2.
1. In a separate session, insert a new bird that satisfies one of the existing rules.
    ```sql
    INSERT INTO birds VALUES (11, 'Really big robin', 25.0, '["Red"]');
    ```
    Back in the `SUBSCRIBE` terminal, notice the output was immediately updated.
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |       name         |  colors   | wingspan_cm
    --------------|---------|---------|--------------------|-----------|------------
    1688674195279      1         3       Really big robin     ["Red"]        25
    ```
1. To see what happens, delete rule 3.
    ```sql
    DELETE FROM bird_rules WHERE id = 3;
    ```
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |       name         |  colors   | wingspan_cm
    --------------|---------|---------|--------------------|-----------|------------
    1688674195279     -1         3       Really big robin     ["Red"]        25
    ```
    Notice the bird was removed because the rule no longer exists.
1. Now let's update an existing bird so that it satisfies a new rule. It turns out our penguin also has some blue coloration.
    ```sql
    UPDATE birds SET colors = '["Black","White","Blue"]' WHERE name = 'Penguin';
    ```
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |   name   |      colors              | wingspan_cm
    --------------|---------|---------|----------|--------------------------|------------
    1688675781416      1         1       Penguin   ["Black","White","Blue"]        99.5
    1688675781416      1         2       Penguin   ["Black","White","Blue"]        99.5
    ```
    Now the penguin satisfies both rules 1 and 2.

### Clean Up

```sql
DROP TABLE birds CASCADE;
DROP TABLE bird_rules CASCADE;
```

## Conclusion

The example presented here 
