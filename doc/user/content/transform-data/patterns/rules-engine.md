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

A rules engine is a powerful way to make decisions based on data.
With Materialize, you can execute those rules continuously.

Rule execution use cases can have many thousands of rules, so it's sometimes impractical to have a separate SQL view for each one.
Fortunately there is a pattern to address this need without having to create and manage many separate views.
If your rules are simple enough to be expressed as data (i.e. not arbitrary SQL), then you can use `LATERAL` joins to implement a rules execution engine.

## Hands-on Example

In this example, we have a dataset about birds. We need to subscribe to all birds in the dataset that satisfy a set of rules.
Instead of creating separate views for each rule, you can encode the rules **as data** and use a `LATERAL` join to execute them.

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
1. Create the `bird_rules` table and insert a few rules.
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
    Each rule has a unique `id` and encodes filters on starting letter, wingspan, and color. For `wingspan_operator`, `'GTE'` means "greater than or equal" and `'LTE'` means "less than or equal". For more complicated rules with varying schemas, consider using the [`jsonb` type](/sql/types/jsonb) and adjust the logic in the upcoming `LATERAL` join to suit your needs.

### Create the View

Here is the view that will execute our bird rules:

```sql
CREATE VIEW birds_filtered AS
SELECT r.id AS rule_id, b.name, b.colors, b.wingspan_cm
FROM
-- retrieve bird rules
(SELECT id, starts_with, wingspan_operator, wingspan_cm, colors FROM bird_rules) AS r,
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
    Notice that the majestic penguin satisfies rule 2. None of the other birds satisfy any of the rules.
1. In a separate session, insert a new bird that satisfies rule 3. Rule 3 requires a bird whose first letter is 'R', with a wingspan greater than or equal to 20 centimeters, and whose colors contain "Red". We will insert a "Really big robin" that satisfies this rule.
    ```sql
    INSERT INTO birds VALUES (11, 'Really big robin', 25.0, '["Red"]');
    ```
    Back in the `SUBSCRIBE` terminal, notice the output was immediately updated.
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |       name         |  colors   | wingspan_cm
    --------------|---------|---------|--------------------|-----------|------------
    1688674195279      1         3       Really big robin     ["Red"]        25
    ```
1. For fun, let's delete rule 3 and see what happens.
    ```sql
    DELETE FROM bird_rules WHERE id = 3;
    ```
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |       name         |  colors   | wingspan_cm
    --------------|---------|---------|--------------------|-----------|------------
    1688674195279     -1         3       Really big robin     ["Red"]        25
    ```
    Notice the bird was removed because the rule no longer exists.
1. Now let's update an existing bird so that it satisfies a new rule. It turns out our penguin also has some blue coloration we didn't notice before.
    ```sql
    UPDATE birds SET colors = '["Black","White","Blue"]' WHERE name = 'Penguin';
    ```
    ```nofmt
    mz_timestamp  | mz_diff | rule_id |   name   |      colors              | wingspan_cm
    --------------|---------|---------|----------|--------------------------|------------
    1688675781416     -1         2       Penguin   ["Black","White"]               99.5
    1688675781416      1         2       Penguin   ["Black","White","Blue"]        99.5
    1688675781416      1         1       Penguin   ["Black","White","Blue"]        99.5
    ```
    First there was an update to the row corresponding to the penguin's adherence to rule 2: a diff of -1 to delete the old value with just the black and white colors, and a diff of +1 to add the new value with black, white, and blue colors. Then there was a new record showing that the penguin now also adheres to rule 1.

### Clean Up

Press `Ctrl+C` to stop your `SUBSCRIBE` query and then drop the tables to clean up.

```sql
DROP TABLE birds CASCADE;
DROP TABLE bird_rules CASCADE;
```

## Conclusion

Rule execution engines can be much more complex than the minimal example presented here, but the underlying principle is the same; define the rules as **data** and use a `LATERAL` join to apply each rule to the dataset. Once you materialize the view, either by creating an index or creating it as a materialized view, the results will be kept up to date automatically as the dataset changes and as the rules change.
