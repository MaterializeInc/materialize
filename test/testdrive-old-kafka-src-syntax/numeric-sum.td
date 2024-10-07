# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Note that this test uses an append-only source, so doesn't fully express the
# semantics of summing numeric values because values are non-retractable.

> CREATE TABLE numeric_insertions (a numeric);

> CREATE TABLE numeric_deletions (a numeric);

> CREATE VIEW numeric_values AS
  SELECT a FROM numeric_insertions
    EXCEPT (SELECT a FROM numeric_deletions);

> CREATE MATERIALIZED VIEW numeric_values_sum AS
    SELECT sum(a) AS sum_a FROM numeric_values;

> INSERT INTO numeric_insertions VALUES
  ('1.2'), ('2.3'), ('3.4');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
6.9

> INSERT INTO numeric_insertions VALUES ('0.0');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
6.9

> INSERT INTO numeric_insertions VALUES
  ('-1.2'), ('-2.3'), ('-3.4');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0

> INSERT INTO numeric_deletions VALUES
  ('1.2'), ('2.3'), ('3.4');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
-6.9

> INSERT INTO numeric_deletions VALUES
  ('-1.2'), ('-2.3'), ('-3.4');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0

# sum operation preserves commutativity, even when it appears lost from the
# datum's perspective.

> INSERT INTO numeric_insertions VALUES ('1e38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
100000000000000000000000000000000000000

> INSERT INTO numeric_insertions VALUES ('9e38');

# When you "fill" >38 digits of precision in the aggregator, signal
# pseudo-overflow with infinity. By returning infinity and preserving the actual
# sum in a larger data type behind the aggregation, we can preserve associativity
# and commutativity by e.g. allowing users to retract values that caused the
# "overflow." We can still continue to aggregate values "behind" this infinity,
# but this is meant to signal to users that they need to start retracting values
# from the aggregation or they risk a panic, which will occur once the
# aggregator's value exceeds its max precision.
> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
Infinity

# Side note that you cannot rescale Infinity
! SELECT sum_a::numeric(39,1)::text from numeric_values_sum;
contains:numeric field overflow

# Retracting/subtracting values lets you return to a valid state
> INSERT INTO numeric_insertions VALUES ('-9e38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
100000000000000000000000000000000000000

# Re-enter "overflow" state
> INSERT INTO numeric_deletions VALUES ('-9e38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
Infinity

# If you receive values while the aggregator in this "overflow" state, new
# values still received/tracked.
> INSERT INTO numeric_insertions VALUES ('1e-39');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
Infinity

> INSERT INTO numeric_deletions VALUES ('1e38'), ('9e38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0.000000000000000000000000000000000000001

# Infinity in this context is signed
> INSERT INTO numeric_insertions VALUES ('-8e38'), ('-7e38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
-Infinity

# Returns to zero
> INSERT INTO numeric_deletions VALUES ('1e-39'), ('-8e38'), ('-7e38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0

# Rounded values are still commutative, i.e. rounding is deterministic.
> INSERT INTO numeric_insertions VALUES ('1.23456789e-38'), ('-1.23456789e-38');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0

# However, sum is not associative from perspective of output
> INSERT INTO numeric_insertions VALUES
    ('0.987654321098765432109876543210987654321'),
    ('0.87654321098765432109876543210987654321');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
1.86419753208641975320864197532086419753

> INSERT INTO numeric_insertions VALUES
  ('-1.86419753208641975320864197532086419753');

# One might expect this to be zero, but there is a remainder from the original
# inputs in the aggregator that isn't visible from the narrower datum.
> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0.000000000000000000000000000000000000001

# Test NaN
> INSERT INTO numeric_insertions VALUES ('NaN');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
NaN

> INSERT INTO numeric_deletions VALUES ('NaN');

> SELECT sum_a::text AS sum_a FROM numeric_values_sum;
sum_a
----
0.000000000000000000000000000000000000001

# Test with specified scale
# - Rescale over sum

> CREATE TABLE numeric_scaled_insertions (a numeric);
> CREATE TABLE numeric_scaled_deletions (a numeric);

> CREATE VIEW numeric_scaled_values AS
  SELECT a FROM numeric_scaled_insertions
    EXCEPT (SELECT a FROM numeric_scaled_deletions);

> CREATE MATERIALIZED VIEW numeric_scaled_values_sum AS
    SELECT sum(a)::numeric(39, 3) AS sum_a FROM numeric_scaled_values;

> INSERT INTO numeric_scaled_insertions VALUES ('1.2'), ('2.3'), ('3.4001');

# Even though the sum is supposed to be rescaled to 3 decimals, the reduction
# before packing the values into a row trims the trailing zeroes. However, the
# rescale still works because the values are rounded to 3 units of scale.
> SELECT sum_a::text AS sum_a FROM numeric_scaled_values_sum;
sum_a
----
6.9

# Values < 5e(-scale) round to a version of zero
> INSERT INTO numeric_scaled_insertions VALUES ('0.00009');

> SELECT sum_a::text AS sum_a FROM numeric_scaled_values_sum;
sum_a
----
6.9

> INSERT INTO numeric_scaled_insertions VALUES ('0.0005');

> SELECT sum_a::text AS sum_a FROM numeric_scaled_values_sum;
sum_a
----
6.901

> INSERT INTO numeric_scaled_insertions VALUES ('1.2345'), ('2.3456'), ('3.4567');

> SELECT sum_a::text AS sum_a FROM numeric_scaled_values_sum;
sum_a
----
13.937

# Inputing values that are invalid for the scale generates errors, equivalent to
# overflow
> INSERT INTO numeric_scaled_insertions VALUES ('1e38');

# Note that this error happens inside the view, but outside the aggregation,
# i.e. this is an error caused by a unary function on a scalar value. This
# differs from the class of overflow that generates "Infinity," which occurs
# only in aggregation contexts as a means of preserving commutativity and
# associativity.
! SELECT sum_a::text AS sum_a FROM numeric_scaled_values_sum;
contains:Evaluation error: numeric field overflow

# Errored state is invertible by reducing aggregated value so it's expressable
# with the provided scale.
> INSERT INTO numeric_scaled_insertions VALUES ('-1e38');

> SELECT sum_a::text AS sum_a FROM numeric_scaled_values_sum;
sum_a
----
13.937

# - Rescale values from source

> CREATE TABLE numeric_scaled_input_insertions (a numeric);
> CREATE TABLE numeric_scaled_input_deletions (a numeric);

# Nest the `EXCEPT` in another clause to allow only non-retracted values to get
# rescaled.
> CREATE VIEW numeric_scaled_inputs AS
  SELECT a::numeric(38,3) FROM (
    SELECT a FROM numeric_scaled_input_insertions
    EXCEPT (SELECT a FROM numeric_scaled_input_deletions)
  );

> CREATE MATERIALIZED VIEW numeric_scaled_inputs_sum AS
    SELECT sum(a) AS sum_a FROM numeric_scaled_inputs;

> INSERT INTO numeric_scaled_input_insertions VALUES ('1.2'), ('2.3'), ('3.4');

> SELECT sum_a::text AS sum_a FROM numeric_scaled_inputs_sum;
sum_a
----
6.9

# Inputing values that are invalid for the scale generates errors, equivalent to
# overflow.
> INSERT INTO numeric_scaled_input_insertions VALUES ('1e38');

# Note that this error actually occurs in numeric_scaled_inputs, so is an
# evaluation error, rather than the aggregation returning infinity.
! SELECT sum_a::text AS sum_a FROM numeric_scaled_inputs_sum;
contains:Evaluation error: numeric field overflow

# However, retracting this values returns us to a good state.
> INSERT INTO numeric_scaled_input_deletions VALUES ('1e38');

> SELECT sum_a::text AS sum_a FROM numeric_scaled_inputs_sum;
sum_a
----
6.9
