# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Note that this test uses an append-only source, so doesn't fully express the
# semantics of summing APD values because values are non-retractable. # A future
# iteration of this test will use a more robust source and improve the tests # to
# more accurately reflect the operation's semantics.

$ file-append path=values.csv
1.2
2.3
3.4

> CREATE MATERIALIZED SOURCE apd_csv
  FROM FILE '${testdrive.temp-dir}/values.csv' WITH (tail = true)
  FORMAT CSV WITH 1 COLUMNS

> CREATE VIEW apd_values AS
    SELECT column1::apd AS a
    FROM apd_csv;

> CREATE MATERIALIZED VIEW apd_values_sum AS
    SELECT sum(a) AS sum_a FROM apd_values;

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
6.9

$ file-append path=values.csv
0.0

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
6.9

$ file-append path=values.csv
-1.2
-2.3
-3.4
-1.2
-2.3
-3.4

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
-6.9

$ file-append path=values.csv
1.2
2.3
3.4

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0

# sum operation preserves commutativity, even when it appears lost from the
# datum's perspective.

$ file-append path=values.csv
1e38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
100000000000000000000000000000000000000

$ file-append path=values.csv
9e38

# When you "fill" >38 digits of precision in the aggregator, signal
# pseudo-overflow with infinity. By returning infinity and preserving the actual
# sum in a larger data type behind the aggregation, we can preserve associativity
# and commutativity by e.g. allowing users to retract values that caused the
# "overflow." We can still continue to aggregate values "behind" this infinity,
# but this is meant to signal to users that they need to start retracting values
# from the aggregation or they risk a panic, which will occur once the
# aggregator's value exceeds its max precision.
> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
Infinity

# Side note that you cannot rescale Infinity
! SELECT sum_a::apd(39,1)::text from apd_values_sum;
numeric field overflow

# Retracting/subtracting values lets you return to a valid state
$ file-append path=values.csv
-9e38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
100000000000000000000000000000000000000

# Re-enter "overflow" state
$ file-append path=values.csv
9e38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
Infinity

# If you receive values while the aggregator in this "overflow" state, new
# values still received/tracked.

$ file-append path=values.csv
1e-39
-1e38
-9e38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0.000000000000000000000000000000000000001

# Infinity in this context is signed
$ file-append path=values.csv
-9e38
-9e38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
-Infinity

# Returns to initial value successfully
$ file-append path=values.csv
9e38
9e38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0.000000000000000000000000000000000000001

# Returns to zero
$ file-append path=values.csv
-1e-39

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0

# Rounded values are still commutative, i.e. rounding is deterministic.
$ file-append path=values.csv
1.23456789e-38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0.000000000000000000000000000000000000012

$ file-append path=values.csv
-1.23456789e-38

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0

$ file-append path=values.csv
0.987654321098765432109876543210987654321
0.987654321098765432109876543210987654321

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
1.97530864219753086421975308642197530864

# However, sum is not associative from perspective of output
$ file-append path=values.csv
-1.97530864219753086421975308642197530864

# One might expect this to be zero, but there is a remainder from the original
# inputs in the aggregator that isn't visible from the narrower datum.
> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
0.000000000000000000000000000000000000002

# Test NaN

# NaN is valid input, and generates NaN
$ file-append path=values.csv
NaN

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
NaN

# However, NaN is non-negatable and our append-only source is non-retractable,
# so this outputs NaN forevermore
$ file-append path=values.csv
123

> SELECT sum_a::text AS sum_a FROM apd_values_sum;
sum_a
----
NaN

# Test with specified scale
# - Rescale over sum

$ file-append path=scaled_values.csv
1.2
2.3
3.4

> CREATE MATERIALIZED SOURCE apd_csv_2
  FROM FILE '${testdrive.temp-dir}/scaled_values.csv' WITH (tail = true)
  FORMAT CSV WITH 1 COLUMNS

> CREATE VIEW apd_scaled_values AS
    SELECT column1::apd AS a
    FROM apd_csv_2;

> CREATE MATERIALIZED VIEW apd_scaled_values_sum AS
    SELECT sum(a)::apd(39, 3) AS sum_a FROM apd_scaled_values;

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
6.900

$ file-append path=scaled_values.csv
0.0

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
6.900

$ file-append path=scaled_values.csv
-1.2
-2.3
-3.4

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
0.000

# Values < 5e(-scale) round to a version of zero
$ file-append path=scaled_values.csv
0.00049

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
0.000

$ file-append path=scaled_values.csv
0.0005

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
0.001

$ file-append path=scaled_values.csv
1.2345
2.3456
3.4567

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
7.038

# Inputing values that are invalid for the scale generates errors, equivalent to
# overflow
$ file-append path=scaled_values.csv
1e38

# Note that this error happens inside the view, but outside the aggregation,
# i.e. this is an error caused by a unary function on a scalar value. This differs
# from the class of overflow that generates "Infinity," which occurs only in
# aggregation contexts as a means of preserving commutativity and associativity.
! SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
Evaluation error: numeric field overflow

# Errored state is invertible by reducing aggregated value so it's expressable
# with the provided scale.
$ file-append path=scaled_values.csv
-1e38

> SELECT sum_a::text AS sum_a FROM apd_scaled_values_sum;
sum_a
----
7.038

# - Rescale values from source

$ file-append path=scaled_inputs.csv
1.2
2.3
3.4

> CREATE MATERIALIZED SOURCE apd_csv_3
  FROM FILE '${testdrive.temp-dir}/scaled_inputs.csv' WITH (tail = true)
  FORMAT CSV WITH 1 COLUMNS

> CREATE VIEW apd_scaled_inputs AS
    SELECT column1::apd(38,3) AS a
    FROM apd_csv_3;

> CREATE MATERIALIZED VIEW apd_scaled_inputs_sum AS
    SELECT sum(a)::apd AS sum_a FROM apd_scaled_inputs;

> SELECT sum_a::text AS sum_a FROM apd_scaled_inputs_sum;
sum_a
----
6.9

$ file-append path=scaled_inputs.csv
0.0

> SELECT sum_a::text AS sum_a FROM apd_scaled_inputs_sum;
sum_a
----
6.9

$ file-append path=scaled_inputs.csv
-1.2
-2.3
-3.4

> SELECT sum_a::text AS sum_a FROM apd_scaled_inputs_sum;
sum_a
----
0

# Inputing values that are invalid for the scale generates errors, equivalent to
# overflow
$ file-append path=scaled_inputs.csv
1e38

# Note that this error actually occurs in apd_scaled_inputs and permanently
# wedges the view
! SELECT sum_a::text AS sum_a FROM apd_scaled_inputs_sum;
Evaluation error: numeric field overflow

# Errored state is NOT invertible; would require retracting value that caused
# error in apd_scaled_inputs
$ file-append path=scaled_inputs.csv
-1e38

! SELECT sum_a::text AS sum_a FROM apd_scaled_inputs_sum;
Evaluation error: numeric field overflow
