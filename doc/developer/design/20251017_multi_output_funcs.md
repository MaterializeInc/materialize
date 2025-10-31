# Faster jsonb decoding

Json decoding in Materialize is slow when extracting multiple fields from a single object.
Extracting a single field is a linear-time operation in the length of the input.
Extracting $k$ fields from a row is quadratic in $k$ because each field extraction is independent.

This design doc outlines an approach to speed up multi-field extraction by performing a single pass over the input.

## Solution 1: Multi-output functions

At the moment, all scalar expressions produce exactly one output value in the non-error case.
We cannot express a function that is variadic in its output elements.

If we were to change this, we could offer a function from `jsonb` and a list of expressions to extract,
and return a datum for each extracted value. Roughly:

1. Evaluate the first argument to a `jsonb` value. If it is a map, all subsequent expressions must be string-valued, otherwise they must be integer-valued.
2. Push as many null outputs to the output vector as there are subsequent expressions.
3. Collect the subsequent expressions into a hash/btree map keyed by literal string or integer value and mapping to the output slot index.
4. Perform a single pass over the `jsonb` value, extracting each requested field and populating the corresponding output slot.

If any of the expressions error, the output is left in an undefined state and an error is returned.

Implementing this isn't easy, as it requires changes to how we optimize and evaluate expressions.
While this isn't a complete list, it might give an intuition as to what would need to change.
* `eval` on scalar expressions would take a mutable vector to push outputs to, rather than returning a single `Datum`. All call sites would need to be updated to pass the mutable vector.
* Literals could encode multiple columns (and types) rather than a single one.
* The expression optimizer would need to be updated to handle multi-output expressions.
* The output column type of scalar expression would be a list of types rather than a single type. Adjusting Materialize to handle this would be a large undertaking.
* Now it becomes spooky! A binary function takes two input values and maps it to one. It takes _two_ expressions, but it really needs two input values. A valid capp to a binary function would be a single multi-output expression that produces two outputs. This would require rethinking how functions are represented and evaluated.
  * Similarly, `if` need a single input expression for the condition, but for the branches it could accept multi-output expressions with the same number of outputs.

For these reasons, I think it's better to introduce a new kind of scalar expression that we lower from `MirScalarExpr` to decoupule inputs and outputs.

### StackExpression

A `StackExpression` is a scalar expression that operators on a stack of input values and produces any number of output values.


## Solution 2: Complex state in `Datum`

## Solution 3: Compiled scalar expression
