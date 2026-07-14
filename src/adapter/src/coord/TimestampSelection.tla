\* Copyright Materialize, Inc. and contributors. All rights reserved.
\*
\* Use of this software is governed by the Business Source License
\* included in the LICENSE file at the root of this repository.
\*
\* As of the Change Date specified in that file, in accordance with
\* the Business Source License, use of this software will be governed
\* by the Apache License, Version 2.0.

--------------------------- MODULE TimestampSelection ---------------------------
\* Formal specification of the timestamp selection constraint solver in
\* Materialize's adapter layer (src/adapter/src/coord/timestamp_selection.rs).
\*
\* This models `determine_timestamp_via_constraints` as a pure function over
\* a bounded timestamp domain. The spec covers constraint generation for each
\* isolation level and QueryWhen variant, the solver logic, and the preference
\* (FreshestAvailable vs StalestValid) that breaks ties.
\*
\* Properties verified:
\*   1. Result respects all lower bounds (>= join of lower bounds)
\*   2. Result respects all upper bounds (<= meet of upper bounds, if any)
\*   3. Real-time ordering: result >= oracle_read_ts when oracle is a constraint
\*   4. Completeness: non-empty valid interval => solver succeeds
\*   5. Soundness: empty valid interval => solver returns error
\*   6. FreshestAvailable picks the largest valid timestamp <= upper
\*   7. StalestValid picks the smallest valid timestamp
\*
\* We use a small bounded domain (0..MaxTs) so TLC can exhaustively enumerate
\* all input combinations.

EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    MaxTs       \* Upper bound of the timestamp domain, e.g. 4

\* --------------------------------------------------------------------------
\* Timestamp domain
\* --------------------------------------------------------------------------

\* The set of all timestamps in our bounded domain.
Timestamps == 0..MaxTs

\* We model antichains as single-element sets (totally ordered timestamps).
\* The empty antichain {} represents the "closed" / unreachable frontier.
\* A singleton {t} represents the frontier at t.
\* This is a faithful simplification: Materialize timestamps are u64, totally ordered.

\* --------------------------------------------------------------------------
\* Enumerations
\* --------------------------------------------------------------------------

IsolationLevels == {"Serializable", "StrictSerializable", "StrongSessionSerializable"}

\* QueryWhen variants. AtTimestamp and AtLeastTimestamp carry a timestamp parameter,
\* modeled separately via the `when_ts` variable.
QueryWhens == {"Immediately", "FreshestTableWrite", "AtTimestamp", "AtLeastTimestamp"}

Preferences == {"FreshestAvailable", "StalestValid"}

\* --------------------------------------------------------------------------
\* Helper operators
\* --------------------------------------------------------------------------

\* The maximum of two timestamps.
Max(a, b) == IF a >= b THEN a ELSE b

\* The minimum of two timestamps.
Min(a, b) == IF a <= b THEN a ELSE b

\* Step back: the largest timestamp strictly less than t, saturating at 0.
StepBack(t) == IF t > 0 THEN t - 1 ELSE 0

\* largest_not_in_advance_of_upper: given the write frontier (upper),
\* compute the largest readable timestamp.
\* If upper is empty (closed collection), return MaxTs.
\* If upper is {t}, return StepBack(t).
LargestNotInAdvanceOfUpper(upper) ==
    IF upper = {}
    THEN MaxTs
    ELSE LET t == CHOOSE x \in upper : TRUE
         IN  StepBack(t)

\* --------------------------------------------------------------------------
\* QueryWhen predicates (mirroring the Rust methods)
\* --------------------------------------------------------------------------

AdvanceToTimestamp(when, when_ts) ==
    IF when \in {"AtTimestamp", "AtLeastTimestamp"}
    THEN when_ts
    ELSE -1  \* sentinel: no advance

ConstrainsUpper(when) ==
    when = "AtTimestamp"

CanAdvanceToUpper(when) ==
    when = "Immediately"

CanAdvanceToTimelineTs(when) ==
    when \in {"Immediately", "FreshestTableWrite"}

MustAdvanceToTimelineTs(when) ==
    when = "FreshestTableWrite"

\* --------------------------------------------------------------------------
\* needs_linearized_read_ts (determines whether oracle_read_ts is present)
\* --------------------------------------------------------------------------

NeedsLinearizedReadTs(isolation_level, when) ==
    MustAdvanceToTimelineTs(when) \/
    (CanAdvanceToTimelineTs(when) /\
     isolation_level \in {"StrictSerializable", "StrongSessionSerializable"})

\* --------------------------------------------------------------------------
\* Constraint generation
\*
\* Models the body of determine_timestamp_via_constraints.
\* We compute the effective lower bound and upper bound as single timestamps
\* (since our domain is totally ordered, the join of lower bounds is just max,
\* and the meet of upper bounds is just min).
\* --------------------------------------------------------------------------

\* ComputeLowerBound: given all inputs, compute the effective lower bound.
\* This is the maximum of all lower-bound constraints.
ComputeLowerBound(since, when, when_ts, oracle_read_ts, has_oracle,
                  real_time_recency_ts, has_rtr,
                  isolation_level, session_oracle_ts, has_session_oracle,
                  largest_niao_upper) ==
    LET
        \* Start from 0 (Timestamp::minimum)
        base == 0

        \* Lower bound from collection since (read hold)
        after_since == Max(base, since)

        \* Lower bound from QueryWhen (AS OF / AT LEAST)
        adv == AdvanceToTimestamp(when, when_ts)
        after_when == IF adv >= 0 THEN Max(after_since, adv) ELSE after_since

        \* Lower bound from oracle_read_ts
        \* Added when: has_oracle /\ (isolation_level /= "StrongSessionSerializable" \/ MustAdvanceToTimelineTs(when))
        oracle_applies == has_oracle /\
            (isolation_level /= "StrongSessionSerializable" \/ MustAdvanceToTimelineTs(when))
        after_oracle == IF oracle_applies THEN Max(after_when, oracle_read_ts) ELSE after_when

        \* Lower bound from real_time_recency_ts
        after_rtr == IF has_rtr THEN Max(after_oracle, real_time_recency_ts) ELSE after_oracle

        \* Lower bounds from StrongSessionSerializable
        after_sss_session == IF isolation_level = "StrongSessionSerializable" /\ has_session_oracle
                             THEN Max(after_rtr, session_oracle_ts)
                             ELSE after_rtr

        \* The heuristic: min(largest_niao_upper, oracle_read_ts) as additional lower bound
        \* Applies when: StrongSessionSerializable /\ can_advance_to_upper /\ can_advance_to_timeline_ts
        sss_heuristic_applies == isolation_level = "StrongSessionSerializable" /\
                                 CanAdvanceToUpper(when) /\ CanAdvanceToTimelineTs(when)
        sss_advance_to == IF has_oracle
                          THEN Min(largest_niao_upper, oracle_read_ts)
                          ELSE largest_niao_upper
        after_sss_heuristic == IF sss_heuristic_applies
                               THEN Max(after_sss_session, sss_advance_to)
                               ELSE after_sss_session
    IN
        after_sss_heuristic

\* ComputeUpperBound: the effective upper bound.
\* Returns MaxTs + 1 if there is no upper bound constraint (unconstrained above).
\* Otherwise returns the AS OF timestamp.
NoUpperBound == MaxTs + 1

ComputeUpperBound(when, when_ts) ==
    IF ConstrainsUpper(when)
    THEN when_ts
    ELSE NoUpperBound

\* --------------------------------------------------------------------------
\* Preference selection (mirrors the Rust code)
\* --------------------------------------------------------------------------

ComputePreference(when, isolation_level, has_timeline) ==
    IF CanAdvanceToUpper(when) /\
       (isolation_level = "Serializable" \/ ~has_timeline)
    THEN "FreshestAvailable"
    ELSE "StalestValid"

\* --------------------------------------------------------------------------
\* The solver: given lower bound, upper bound, preference, and
\* largest_not_in_advance_of_upper, compute the chosen timestamp or error.
\*
\* Returns a record [ok |-> BOOLEAN, ts |-> Int]
\* --------------------------------------------------------------------------

Solve(lower_bound, upper_bound, preference, largest_niao_upper) ==
    LET
        \* Start from lower_bound
        candidate_base == lower_bound

        \* If FreshestAvailable, advance to min(upper_bound, largest_niao_upper)
        freshest_target == IF upper_bound = NoUpperBound
                           THEN largest_niao_upper
                           ELSE Min(upper_bound, largest_niao_upper)
        candidate == IF preference = "FreshestAvailable"
                     THEN Max(candidate_base, freshest_target)
                     ELSE candidate_base

        \* Check validity: candidate must be >= lower_bound and <= upper_bound
        valid == candidate >= lower_bound /\
                 (upper_bound = NoUpperBound \/ candidate <= upper_bound)
    IN
        [ok |-> valid, ts |-> candidate]

\* --------------------------------------------------------------------------
\* Full timestamp determination: compose constraint generation + solver
\* --------------------------------------------------------------------------

DetermineTimestamp(since, upper, when, when_ts, oracle_read_ts, has_oracle,
                   real_time_recency_ts, has_rtr,
                   isolation_level, session_oracle_ts, has_session_oracle) ==
    LET
        largest_niao == LargestNotInAdvanceOfUpper(upper)

        lower_bound == ComputeLowerBound(since, when, when_ts, oracle_read_ts, has_oracle,
                                         real_time_recency_ts, has_rtr,
                                         isolation_level, session_oracle_ts, has_session_oracle,
                                         largest_niao)

        upper_bound == ComputeUpperBound(when, when_ts)

        \* We model has_timeline as TRUE for simplicity (the common case).
        preference == ComputePreference(when, isolation_level, TRUE)

        result == Solve(lower_bound, upper_bound, preference, largest_niao)
    IN
        [ok          |-> result.ok,
         ts          |-> result.ts,
         lower_bound |-> lower_bound,
         upper_bound |-> upper_bound,
         preference  |-> preference]

\* --------------------------------------------------------------------------
\* State: we use a single-state specification (no temporal behavior).
\* TLC will check the invariants for all possible input combinations.
\* --------------------------------------------------------------------------

VARIABLES
    since,                  \* Read frontier (since) of collections, a timestamp
    upper,                  \* Write frontier (upper) of collections, a set (antichain)
    when,                   \* QueryWhen variant
    when_ts,                \* Timestamp parameter for AtTimestamp / AtLeastTimestamp
    oracle_read_ts,         \* Global oracle read timestamp
    has_oracle,             \* Whether oracle_read_ts is present
    real_time_recency_ts,   \* Real-time recency timestamp
    has_rtr,                \* Whether real_time_recency_ts is present
    isolation_level,        \* Isolation level
    session_oracle_ts,      \* Session-local oracle timestamp (for StrongSessionSerializable)
    has_session_oracle,     \* Whether session_oracle_ts is present
    result                  \* The computed result

vars == <<since, upper, when, when_ts, oracle_read_ts, has_oracle,
          real_time_recency_ts, has_rtr, isolation_level,
          session_oracle_ts, has_session_oracle, result>>

\* --------------------------------------------------------------------------
\* Init: enumerate all valid input combinations
\* --------------------------------------------------------------------------

Init ==
    /\ since \in Timestamps
    /\ upper \in {{t} : t \in Timestamps} \cup {{}}  \* singleton antichains or empty
    /\ when \in QueryWhens
    /\ when_ts \in Timestamps
    /\ oracle_read_ts \in Timestamps
    \* has_oracle is determined by NeedsLinearizedReadTs + having a timeline
    /\ has_oracle \in BOOLEAN
    /\ real_time_recency_ts \in Timestamps
    /\ has_rtr \in BOOLEAN
    /\ isolation_level \in IsolationLevels
    /\ session_oracle_ts \in Timestamps
    /\ has_session_oracle \in BOOLEAN
    \* Input validity constraints (from the Rust code's assertions/preconditions):
    \* real_time_recency_ts is only present when isolation_level = StrictSerializable
    /\ (has_rtr => isolation_level = "StrictSerializable")
    \* session_oracle_ts is only present when isolation_level = StrongSessionSerializable
    /\ (has_session_oracle => isolation_level = "StrongSessionSerializable")
    \* has_oracle should align with NeedsLinearizedReadTs for the common case
    \* (but we also allow has_oracle = FALSE even when needed, to model the case
    \* where there's no timeline)
    \* Compute the result
    /\ result = DetermineTimestamp(since, upper, when, when_ts,
                                    oracle_read_ts, has_oracle,
                                    real_time_recency_ts, has_rtr,
                                    isolation_level,
                                    session_oracle_ts, has_session_oracle)

\* No state transitions - this is a single-state spec for checking properties
\* over all input combinations.
Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

\* --------------------------------------------------------------------------
\* Invariants (Properties to verify)
\* --------------------------------------------------------------------------

\* Property 1: If the solver succeeds, the result >= the effective lower bound.
RespectLowerBound ==
    result.ok => result.ts >= result.lower_bound

\* Property 2: If the solver succeeds and there is an upper bound, the result <= it.
RespectUpperBound ==
    result.ok =>
        (result.upper_bound = NoUpperBound \/ result.ts <= result.upper_bound)

\* Property 3: Real-time ordering.
\* If the solver succeeds and oracle_read_ts was used as a constraint,
\* then the result >= oracle_read_ts.
\* The oracle is used as a lower bound when:
\*   has_oracle /\ (isolation /= StrongSessionSerializable \/ MustAdvanceToTimelineTs(when))
RealTimeOrdering ==
    LET oracle_was_constraint ==
        has_oracle /\
        (isolation_level /= "StrongSessionSerializable" \/ MustAdvanceToTimelineTs(when))
    IN
        (result.ok /\ oracle_was_constraint) => result.ts >= oracle_read_ts

\* Property 4: Completeness - if the valid interval is non-empty, the solver succeeds.
\* The valid interval is [lower_bound, upper_bound].
Completeness ==
    (result.lower_bound <= result.upper_bound \/ result.upper_bound = NoUpperBound)
        => result.ok

\* Property 5: Soundness - if the solver succeeds, the interval was non-empty.
Soundness ==
    result.ok =>
        (result.upper_bound = NoUpperBound \/ result.lower_bound <= result.upper_bound)

\* Property 6: FreshestAvailable picks the largest valid timestamp <= largest_not_in_advance_of_upper
\* (bounded by upper_bound if present).
FreshestIsMaximal ==
    LET
        largest_niao == LargestNotInAdvanceOfUpper(upper)
        target == IF result.upper_bound = NoUpperBound
                  THEN largest_niao
                  ELSE Min(result.upper_bound, largest_niao)
    IN
        (result.ok /\ result.preference = "FreshestAvailable") =>
            result.ts = Max(result.lower_bound, target)

\* Property 7: StalestValid picks the smallest valid timestamp (= the lower bound).
StalestIsMinimal ==
    (result.ok /\ result.preference = "StalestValid") =>
        result.ts = result.lower_bound

\* --------------------------------------------------------------------------
\* Combined invariant for TLC
\* --------------------------------------------------------------------------

TypeOK ==
    /\ since \in Timestamps
    /\ when \in QueryWhens
    /\ isolation_level \in IsolationLevels
    /\ result.ok \in BOOLEAN
    /\ result.ok => result.ts \in 0..(MaxTs + 1)

AllProperties ==
    /\ TypeOK
    /\ RespectLowerBound
    /\ RespectUpperBound
    /\ RealTimeOrdering
    /\ Completeness
    /\ Soundness
    /\ FreshestIsMaximal
    /\ StalestIsMinimal

================================================================================
