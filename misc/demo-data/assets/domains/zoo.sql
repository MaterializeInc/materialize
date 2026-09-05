-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- =============================================================================
-- Zoo of Disappointing Animals (a front for mostly crime)
--
-- Two layers, one schema:
--   Front of house: visits, ratings of 64 disappointing animals.
--   Back of house:  shipments with consistent skim, "consultations" that only
--                   happen on skeptical-mood visits.
--
-- The correlation between disappointment and crime is baked in: skeptical
-- guests rate harsher AND trigger consultations, so hours with high average
-- disappointment also have higher envelope totals. The cover IS the tell.
--
-- Invariants:
--   1. COUNT(ratings) per visit = visits.n_animals_seen          (fanout)
--   2. SUM(shipments.declared - actual) is monotone non-positive (skim)
--   3. consultations.visit_id ⊆ visits where mood = 'skeptical'  (subset)
--   4. consultations.client_id is always a real visitor          (re-derivation)
--
-- Prerequisites: scaffold.sql, common/people.sql
-- =============================================================================

SELECT EXISTS (SELECT 1 FROM mz_views WHERE name = 'visits_core') AS already_loaded \gset
\if :already_loaded
\echo zoo already loaded; skipping.
\else

-- -----------------------------------------------------------------------------
-- Static lookups
-- -----------------------------------------------------------------------------

CREATE VIEW moods (id, name) AS VALUES
    (0, 'hopeful'),
    (1, 'skeptical'),
    (2, 'dragged_along'),
    (3, 'field_trip');

CREATE VIEW crate_labels (id, name) AS VALUES
    (0, 'fresh hay'),
    (1, 'veterinary supplies'),
    (2, 'enrichment toys'),
    (3, 'do not open');

CREATE VIEW service_codes (id, name) AS VALUES
    (0, 'tax advisory'),
    (1, 'logistics'),
    (2, 'asset relocation'),
    (3, 'reputation management'),
    (4, 'witness preparation'),
    (5, 'inventory adjustment'),
    (6, 'jurisdictional planning'),
    (7, 'unspecified consulting');

-- Species roster (the brochure version).
CREATE VIEW species (id, name, expected_disappointment) AS VALUES
    (0,  'Capybara',              2),
    (1,  'Sleeping Lion',         5),
    (2,  'Pigeon (Allegedly Eagle)', 5),
    (3,  'Sloth',                 3),
    (4,  'Empty Enclosure',       5),
    (5,  'Goat',                  2),
    (6,  'Tiger (Painted Mule)',  5),
    (7,  'Lizard in a Jar',       4),
    (8,  'Bear (Costume)',        4),
    (9,  'Penguin (Plastic)',     5),
    (10, 'Rabbit',                1),
    (11, 'Snake (Garden Hose)',   5);

-- 64 animals, each with a name, species, and a specific letdown reason.
-- animal_id is 0..63 so one byte mod 64 picks uniformly.
CREATE VIEW animals AS
SELECT
    id::int                                                            AS id,
    'ANI-' || lpad(id::text, 3, '0')                                   AS tag,
    (ARRAY[
        'Bartholomew','Mister Whiskers','Doctor Paws','Sir Naps-a-Lot',
        'Greg','The Disappointment','Beatrice','Captain Snore',
        'Marquis de Lounge','Pebbles','Twiggy','Mayor McMolt',
        'Lord Featherbottom','Janet','Cinder','Pudding',
        'Stumpy','Princess Vacant','Reginald','The Smell',
        'Ottoman','Doorstop','Ms. Hibernation','Mister Allegedly',
        'The Glistener','Frank','Fenestra','Dim Bulb',
        'Sergeant Nope','Roy','Crouton','The Refunder',
        'Hazelnut','Hugo','Mavis','Crinkle',
        'The Audit','Snickers','Phyllis','Buttercup',
        'Vince','Mister Eyes','Lump','The Suggestion',
        'Pretzel','Boris','Calamity','Tuffet',
        'Dr. Disappointment','The Husk','Marbles','The Receipt',
        'Twitch','The Bystander','Norbert','Cricket',
        'The Witness','Edna','Lasagna','The Defendant',
        'Pinto','Whimsy','The Inheritance','The Final Straw'
    ])[1 + id]                                                         AS name,
    mod(get_byte(digest('species:' || id::text, 'md5'), 0)::int, 12)   AS species_id,
    (ARRAY[
        'sleeps 22 hours a day',
        'is actually a rock with eyes glued on',
        'smaller than the brochure photo',
        'technically a pigeon',
        'enclosure is just a mirror',
        'has not moved since 2019',
        'turns out to be a costume',
        'enclosure card reads "TBD"',
        'visible only on tuesdays',
        'painted to look like another animal',
        'is a taxidermy with a fan blowing on it',
        'smells worse than expected',
        'enclosure contains only a sign reading SOON',
        'absent due to "training"',
        'considerably damper than promised'
    ])[1 + mod(get_byte(digest('reason:' || id::text, 'md5'), 0)::int, 15)] AS letdown_reason
FROM generate_series(0, 63) AS id;

CREATE DEFAULT INDEX ON animals;

-- -----------------------------------------------------------------------------
-- Visits: one per moment.
--
-- Byte budget on `random`:
--   [0..2] visit_id
--   [3]    visitor_id mod 256 (FK to people)
--   [4]    mood mod 4
--   [5]    n_animals_seen = (byte mod 5) + 1
--   [6]    arrival offset in seconds
--   [7]    consultation count seed (mod 3, gated by mood = skeptical)
-- -----------------------------------------------------------------------------
CREATE VIEW visits_core AS
SELECT
    moment,
    random,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                  AS id,
    get_byte(random, 3)                                          AS visitor_id,
    mod(get_byte(random, 4)::int, 4)                             AS mood_id,
    1 + mod(get_byte(random, 5)::int, 5)                         AS n_animals_seen,
    moment + (get_byte(random, 6)::text || ' seconds')::interval AS arrived_at,
    mod(get_byte(random, 7)::int, 3)                             AS consultation_seed
FROM random;

CREATE MATERIALIZED VIEW visits AS
SELECT
    v.id,
    v.visitor_id,
    p.name                                                       AS visitor_name,
    p.region                                                     AS visitor_region,
    m.name                                                       AS mood,
    v.n_animals_seen,
    v.arrived_at,
    -- Consultations only happen when the guest is "skeptical".
    CASE WHEN m.name = 'skeptical' THEN v.consultation_seed ELSE 0 END
                                                                 AS n_consultations
FROM visits_core v
JOIN people p ON p.id = v.visitor_id
JOIN moods m  ON m.id = v.mood_id;

-- -----------------------------------------------------------------------------
-- Ratings: one row per (visit, animal seen). Re-hash random with the rating
-- index. Skeptical guests rate +2 stars harsher (clamped to 5). That single
-- nudge produces the correlation between front-of-house disappointment and
-- back-of-house activity.
--
-- Child random bytes:
--   [0] animal_id mod 64
--   [1] base stars = (byte mod 5) + 1; bumped to +2 if parent mood skeptical
--   [2] viewed_at offset (seconds after arrival)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW ratings AS
WITH expanded AS (
    SELECT
        v.id                                                     AS visit_id,
        v.arrived_at,
        v.mood_id,
        generate_series(1, v.n_animals_seen)                     AS rating_no,
        digest(v.random::text || 'rating' ||
               generate_series(1, v.n_animals_seen)::text, 'md5') AS random
    FROM visits_core v
)
SELECT
    visit_id,
    rating_no,
    mod(get_byte(random, 0)::int, 64)                            AS animal_id,
    LEAST(
        5,
        1 + mod(get_byte(random, 1)::int, 5)
          + CASE WHEN mood_id = 1 THEN 2 ELSE 0 END
    )                                                            AS stars,
    arrived_at + (get_byte(random, 2)::text || ' seconds')::interval
                                                                 AS viewed_at
FROM expanded;

-- -----------------------------------------------------------------------------
-- Shipments: one per moment, re-hashing `random || 'shipment'`. Independent
-- entropy from visits so the two streams are not trivially correlated.
--
-- Byte budget:
--   [0..2] shipment_id
--   [3]    handler_id mod 256 (FK to people)
--   [4]    crate_label mod 4
--   [5]    declared_weight = byte + 50  (50..305 kg)
--   [6]    skim = byte mod 8            (subtracted, always)
-- -----------------------------------------------------------------------------
CREATE VIEW shipments_core AS
SELECT
    moment,
    digest(random::text || 'shipment', 'md5')                    AS random
FROM random;

CREATE MATERIALIZED VIEW shipments AS
SELECT
    moment                                                       AS arrived_at,
    get_byte(random, 0) +
    get_byte(random, 1) * 256 +
    get_byte(random, 2) * 65536                                  AS id,
    get_byte(random, 3)                                          AS handler_id,
    p.name                                                       AS handler_name,
    cl.name                                                      AS crate_label,
    (get_byte(random, 5) + 50)::numeric                          AS declared_weight_kg,
    ((get_byte(random, 5) + 50) - mod(get_byte(random, 6)::int, 8))::numeric
                                                                 AS actual_weight_kg,
    mod(get_byte(random, 6)::int, 8)::numeric                    AS skim_kg
FROM shipments_core
JOIN people p ON p.id = get_byte(random, 3)
JOIN crate_labels cl ON cl.id = mod(get_byte(random, 4)::int, 4);

-- -----------------------------------------------------------------------------
-- Consultations: 0..2 child rows per visit, gated by mood = skeptical.
-- client_id is CARRIED from the parent visit (not re-rolled) — that's how
-- "the guest meeting" maps to "the visitor in the cover story".
--
-- Child random bytes:
--   [0..2] consultation_id
--   [4]    service_code mod 8
--   [5]    envelope_mm = byte (0..255)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW consultations AS
WITH gated AS (
    SELECT
        v.id                                                     AS visit_id,
        v.visitor_id                                             AS client_id,
        v.arrived_at,
        v.consultation_seed,
        v.random
    FROM visits_core v
    JOIN moods m ON m.id = v.mood_id
    WHERE m.name = 'skeptical'
      AND v.consultation_seed > 0
),
expanded AS (
    SELECT
        visit_id,
        client_id,
        arrived_at,
        generate_series(1, consultation_seed)                    AS meeting_no,
        digest(random::text || 'consult' ||
               generate_series(1, consultation_seed)::text, 'md5') AS random
    FROM gated
)
SELECT
    e.visit_id,
    e.meeting_no,
    get_byte(e.random, 0) +
    get_byte(e.random, 1) * 256 +
    get_byte(e.random, 2) * 65536                                AS id,
    e.client_id,
    p.name                                                       AS client_name,
    sc.name                                                      AS service_code,
    get_byte(e.random, 5)::int                                   AS envelope_mm,
    e.arrived_at + (e.meeting_no * 7 || ' minutes')::interval    AS met_at
FROM expanded e
JOIN people p ON p.id = e.client_id
JOIN service_codes sc ON sc.id = mod(get_byte(e.random, 4)::int, 8);

-- -----------------------------------------------------------------------------
-- Aggregates: the demos.
-- -----------------------------------------------------------------------------

-- The headline join. Per-hour: how disappointing was the cover, and how busy
-- was the back of house? High avg_disappointment should track high envelope
-- totals, because both ride the skeptical-mood bytes.
CREATE VIEW cover_quality AS
WITH visit_disappointment AS (
    SELECT v.id AS visit_id, v.arrived_at, AVG(r.stars)::numeric(10,2) AS avg_stars
    FROM visits v
    JOIN ratings r ON r.visit_id = v.id
    GROUP BY v.id, v.arrived_at
),
hourly_visits AS (
    SELECT
        date_trunc('hour', arrived_at)             AS hour,
        COUNT(*)                                   AS visits,
        AVG(avg_stars)::numeric(10,2)              AS avg_disappointment
    FROM visit_disappointment
    GROUP BY date_trunc('hour', arrived_at)
),
hourly_shipments AS (
    SELECT
        date_trunc('hour', arrived_at)             AS hour,
        SUM(skim_kg)                               AS skim_kg
    FROM shipments
    GROUP BY date_trunc('hour', arrived_at)
),
hourly_consults AS (
    SELECT
        date_trunc('hour', met_at)                 AS hour,
        COUNT(*)                                   AS consultations,
        SUM(envelope_mm)                           AS envelope_mm_total
    FROM consultations
    GROUP BY date_trunc('hour', met_at)
)
SELECT
    v.hour,
    v.visits,
    v.avg_disappointment,
    COALESCE(s.skim_kg, 0)             AS skim_kg,
    COALESCE(c.consultations, 0)       AS consultations,
    COALESCE(c.envelope_mm_total, 0)   AS envelope_mm_total
FROM hourly_visits v
LEFT JOIN hourly_shipments s ON s.hour = v.hour
LEFT JOIN hourly_consults  c ON c.hour = v.hour;

-- Which animals attract the worst reviews? (The leaderboard of letdowns.)
CREATE VIEW animal_disappointment AS
SELECT
    a.tag,
    a.name,
    sp.name                              AS species,
    a.letdown_reason,
    COUNT(*)                             AS sightings,
    AVG(r.stars)::numeric(10,2)          AS avg_stars
FROM ratings r
JOIN animals a ON a.id = r.animal_id
JOIN species sp ON sp.id = a.species_id
GROUP BY a.tag, a.name, sp.name, a.letdown_reason;

-- Which services run hottest, and under how much cover?
CREATE VIEW business_efficiency AS
WITH visit_cover AS (
    SELECT v.id AS visit_id, AVG(r.stars)::numeric(10,2) AS cover_score
    FROM visits v JOIN ratings r ON r.visit_id = v.id
    GROUP BY v.id
)
SELECT
    c.service_code,
    COUNT(*)                                       AS meetings,
    AVG(c.envelope_mm)::numeric(10,2)              AS avg_envelope_mm,
    SUM(c.envelope_mm)                             AS total_envelope_mm,
    AVG(vc.cover_score)::numeric(10,2)             AS avg_cover_disappointment
FROM consultations c
JOIN visit_cover vc ON vc.visit_id = c.visit_id
GROUP BY c.service_code;

-- Cross-domain hook: per-person rap sheet. Joins client_id back through
-- people for use with banking / ecommerce demos.
CREATE VIEW client_activity AS
SELECT
    p.id                                AS person_id,
    p.name                              AS person_name,
    p.region,
    COUNT(*)                            AS consultations,
    SUM(c.envelope_mm)                  AS envelope_mm_total
FROM consultations c
JOIN people p ON p.id = c.client_id
GROUP BY p.id, p.name, p.region;

\endif

-- -----------------------------------------------------------------------------
-- Validation queries:
--
-- Heartbeat:
--   COPY (SUBSCRIBE (SELECT COUNT(*) FROM visits) WITH (progress = true)) TO STDOUT;
--
-- Invariant 1: total ratings = total declared animals seen (aggregate fanout).
--   SELECT (SELECT COUNT(*)             FROM ratings) =
--          (SELECT SUM(n_animals_seen)  FROM visits)  AS fanout_balances;
--
-- Invariant 2: skim is monotone non-positive in aggregate.
--   SELECT SUM(declared_weight_kg - actual_weight_kg) AS total_skim_kg FROM shipments;
--   -- should always be >= 0 (we're skimming, not adding)
--
-- Invariant 3: count of consultations equals total declared meetings from
-- skeptical visits. Aggregate form — survives 24-bit id collisions.
-- (The per-row JOIN version reports false positives when two visits share an
-- id and one is skeptical and the other isn't; see SKILL.md "Invariants vs.
-- id collisions".)
--   SELECT (SELECT COUNT(*) FROM consultations) =
--          (SELECT SUM(CASE WHEN m.name = 'skeptical'
--                           THEN mod(get_byte(v.random, 7)::int, 3) ELSE 0 END)
--           FROM visits_core v JOIN moods m ON m.id = v.mood_id) AS consultations_match;
--
-- Demo: hours where the cover ran best AND the back of house was busy.
--   SELECT * FROM cover_quality ORDER BY hour DESC LIMIT 20;
--
-- Demo: the leaderboard of letdowns.
--   SELECT * FROM animal_disappointment ORDER BY avg_stars DESC, sightings DESC LIMIT 10;
--
-- Demo (with banking loaded): people who are both clients AND have large
-- round-number transactions.
--   SELECT ca.person_name, ca.envelope_mm_total, ab.balance
--   FROM client_activity ca
--   JOIN account_balances ab ON ab.holder_name = ca.person_name
--   ORDER BY ca.envelope_mm_total DESC LIMIT 20;
-- -----------------------------------------------------------------------------
