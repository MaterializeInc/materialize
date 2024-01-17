\set city1Id 655::bigint
\set city2Id 1138::bigint
-- materialize=> \i q19.sql
-- CREATE MATERIALIZED VIEW
-- Time: 624.232 ms
-- CREATE INDEX
-- Time: 497.689 ms
--        f        |       t        | w
-- ----------------+----------------+----
--  24189255818627 | 26388279072272 | 39
-- (1 row)
--
-- Time: 8267.997 ms (00:08.268)
--
-- our simpler query
--
--       src       |      dst       | w
-- ----------------+----------------+----
--  24189255818627 | 26388279072272 | 39
-- (1 row)
--
-- Time: 21601.377 ms (00:21.601)

/* Q19. Interaction path between cities
\set city1Id 608
\set city2Id 1148
 */

CREATE OR REPLACE VIEW PathQ19 AS
WITH
  -- asymmetrize...
  knows_asymmetric AS (
    SELECT person1id, person2id
      FROM Person_knows_person
     WHERE person1id < person2id
  ),
  -- compute interaction scores (no interactions means we ignore that 'knows' relationship)
  weights(src, dst, w) AS (
    SELECT
        person1id AS src,
        person2id AS dst,
        greatest(round(40 - sqrt(count(*)))::bigint, 1) AS w
    FROM Message m1,
         Message m2,
         knows_asymmetric pp
    WHERE pp.person1id = least(m1.creatorpersonid, m2.creatorpersonid)
      AND pp.person2id = greatest(m1.creatorpersonid, m2.creatorpersonid)
      AND m1.parentmessageid = m2.messageid
      AND m1.creatorpersonid <> m2.creatorpersonid
 GROUP BY src, dst
)
-- resymmetrize
SELECT src, dst, w FROM weights
UNION ALL
SELECT dst, src, w FROM weights;

CREATE INDEX PathQ19_src ON PathQ19 (src);

WITH
  srcs AS (SELECT id FROM Person WHERE locationcityid = :city1Id),
  dsts AS (SELECT id FROM Person WHERE locationcityid = :city2Id),
  completed_paths AS (
    WITH MUTUALLY RECURSIVE
      paths (src bigint, dst bigint, w double precision) AS (
          SELECT id AS src,
                 id AS dst,
                 0::double precision AS w
                 FROM srcs
          UNION
          SELECT paths1.src AS src,
                 paths2.dst AS dst,
                 paths1.w + paths2.w AS w
            FROM minimal_paths paths1
            JOIN PathQ19 paths2 -- step-transitive closure
              ON paths1.dst = paths2.src
      ),
      minimal_paths (src bigint, dst bigint, w double precision) AS (
        SELECT src, dst, min(w)
          FROM paths
      GROUP BY src, dst
      )
    SELECT src, dst, w
      FROM minimal_paths
     WHERE dst = ANY (SELECT id FROM dsts)
  )
SELECT src, dst, w
  FROM completed_paths
 WHERE w = (SELECT min(w) FROM completed_paths);

/*
WITH MUTUALLY RECURSIVE
  srcs (f bigint) AS (SELECT id FROM Person WHERE locationcityid = :city1Id),
  dsts (t bigint) AS (SELECT id FROM Person WHERE locationcityid = :city2Id),
  shorts (dir bool, gsrc bigint, dst bigint, w double precision, dead bool, iter bigint) AS (
      (
          SELECT false, f, f, 0::double precision, false, 0 FROM srcs
          UNION ALL
          SELECT true, t, t, 0::double precision, false, 0 FROM dsts
      )
      UNION
      (
          WITH
          ss AS (SELECT * FROM shorts),
          toExplore AS (SELECT * FROM ss WHERE dead = false ORDER BY w LIMIT 1000),
          -- assumes graph is undirected
          newPoints(dir, gsrc, dst, w, dead) AS (
              SELECT e.dir, e.gsrc AS gsrc, p.dst AS dst, e.w + p.w AS w, false AS dead
              FROM PathQ19 p JOIN toExplore e ON (e.dst = p.src)
              UNION
              SELECT dir, gsrc, dst, w, dead OR EXISTS (SELECT * FROM toExplore e WHERE e.dir = o.dir AND e.gsrc = o.gsrc AND e.dst = o.dst) FROM ss o
          ),
          fullTable AS (
              SELECT DISTINCT ON(dir, gsrc, dst) dir, gsrc, dst, w, dead
              FROM newPoints
              ORDER BY dir, gsrc, dst, w, dead DESC
          ),
          found AS (
              SELECT min(l.w + r.w) AS w
              FROM fullTable l, fullTable r
              WHERE l.dir = false AND r.dir = true AND l.dst = r.dst
          )
          SELECT dir,
                 gsrc,
                 dst,
                 w,
                 dead or (coalesce(t.w > (SELECT f.w/2 FROM found f), false)),
                 e.iter + 1 AS iter
          FROM fullTable t, (SELECT iter FROM toExplore LIMIT 1) e
      )
  ),
  ss (dir bool, gsrc bigint, dst bigint, w double precision, iter bigint) AS (
      SELECT dir, gsrc, dst, w, iter FROM shorts WHERE iter = (SELECT max(iter) FROM shorts)
  ),
  results (f bigint, t bigint, w double precision) AS (
      SELECT l.gsrc, r.gsrc, min(l.w + r.w)
      FROM ss l, ss r
      WHERE l.dir = false AND r.dir = true AND l.dst = r.dst
      GROUP BY l.gsrc, r.gsrc
  )
SELECT * FROM results WHERE w = (SELECT min(w) FROM results) ORDER BY f, t;
*/
