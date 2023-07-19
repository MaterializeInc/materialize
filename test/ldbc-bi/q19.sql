\set city1Id 655::bigint
\set city2Id 1138::bigint
-- materialize=> \i q19.sql
-- CREATE MATERIALIZED VIEW
-- Time: 804.867 ms
-- CREATE INDEX
-- Time: 643.425 ms
-- ^CCancel request sent
-- psql:q19.sql:76: ERROR:  canceling statement due to user request
-- Time: 598992.554 ms (09:58.993)

/* Q19. Interaction path between cities
\set city1Id 608
\set city2Id 1148
 */

CREATE OR REPLACE MATERIALIZED VIEW PathQ19 AS
WITH
weights(src, dst, w) AS (
    SELECT
        person1id AS src,
        person2id AS dst,
        greatest(round(40 - sqrt(count(*)))::bigint, 1) AS w
    FROM (SELECT person1id, person2id FROM Person_knows_person WHERE person1id < person2id) pp, Message m1, Message m2
    WHERE pp.person1id = least(m1.creatorpersonid, m2.creatorpersonid) and pp.person2id = greatest(m1.creatorpersonid, m2.creatorpersonid) and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    GROUP BY src, dst
)
SELECT src, dst, w FROM weights
UNION ALL
SELECT dst, src, w FROM weights;

CREATE INDEX PathQ19_src_dst ON PathQ19 (src, dst);


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
