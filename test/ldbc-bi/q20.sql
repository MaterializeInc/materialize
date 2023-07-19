\set company '\'Balkh_Airlines\''
\set person2Id 10995116285979::bigint
-- materialize=> \i q20.sql
-- CREATE MATERIALIZED VIEW
-- Time: 845.782 ms
-- CREATE INDEX
-- Time: 477.543 ms
-- ^CCancel request sent
-- psql:q20.sql:86: ERROR:  canceling statement due to user request
-- Time: 209054.017 ms (03:29.054)

/* Q20. Recruitment
\set person2Id 32985348834889
\set company 'Express_Air'
 */
CREATE OR REPLACE MATERIALIZED VIEW PathQ20 AS
  SELECT p1.personid AS src, p2.personid AS dst, min(abs(p1.classYear - p2.classYear)) + 1 AS w
  FROM Person_knows_person pp, Person_studyAt_University p1, Person_studyAt_University p2
  WHERE pp.person1id = p1.personid
    AND pp.person2id = p2.personid
    AND p1.universityid = p2.universityid
  GROUP BY p1.personid, p2.personid;
CREATE INDEX PathQ20_src_dst ON PathQ20 (src, dst);


WITH MUTUALLY RECURSIVE
  srcs(f bigint) AS (SELECT :person2Id),
  dsts(t bigint) AS (
      SELECT personid
      FROM Person_workat_company pwc, Company c
      WHERE pwc.companyid = c.id AND c.name=:company
  ),
  -- Try to find any path with a faster two way BFS
  anyPath (pos bigint) AS (
      SELECT f FROM srcs
      UNION
      (
          WITH
          ss AS (SELECT pos FROM anyPath)
          SELECT dst
          FROM ss, PathQ20
          WHERE pos = src AND NOT EXISTS (SELECT 1 FROM ss, dsts WHERE ss.pos = dsts.t)
      )
  ),
  pathexists (exists bool) AS (
      SELECT true WHERE EXISTS (SELECT 1 FROM anyPath ss, dsts WHERE ss.pos = dsts.t)
  ),
  shorts (dir bool, gsrc bigint, dst bigint, w bigint, dead bool, iter bigint) AS (
      (
          SELECT false, f, f, 0, false, 0 FROM srcs WHERE EXISTS (SELECT 1 FROM pathexists)
          UNION
          SELECT true, t, t, 0, false, 0 FROM dsts WHERE EXISTS (SELECT 1 FROM pathexists)
      )
      UNION
      (
          with
          ss AS (SELECT * FROM shorts),
          toExplore AS (SELECT * FROM ss WHERE dead = false ORDER BY w limit 1000),
          -- assumes graph is undirected
          newPoints(dir, gsrc, dst, w, dead) AS (
              SELECT e.dir, e.gsrc AS gsrc, p.dst AS dst, e.w + p.w AS w, false AS dead
              FROM PathQ20 p JOIN toExplore e ON (e.dst = p.src)
              union all
              SELECT dir, gsrc, dst, w, dead OR EXISTS (SELECT * FROM toExplore e WHERE e.dir = o.dir AND e.gsrc = o.gsrc AND e.dst = o.dst) FROM ss o
          ),
          fullTable AS (
              SELECT distinct ON(dir, gsrc, dst) dir, gsrc, dst, w, dead
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
          FROM fullTable t, (SELECT iter FROM toExplore limit 1) e
      )
  ),
  ss (dir bool, gsrc bigint, dst bigint, w bigint, iter bigint) AS (
      SELECT dir, gsrc, dst, w, iter FROM shorts WHERE iter = (SELECT max(iter) FROM shorts)
  ),
  results(f bigint, t bigint, w bigint) AS (
      SELECT l.gsrc, r.gsrc, min(l.w + r.w)
      FROM ss l, ss r
      WHERE l.dir = false AND r.dir = true AND l.dst = r.dst
      GROUP BY l.gsrc, r.gsrc
  )
SELECT t, w FROM results WHERE w = (SELECT min(w) FROM results) ORDER BY t LIMIT 20;
