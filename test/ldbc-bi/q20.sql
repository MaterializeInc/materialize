\set company '\'Balkh_Airlines\''
\set person2Id 10995116285979::bigint
-- CREATE MATERIALIZED VIEW
-- Time: 650.821 ms
-- CREATE INDEX
-- Time: 476.647 ms
--  t | w
-- ---+---
-- (0 rows)
--
-- Time: 843.523 ms

-- emptiness is expected from a 20a parameter!

\set company '\'National_Airlines_(5M)\''
\set person2Id 13194139540317::bigint
-- CREATE MATERIALIZED VIEW
-- Time: 469.903 ms
-- CREATE INDEX
-- Time: 568.686 ms
--       dst       | w
-- ----------------+---
--  24189255819104 | 3
-- (1 row)
--
-- Time: 99518.999 ms (01:39.519)
-- Time: 0.145 ms

/* Q20. Recruitment
\set person2Id 32985348834889
\set company 'Express_Air'
 */
-- edge relation
CREATE OR REPLACE VIEW PathQ20 AS
  SELECT p1.personid AS src, p2.personid AS dst, min(abs(p1.classYear - p2.classYear)) + 1 AS w
  FROM Person_knows_person pp, Person_studyAt_University p1, Person_studyAt_University p2
  WHERE pp.person1id = p1.personid
    AND pp.person2id = p2.personid
    AND p1.universityid = p2.universityid
  GROUP BY p1.personid, p2.personid;
CREATE INDEX PathQ20_src ON PathQ20 (src);

-- materialize=> select count(*) from PathQ20;
--  count
-- -------
--  13732
-- (1 row)
--
-- Time: 22.068 ms

\set limit 1000

WITH minimal_paths AS (
  WITH MUTUALLY RECURSIVE
    paths (src bigint, dst bigint, w bigint) AS (
      SELECT :person2Id AS src, :person2Id AS dst, 0 AS w
      UNION
      SELECT paths1.src, paths2.dst, paths1.w + paths2.w
        FROM minimal_paths paths1
        JOIN PathQ20 paths2 -- step-transitive closure
          ON paths1.dst = paths2.src
    ),
    minimal_paths (src bigint, dst bigint, w bigint) AS (
      SELECT src, dst, min(w)
        FROM paths
    GROUP BY src, dst
    )
  SELECT src, dst, w FROM minimal_paths),
  dsts AS (
    SELECT personid
      FROM Person_workat_company pwc, Company c
     WHERE pwc.companyid = c.id AND c.name=:company
  ),
  completed_paths AS (
    SELECT dst, w
      FROM minimal_paths
     WHERE dst IN (SELECT * FROM dsts)
  ),
  results AS (
    SELECT dst, w
      FROM completed_paths
     WHERE w IN (SELECT min(w) FROM completed_paths)
  )
SELECT dst, w FROM results ORDER BY dst LIMIT 20;

-- dropping the unused src
/*
WITH minimal_paths AS (
  WITH MUTUALLY RECURSIVE
    paths (dst bigint, w bigint) AS (
      SELECT :person2Id AS dst, 0 AS w
      UNION
      SELECT paths2.dst, paths1.w + paths2.w
        FROM minimal_paths paths1
        JOIN PathQ20 paths2 -- step-transitive closure
          ON paths1.dst = paths2.src
    ),
    minimal_paths (dst bigint, w bigint) AS (
      SELECT dst, min(w)
        FROM paths
    GROUP BY dst
    )
  SELECT dst, w FROM minimal_paths),
  dsts AS (
    SELECT personid
      FROM Person_workat_company pwc, Company c
     WHERE pwc.companyid = c.id AND c.name=:company
  ),
  completed_paths AS (
    SELECT dst, w
      FROM minimal_paths
     WHERE dst IN (SELECT * FROM dsts)
  ),
  results AS (
    SELECT dst, w
      FROM completed_paths
     WHERE w IN (SELECT min(w) FROM completed_paths)
  )
SELECT dst, w FROM results ORDER BY dst LIMIT 20;
*/

-- tracking hops
/*
WITH minimal_paths AS (
  WITH MUTUALLY RECURSIVE (RETURN AT RECURSION LIMIT=:limit)
    paths (src bigint, dst bigint, w bigint, hops bigint) AS (
      SELECT :person2Id AS src, :person2Id AS dst, 0 AS w, 0 AS hops
      UNION
      SELECT paths1.src, paths2.dst, paths1.w + paths2.w, paths1.hops + 1
        FROM minimal_paths paths1
        JOIN PathQ20 paths2 -- step-transitive closure
          ON paths1.dst = paths2.src
    ),
    minimal_weights (src bigint, dst bigint, w bigint, hops bigint) AS (
      SELECT src, dst, min(w), hops
        FROM paths
    GROUP BY src, dst, hops
    ),
    minimal_paths (src bigint, dst bigint, w bigint, hops bigint) AS (
      SELECT src, dst, w, min(hops)
        FROM minimal_weights
    GROUP BY src, dst, w
    )
  SELECT src, dst, w, hops FROM minimal_paths),
  dsts AS (
    SELECT personid
      FROM Person_workat_company pwc, Company c
     WHERE pwc.companyid = c.id AND c.name=:company
  ),
  completed_paths AS (
    SELECT dst, w, hops
      FROM minimal_paths
     WHERE dst IN (SELECT * FROM dsts)
  ),
  results AS (
    SELECT dst, w, hops
      FROM completed_paths
     WHERE w IN (SELECT min(w) FROM completed_paths)
  )
SELECT dst, w, hops FROM results ORDER BY dst LIMIT 20;
*/

/*
WITH MUTUALLY RECURSIVE
  srcs(f bigint) AS (SELECT :person2Id),
  dsts(t bigint) AS (
      SELECT personid
      FROM Person_workat_company pwc, Company c
      WHERE pwc.companyid = c.id AND c.name=:company
  ),
  -- Try to find any path with a faster two way BFS

  -- visited nodes plus (on each iteration) nodes in PathQ20 we haven't yet seen
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

  -- are we there yet? at first, no (unless src is a dst)
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
          WITH ss AS (SELECT * FROM shorts),
               toExplore AS (SELECT * FROM ss WHERE dead = false ORDER BY w limit 1000),
               -- assumes graph is undirected
               newPoints(dir, gsrc, dst, w, dead) AS (
                   SELECT e.dir, e.gsrc AS gsrc, p.dst AS dst, e.w + p.w AS w, false AS dead
                   FROM PathQ20 p JOIN toExplore e ON (e.dst = p.src)
                   UNION ALL
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
*/
