\set person1Id 1450::bigint
\set person2Id 15393162796819
\set startDate '\'2012-11-06\''::timestamp
\set endDate '\'2012-11-10\''::timestamp
--  coalesce
-- ----------
--         4
-- (1 row)
--
-- Time: 144025.594 ms (02:24.026)

/* Q15. Trusted connection paths through forums created in a given timeframe
\set person1Id 21990232564808
\set person2Id 26388279076936
\set startDate '\'2010-11-01\''::timestamp
\set endDate '\'2010-12-01\''::timestamp
 */

WITH
  -- forums within the date range
  myForums AS (
      SELECT id FROM Forum f WHERE f.creationDate BETWEEN :startDate AND :endDate
  ),
  -- the (inverse) interaction scores between folks who know each other
  mm AS (
      SELECT least(msg.CreatorPersonId, reply.CreatorPersonId) AS src,
             greatest(msg.CreatorPersonId, reply.CreatorPersonId) AS dst,
             sum(case when msg.ParentMessageId is null then 10 else 5 end) AS w
      FROM Person_knows_Person pp, Message msg, Message reply
      WHERE true
            AND pp.person1id = msg.CreatorPersonId
            AND pp.person2id = reply.CreatorPersonId
            AND reply.ParentMessageId = msg.MessageId
            AND EXISTS (SELECT * FROM myForums f WHERE f.id = msg.containerforumid)
            AND EXISTS (SELECT * FROM myForums f WHERE f.id = reply.containerforumid)
      GROUP BY src, dst
  ),
  -- the true interaction scores, with 0 default for folks with no interactions
  edge AS (
      SELECT pp.person1id AS src,
             pp.person2id AS dst,
             10::double precision / (coalesce(w, 0) + 10) AS w
        FROM Person_knows_Person pp
   LEFT JOIN mm
          ON least(pp.person1id, pp.person2id) = mm.src
         AND greatest(pp.person1id, pp.person2id) = mm.dst
  ),
  completed_paths AS (
    WITH MUTUALLY RECURSIVE
      paths (src bigint, dst bigint, w double precision) AS (
          SELECT :person1Id AS src, :person1Id AS dst, 0::double precision AS w
          UNION
          SELECT paths1.src, paths2.dst, paths1.w + paths2.w
            FROM minimal_paths paths1
            JOIN edge paths2 -- step-transitive closure
              ON paths1.dst = paths2.src
      ),
      minimal_paths (src bigint, dst bigint, w double precision) AS (
        SELECT src, dst, min(w)
          FROM paths
      GROUP BY src, dst
      )
    SELECT src, dst, w
      FROM minimal_paths
     WHERE dst = :person2Id),
  results AS (
    SELECT dst, w
      FROM completed_paths
     WHERE w IN (SELECT min(w) FROM completed_paths)
  )
SELECT coalesce(w, -1) FROM results ORDER BY w ASC LIMIT 20;

/*
EXPLAIN WITH MUTUALLY RECURSIVE
  srcs (f bigint) AS (SELECT :person1Id),
  dsts (t bigint) AS (SELECT :person2Id),
  myForums (id bigint) AS (
      SELECT id FROM Forum f WHERE f.creationDate BETWEEN :startDate AND :endDate
  ),
  mm (src bigint, dst bigint, w bigint) AS (
      SELECT least(msg.CreatorPersonId, reply.CreatorPersonId) AS src,
             greatest(msg.CreatorPersonId, reply.CreatorPersonId) AS dst,
             sum(case when msg.ParentMessageId is null then 10 else 5 end) AS w
      FROM Person_knows_Person pp, Message msg, Message reply
      WHERE true
            AND pp.person1id = msg.CreatorPersonId
            AND pp.person2id = reply.CreatorPersonId
            AND reply.ParentMessageId = msg.MessageId
            AND EXISTS (SELECT * FROM myForums f WHERE f.id = msg.containerforumid)
            AND EXISTS (SELECT * FROM myForums f WHERE f.id = reply.containerforumid)
      GROUP BY src, dst
  ),
  path (src bigint, dst bigint, w double precision) AS (
      SELECT pp.person1id, pp.person2id, 10::double precision / (coalesce(w, 0) + 10)
      FROM Person_knows_Person pp left join mm on least(pp.person1id, pp.person2id) = mm.src AND greatest(pp.person1id, pp.person2id) = mm.dst
  ),
  -- bidirectional bfs for nonexistant paths
  pexists (src bigint, dir bool) AS (
      (
          SELECT f, true FROM srcs
          UNION
          SELECT t, false FROM dsts
      )
      UNION
      (
          WITH
          ss (src, dir) AS (SELECT src, dir FROM pexists),
          ns (src, dir) AS (SELECT p.dst, ss.dir FROM ss, path p WHERE ss.src = p.src),
          bb (src, dir) AS (SELECT src, dir FROM ns UNION ALL SELECT src, dir FROM ss),
          found (found) AS (
              SELECT 1 AS found
              FROM bb b1, bb b2
              WHERE b1.dir AND (NOT b2.dir) AND b1.src = b2.src
          )
          SELECT src, dir
          FROM ns
          WHERE NOT EXISTS (SELECT 1 FROM found)
          UNION
          SELECT -1, true
          WHERE EXISTS (SELECT 1 FROM found)
      )
  ),
  pathfound (c bool) AS (
      SELECT true AS c
      FROM pexists
      WHERE src = -1 AND dir
  ),
  shorts (dir bool, gsrc bigint, dst bigint, w double precision, dead bool, iter bigint) AS (
      (
          SELECT false, f, f, 0::double precision, false, 0 FROM srcs WHERE EXISTS (SELECT 1 FROM pathfound)
          UNION ALL
          SELECT true, t, t, 0::double precision, false, 0 FROM dsts WHERE EXISTS (SELECT 1 FROM pathfound)
      )
      UNION
      (
          WITH
          ss (dir, gsrc, dst, w, dead, iter) AS
             (SELECT * FROM shorts),
          toExplore (dir, gsrc, dst, w, dead, iter) AS
             (SELECT * FROM ss WHERE dead = false ORDER BY w limit 1000),
          -- assumes graph is undirected
          newPoints (dir, gsrc, dst, w, dead) AS (
              SELECT e.dir, e.gsrc AS gsrc, p.dst AS dst, e.w + p.w AS w, false AS dead
              FROM path p join toExplore e on (e.dst = p.src)
              UNION ALL
              SELECT dir, gsrc, dst, w, dead OR EXISTS (SELECT * FROM toExplore e WHERE e.dir = o.dir AND e.gsrc = o.gsrc AND e.dst = o.dst) FROM ss o
          ),
          fullTable (dir, gsrc, dst, w, dead) AS (
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
                 dead OR (coalesce(t.w > (SELECT f.w/2 FROM found f), false)),
                 e.iter + 1 AS iter
          FROM fullTable t, (SELECT iter FROM toExplore limit 1) e
      )
  ),
  ss (dir bool, gsrc bigint, dst bigint, w double precision, iter bigint) AS (
      SELECT dir, gsrc, dst, w, iter FROM shorts WHERE iter = (SELECT max(iter) FROM shorts)
  ),
  results(f bigint, t bigint, w double precision) AS (
      SELECT l.gsrc, r.gsrc, min(l.w + r.w)
      FROM ss l, ss r
      WHERE l.dir = false AND r.dir = true AND l.dst = r.dst
      GROUP BY l.gsrc, r.gsrc
  )
SELECT coalesce(min(w), -1) FROM results;
*/
