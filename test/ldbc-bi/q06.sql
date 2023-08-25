\set tag '\'Bob_Geldof\''
-- materialize=> \i q06.sql
-- CREATE MATERIALIZED VIEW
-- Time: 592.701 ms
-- CREATE INDEX
-- Time: 675.665 ms
--    person1.id   | authorityscore
-- ----------------+----------------
--  10995116278639 |          65214
--  21990232559802 |          51479
--  32985348837247 |          50946
--  21990232556992 |          39017
--  26388279074760 |          39017
--  32985348842998 |          39017
--  17592186047799 |          30823
--  21990232558345 |          27912
--   2199023263408 |          20476
--   2199023262183 |          17492
--  10995116282476 |          16881
--  24189255818812 |          15441
--  19791209306646 |           9344
--  17592186054552 |           6769
--  21990232557756 |           5222
--  24189255813105 |           2204
--  28587302330494 |           2204
--  15393162793576 |           1699
--  10995116286814 |           1674
--  26388279073722 |           1559
--   6597069771930 |            856
--  19791209310240 |            831
--  24189255816982 |            815
--            7402 |            802
--  21990232564251 |            683
--  15393162796905 |            643
--  10995116283030 |            327
--   6597069774064 |            318
--  13194139537210 |            233
--   4398046518031 |            199
--  24189255818900 |            199
--   4398046514439 |            182
--  30786325580499 |            150
--  15393162789011 |             91
--   2199023260638 |             63
--             261 |              0
--             358 |              0
--            2239 |              0
--            2565 |              0
--            2662 |              0
--            2735 |              0
--            3283 |              0
--            3470 |              0
--            4478 |              0
--            4857 |              0
--            6444 |              0
--            7139 |              0
--            7780 |              0
--            9533 |              0
--            9802 |              0
--            9874 |              0
--            9995 |              0
--   2199023255976 |              0
--   2199023256108 |              0
--   2199023256625 |              0
--   2199023256642 |              0
--   2199023256826 |              0
--   2199023257601 |              0
--   2199023257639 |              0
--   2199023257807 |              0
--   2199023258092 |              0
--   2199023258884 |              0
--   2199023259874 |              0
--   2199023260517 |              0
--   2199023260815 |              0
--   2199023261231 |              0
--   2199023261845 |              0
--   2199023263286 |              0
--   2199023263756 |              0
--   2199023264003 |              0
--   2199023264490 |              0
--   2199023265878 |              0
--   4398046511484 |              0
--   4398046512578 |              0
--   4398046512589 |              0
--   4398046513036 |              0
--   4398046513439 |              0
--   4398046514321 |              0
--   4398046514865 |              0
--   4398046515379 |              0
--   4398046517444 |              0
--   4398046518040 |              0
--   4398046519441 |              0
--   4398046519890 |              0
--   4398046519964 |              0
--   4398046521053 |              0
--   4398046521089 |              0
--   6597069768666 |              0
--   6597069768950 |              0
--   6597069768955 |              0
--   6597069769047 |              0
--   6597069769684 |              0
--   6597069771425 |              0
--   6597069771666 |              0
--   6597069773566 |              0
--   6597069773673 |              0
--   6597069774528 |              0
--   6597069776460 |              0
--   6597069776679 |              0
--   8796093022752 |              0
-- (100 rows)
--
-- Time: 30927.052 ms (00:30.927)

CREATE OR REPLACE MATERIALIZED VIEW PopularityScoreQ06 AS
  SELECT
      message2.CreatorPersonId AS person2id,
      count(*) AS popularityScore
  FROM Message message2
  JOIN Person_likes_Message like2
      ON like2.MessageId = message2.MessageId
  GROUP BY message2.CreatorPersonId;

CREATE INDEX PopularityScoreQ06_person2id ON PopularityScoreQ06 (person2id);

/* Q6. Most authoritative users on a given topic
\set tag '\'Arnold_Schwarzenegger\''
 */
 -- alternative version, with CTE to get a better plan
WITH applicable_posts AS (
       SELECT message1.MessageId,
              message1.CreatorPersonId AS person1id
         FROM Tag
         JOIN Message_hasTag_Tag
           ON Message_hasTag_Tag.TagId = Tag.id
         JOIN Message message1
           ON message1.MessageId = Message_hasTag_Tag.MessageId
        WHERE Tag.name = :tag
     ),
     poster_w_liker AS (
        SELECT DISTINCT
            message1.person1id,
            like2.PersonId AS person2id
        FROM applicable_posts message1
        LEFT JOIN Person_likes_Message like2
               ON like2.MessageId = message1.MessageId
           -- we don't need the Person itself as its ID is in the like
    )
SELECT pl.person1id AS "person1.id",
       sum(coalesce(ps.popularityScore, 0)) AS authorityScore
FROM poster_w_liker pl
LEFT JOIN PopularityScoreQ06 ps
         ON ps.person2id = pl.person2id
GROUP BY pl.person1id
ORDER BY authorityScore DESC, pl.person1id ASC
LIMIT 100
;

-- original umbra version, experiences filter pushdown anomalies
/*
WITH poster_w_liker AS (
        SELECT DISTINCT
            message1.CreatorPersonId AS person1id,
            like2.PersonId AS person2id
        FROM Tag
        JOIN Message_hasTag_Tag
          ON Message_hasTag_Tag.TagId = Tag.id
        JOIN Message message1
          ON message1.MessageId = Message_hasTag_Tag.MessageId
        LEFT JOIN Person_likes_Message like2
               ON like2.MessageId = message1.MessageId
           -- we don't need the Person itself as its ID is in the like
         WHERE Tag.name = :tag
    )
SELECT pl.person1id AS "person1.id",
       sum(coalesce(ps.popularityScore, 0)) AS authorityScore
FROM poster_w_liker pl
LEFT JOIN PopularityScoreQ06 ps
         ON ps.person2id = pl.person2id
GROUP BY pl.person1id
ORDER BY authorityScore DESC, pl.person1id ASC
LIMIT 100
;
*/
