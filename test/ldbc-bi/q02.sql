\set date '\'2010-06-08\'::timestamp'
\set tagClass '\'ChristianBishop\''
-- materialize=> \i q02.sql
--         tag.name         | coalesce | coalesce | diff
-- -------------------------+----------+----------+------
--  Pope_John_XXIII         |       13 |       36 |   23
--  Pope_Pius_IX            |       19 |       38 |   19
--  Pope_John_Paul_II       |       38 |       54 |   16
--  Pope_Benedict_XVI       |       27 |       40 |   13
--  Pope_Pius_X             |       22 |       25 |    3
--  Pope_Leo_XIII           |       24 |       26 |    2
--  Pope_Paul_VI            |       37 |       35 |    2
--  Pope_Pius_XI            |       20 |       22 |    2
--  Pope_Gregory_I          |       15 |       16 |    1
--  Pope_Gregory_IX         |        1 |        0 |    1
--  Pope_Pius_XII           |       32 |       33 |    1
--  Ambrose                 |        1 |        1 |    0
--  Anselm_of_Canterbury    |        0 |        0 |    0
--  Augustine_of_Canterbury |        0 |        0 |    0
--  Charles_Borromeo        |        0 |        0 |    0
--  John_Henry_Newman       |        0 |        0 |    0
--  Pope_Adrian_IV          |        0 |        0 |    0
--  Pope_Alexander_III      |        0 |        0 |    0
--  Pope_Alexander_VI       |        0 |        0 |    0
--  Pope_Alexander_VII      |        0 |        0 |    0
--  Pope_Benedict_XIV       |        0 |        0 |    0
--  Pope_Boniface_VIII      |        0 |        0 |    0
--  Pope_Clement_V          |        0 |        0 |    0
--  Pope_Clement_VII        |        0 |        0 |    0
--  Pope_Clement_VIII       |        0 |        0 |    0
--  Pope_Clement_XI         |        0 |        0 |    0
--  Pope_Eugene_III         |        0 |        0 |    0
--  Pope_Eugene_IV          |        0 |        0 |    0
--  Pope_Gregory_XIII       |        0 |        0 |    0
--  Pope_Gregory_XVI        |        0 |        0 |    0
--  Pope_Innocent_III       |        0 |        0 |    0
--  Pope_Innocent_X         |        0 |        0 |    0
--  Pope_Innocent_XI        |        0 |        0 |    0
--  Pope_John_Paul_I        |        0 |        0 |    0
--  Pope_Julius_II          |        0 |        0 |    0
--  Pope_Leo_I              |        0 |        0 |    0
--  Pope_Leo_X              |        0 |        0 |    0
--  Pope_Paul_III           |        0 |        0 |    0
--  Pope_Paul_IV            |        0 |        0 |    0
--  Pope_Paul_V             |        0 |        0 |    0
--  Pope_Pius_II            |        0 |        0 |    0
--  Pope_Pius_IV            |        0 |        0 |    0
--  Pope_Pius_VI            |        0 |        0 |    0
--  Pope_Pius_VII           |        0 |        0 |    0
--  Pope_Sixtus_IV          |        0 |        0 |    0
--  Pope_Sixtus_V           |        0 |        0 |    0
--  Pope_Urban_VIII         |        0 |        0 |    0
--  Rowan_Williams          |        0 |        0 |    0
--  Thomas_Becket           |        0 |        0 |    0
-- (49 rows)
--
-- Time: 42939.038 ms (00:42.939)
/* Q2. Tag evolution
\set date '\'2012-06-01\''::timestamp
\set tagClass '\'MusicalArtist\''
*/
WITH
MyTag AS (
SELECT Tag.id AS id, Tag.name AS name
  FROM TagClass
  JOIN Tag
    ON Tag.TypeTagClassId = TagClass.id
 WHERE TagClass.name = :tagClass
),
detail AS (
SELECT t.id as TagId
     , count(CASE WHEN Message.creationDate <  :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth1
     , count(CASE WHEN Message.creationDate >= :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth2
  FROM MyTag t
  JOIN Message_hasTag_Tag
         ON Message_hasTag_tag.TagId = t.id
  JOIN Message
    ON Message.MessageId = Message_hasTag_tag.MessageId
   AND Message.creationDate >= :date
   AND Message.creationDate <  :date + INTERVAL '200 days'
 GROUP BY t.id
)
SELECT t.name AS "tag.name"
     , coalesce(countMonth1, 0)
     , coalesce(countMonth2, 0)
     , abs(coalesce(countMonth1, 0)-coalesce(countMonth2, 0)) AS diff
  FROM MyTag t LEFT JOIN detail ON t.id = detail.TagId
 ORDER BY diff desc, t.name
 LIMIT 100
;
