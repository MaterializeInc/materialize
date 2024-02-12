\set tagClass '\'Philosopher\''
\set country '\'China\''
-- materialize=> \i q03.sql
--    forum.id    |           forum.title           |     forum.creationDate     |   person.id    | messagecount
-- ---------------+---------------------------------+----------------------------+----------------+--------------
--   549755850815 | Group for John_Locke in Beipiao | 2010-11-01 04:46:01.493+00 |  6597069776126 |          248
--   137438968449 | Wall of Bing Yang               | 2010-04-18 05:33:45.691+00 |  2199023260487 |          158
--   274877928840 | Wall of Ge Liu                  | 2010-05-07 00:59:06.84+00  |  4398046512680 |          129
--   824633776514 | Wall of Yang Yang               | 2011-01-13 09:57:24.14+00  | 13194139536784 |          120
--  1236950657048 | Wall of Peng Li                 | 2011-08-13 17:30:33.571+00 | 19791209304449 |          118
--   274877928282 | Wall of Zheng Li                | 2010-06-22 01:40:02.845+00 |  4398046512214 |          112
--   962073726770 | Wall of Ge Wang                 | 2011-03-25 19:48:11.039+00 | 15393162799092 |          109
--   412316896104 | Wall of Chen Zhu                | 2010-08-27 00:05:21.903+00 |  6597069774737 |          102
--   274877927676 | Wall of Yang Zhang              | 2010-06-19 16:17:21.897+00 |  4398046511410 |           95
--   137438972896 | Wall of Wei Wang                | 2010-04-21 01:27:53.705+00 |  2199023265231 |           91
--   824633775563 | Wall of Jun Wang                | 2011-01-27 22:09:42.233+00 | 13194139535015 |           91
--           2919 | Wall of Jun Li                  | 2010-01-20 14:55:29.574+00 |           3183 |           87
--  1236950656750 | Wall of Yang Lei                | 2011-07-26 00:20:46.178+00 | 19791209303940 |           82
--   412316896414 | Wall of Zhong Hu                | 2010-07-21 15:42:31.151+00 |  6597069775058 |           72
--   962072740197 | Wall of Bingyi He               | 2011-04-28 01:06:30.946+00 | 15393162797324 |           72
--  1511829541762 | Wall of Jun Zhang               | 2012-01-03 11:42:24.106+00 | 24189255814889 |           71
--   687194817684 | Wall of Peng Liu                | 2010-12-23 22:54:22.775+00 | 10995116283294 |           68
--  1236950659174 | Wall of Wei Yang                | 2011-08-04 11:25:00.563+00 | 19791209308889 |           66
--  1099511696581 | Wall of Wei Zhu                 | 2011-05-17 00:27:33.368+00 | 17592186047024 |           65
--   137438972267 | Wall of Peng Li                 | 2010-03-23 00:17:08.427+00 |  2199023264745 |           63
-- (20 rows)
--
-- Time: 28700.050 ms (00:28.700)
/* Q3. Popular topics in a country
\set tagClass '\'MusicalArtist\''
\set country '\'Burma\''
 */
SELECT Forum.id                AS "forum.id"
     , Forum.title             AS "forum.title"
     , Forum.creationDate      AS "forum.creationDate"
     , Forum.ModeratorPersonId AS "person.id"
     , count(Message.MessageId) AS messageCount
FROM Message
JOIN Forum
  ON Forum.id = Message.ContainerForumId
JOIN Person AS ModeratorPerson
  ON ModeratorPerson.id = Forum.ModeratorPersonId
JOIN City
  ON City.id = ModeratorPerson.LocationCityId
JOIN Country
  ON Country.id = City.PartOfCountryId
 AND Country.name = :country
WHERE EXISTS (
  SELECT 1
    FROM TagClass
    JOIN Tag
      ON Tag.TypeTagClassId = TagClass.id
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.TagId = Tag.id
   WHERE Message.MessageId = Message_hasTag_Tag.MessageId AND TagClass.name = :tagClass)
GROUP BY Forum.id, Forum.title, Forum.creationDate, Forum.ModeratorPersonId
ORDER BY messageCount DESC, Forum.id
LIMIT 20
;
