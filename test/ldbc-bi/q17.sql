\set tag '\'Cosmic_Egg\''
\set delta '12'
-- materialize=> \i q17.sql
--  person1.id | messagecount
-- ------------+--------------
-- (0 rows)
--
-- Time: 23811.382 ms (00:23.811)

-- spooked by empty results, tried it for a more popular tag:
--
-- \set tag '\'Wolfgang_Amadeus_Mozart\''
-- \set delta '12'
-- Time: 1.440 ms
-- materialize=> \i q17.sql
--    person1.id   | messagecount
-- ----------------+--------------
--  19791209302816 |           67
--   2199023256087 |           50
--  19791209301614 |           43
--  13194139537074 |           42
--  17592186048228 |           42
--  13194139540244 |           40
--   2199023258027 |           39
--   6597069767000 |           37
--   8796093031814 |           36
--            3819 |           35
-- (10 rows)
--
-- Time: 30821.626 ms (00:30.822)

/* Q17. Information propagation analysis
\set tag '\'Slavoj_Žižek\''
\set delta '4'
 */
WITH MyMessage as (
    SELECT *
    FROM Message
-- (tag)<-[:HAS_TAG]-(message)
    WHERE MessageId in (SELECT MessageId FROM Message_hasTag_Tag WHERE TagId IN (SELECT id FROM Tag WHERE Tag.name = :tag))
)
-- (message1)-[:HAS_CREATOR]->(person1)
SELECT Message1.CreatorPersonId AS "person1.id", count(DISTINCT Message2.MessageId) AS messageCount
FROM MyMessage Message1
-- (message2 <date filtering>})
JOIN MyMessage Message2
 ON (Message1.creationDate + (:delta || ' hour')::interval) < Message2.creationDate
JOIN MyMessage Comment
 ON Comment.ParentMessageId = Message2.MessageId
-- (forum1)-[:Has_MEMBER]->(person2)
JOIN Forum_hasMember_Person Forum_hasMember_Person2
  ON Forum_hasMember_Person2.ForumId = Message1.ContainerForumId -- forum1
 AND Forum_hasMember_Person2.PersonId = Comment.CreatorPersonId -- person2
-- (forum1)-[:Has_MEMBER]->(person3)
JOIN Forum_hasMember_Person Forum_hasMember_Person3
  ON Forum_hasMember_Person3.ForumId = Message1.ContainerForumId -- forum1
 AND Forum_hasMember_Person3.PersonId = Message2.CreatorPersonId -- person3
WHERE Message1.ContainerForumId <> Message2.ContainerForumId
  -- person2 <> person3
  AND Forum_hasMember_Person2.PersonId <> Forum_hasMember_Person3.PersonId
  -- NOT (forum2)-[:HAS_MEMBER]->(person1)
  AND NOT EXISTS (SELECT 1
                  FROM Forum_hasMember_Person Forum_hasMember_Person1
                  WHERE Forum_hasMember_Person1.ForumId = Message2.ContainerForumId -- forum2
                    AND Forum_hasMember_Person1.PersonId = Message1.CreatorPersonId -- person1
                 )
GROUP BY Message1.CreatorPersonId
ORDER BY messageCount DESC, Message1.CreatorPersonId ASC
LIMIT 10
;
