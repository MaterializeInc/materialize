\set tagA '\'Diosdado_Macapagal\''
\set dateA '\'2012-10-07\''::timestamp
\set tagB '\'Thailand_Noriega\''
\set dateB '\'2012-12-14\''::timestamp
\set maxKnowsLimit '5'
-- materialize=> \i q16.sql
--  personid | messagecounta | messagecountb
-- ----------+---------------+---------------
-- (0 rows)
--
-- Time: 14005.429 ms (00:14.005)

-- apparently no results is common... searching a few other parameters still turned up no results

/* Q16. Fake news detection
\set tagA '\'Augustine_of_Hippo\''
\set dateA '\'2011-10-10\''::timestamp
\set tagB '\'Manuel_Noriega\''
\set dateB '\'2012-06-02\''::timestamp
\set maxKnowsLimit '5'
 */
WITH
  subgraphA AS (
    SELECT DISTINCT Person.id AS PersonId, Message.MessageId AS MessageId
    FROM Person
    JOIN Message
      ON Message.CreatorPersonId = Person.id
     AND Message.creationDate::date = :dateA
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.MessageId = Message.MessageId
    JOIN Tag
      ON Tag.id = Message_hasTag_Tag.TagId
     AND Tag.name = :tagA
  ),
  personA AS (
    SELECT
        subgraphA1.PersonId,
        count(DISTINCT subgraphA1.MessageId) AS cm,
        count(DISTINCT Person_knows_Person.Person2Id) AS cp2
    FROM subgraphA subgraphA1
    LEFT JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = subgraphA1.PersonId
    AND Person_knows_Person.Person2Id IN (SELECT PersonId FROM subgraphA)
    GROUP BY subgraphA1.PersonId
    HAVING count(DISTINCT Person_knows_Person.Person2Id) <= :maxKnowsLimit
    ORDER BY subgraphA1.PersonId ASC
  ),
  subgraphB AS (
    SELECT DISTINCT Person.id AS PersonId, Message.MessageId AS MessageId
    FROM Person
    JOIN Message
      ON Message.CreatorPersonId = Person.id
     AND Message.creationDate::date = :dateB
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.MessageId = Message.MessageId
    JOIN Tag
      ON Tag.id = Message_hasTag_Tag.TagId
     AND Tag.name = :tagB
  ),
  personB AS (
    SELECT
        subgraphB1.PersonId,
        count(DISTINCT subgraphB1.MessageId) AS cm,
        count(DISTINCT Person_knows_Person.Person2Id) AS cp2
    FROM subgraphB subgraphB1
    LEFT JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = subgraphB1.PersonId
    AND Person_knows_Person.Person2Id IN (SELECT PersonId FROM subgraphB)
    GROUP BY subgraphB1.PersonId
    HAVING count(DISTINCT Person_knows_Person.Person2Id) <= :maxKnowsLimit
    ORDER BY subgraphB1.PersonId ASC
  )
SELECT
    personA.PersonId AS PersonId,
    personA.cm AS messageCountA,
    personB.cm AS messageCountB
FROM personA
JOIN personB
  ON personB.PersonId = personA.PersonId
ORDER BY personA.cm + personB.cm DESC, PersonId ASC
LIMIT 20
;
