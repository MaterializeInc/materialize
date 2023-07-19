\set date '\'2010-02-12\''::timestamp
-- materialize=> \i q04.sql
-- CREATE MATERIALIZED VIEW
-- Time: 544.434 ms
-- ^CCancel request sent
-- psql:q04.sql:56: ERROR:  canceling statement due to user request
-- Time: 8625309.080 ms (02:23:45.309)

/* Q4. Top message creators by country
\set date '\'2012-09-01\''::timestamp
 */

CREATE OR REPLACE MATERIALIZED VIEW Top100PopularForumsQ04 AS
  SELECT
    T.id AS id,
    Forum.creationdate AS creationDate,
    T.maxNumberOfMembers AS maxNumberOfMembers
  FROM (SELECT
          ForumId AS id,
	  max(numberOfMembers) AS maxNumberOfMembers
        FROM (SELECT
	        Forum_hasMember_Person.ForumId AS ForumId,
		count(Person.id) AS numberOfMembers,
		City.PartOfCountryId AS CountryId
              FROM Forum_hasMember_Person
              JOIN Person
              ON Person.id = Forum_hasMember_Person.PersonId
              JOIN City
              ON City.id = Person.LocationCityId
              GROUP BY City.PartOfCountryId, Forum_hasMember_Person.ForumId)
	      ForumMembershipPerCountry
  GROUP BY ForumId) T, Forum
  WHERE T.id = Forum.id;
CREATE INDEX Top100PopularForumsQ04_id ON Top100PopularForumsQ04 (id);

WITH Top100_Popular_Forums AS (
  SELECT id, creationDate, maxNumberOfMembers
  FROM Top100PopularForumsQ04
  WHERE creationDate > :date
  ORDER BY maxNumberOfMembers DESC, id
  LIMIT 100
)
SELECT au.id AS "person.id"
     , au.firstName AS "person.firstName"
     , au.lastName AS "person.lastName"
     , au.creationDate
     -- a single person might be member of more than 1 of the top100 forums, so their messages should be DISTINCT counted
     , count(Message.MessageId) AS messageCount
  FROM
       Person au
       LEFT JOIN Message
              ON au.id = Message.CreatorPersonId
             AND Message.ContainerForumId IN (SELECT id FROM Top100_Popular_Forums)
  WHERE EXISTS (SELECT 1
                FROM Top100_Popular_Forums
                INNER JOIN Forum_hasMember_Person
                        ON Forum_hasMember_Person.ForumId = Top100_Popular_Forums.id
                WHERE Forum_hasMember_Person.PersonId = au.id
               )
GROUP BY au.id, au.firstName, au.lastName, au.creationDate
ORDER BY messageCount DESC, au.id
LIMIT 100
;
