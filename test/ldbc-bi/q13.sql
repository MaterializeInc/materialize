\set country '\'India\''
\set endDate '\'2012-11-09\''::timestamp
-- materialize=> \i q13.sql
-- ^CCancel request sent
-- psql:q13.sql:39: ERROR:  canceling statement due to user request
-- Time: 432178.513 ms (07:12.179)

/* Q13. Zombies in a country
\set country '\'France\''
\set endDate '\'2013-01-01\''::timestamp
 */
WITH Zombies AS (
    SELECT Person.id AS zombieid
      FROM Country
      JOIN City
        ON City.PartOfCountryId = Country.id
      JOIN Person
        ON Person.LocationCityId = City.id
      LEFT JOIN Message
         ON Person.id = Message.CreatorPersonId
        AND Message.creationDate BETWEEN Person.creationDate AND :endDate -- the lower bound is an optmization to prune messages
     WHERE Country.name = :country
       AND Person.creationDate < :endDate
     GROUP BY Person.id, Person.creationDate
        -- average of [0, 1) messages per month is equivalent with having less messages than the month span between person creationDate and parameter :endDate
    HAVING count(Message.MessageId) < 12*extract(YEAR FROM :endDate) + extract(MONTH FROM :endDate)
                            - (12*extract(YEAR FROM Person.creationDate) + extract(MONTH FROM Person.creationDate))
                            + 1
)
SELECT Z.zombieid AS "zombie.id"
     , coalesce(t.zombieLikeCount, 0) AS zombieLikeCount
     , coalesce(t.totalLikeCount, 0) AS totalLikeCount
     , CASE WHEN t.totalLikeCount > 0 THEN t.zombieLikeCount::float/t.totalLikeCount ELSE 0 END AS zombieScore
  FROM Zombies Z LEFT JOIN (
    SELECT Z.zombieid, count(*) as totalLikeCount, sum(case when exists (SELECT 1 FROM Zombies ZL WHERE ZL.zombieid = p.id) then 1 else 0 end) AS zombieLikeCount
    FROM Person p, Person_likes_Message plm, Message m, Zombies Z
    WHERE Z.zombieid = m.CreatorPersonId AND p.creationDate < :endDate
        AND p.id = plm.PersonId AND m.MessageId = plm.MessageId
    GROUP BY Z.zombieid
  ) t ON (Z.zombieid = t.zombieid)
 ORDER BY zombieScore DESC, Z.zombieid
 LIMIT 100
;
