\set country1 '\'Philippines\''
\set country2 '\'Taiwan\''
-- materialize=> \i q14.sql
-- ^CCancel request sent
-- psql:q14.sql:65: ERROR:  canceling statement due to user request
-- Time: 801461.051 ms (13:21.461)

/* Q14. International dialog
\set country1 '\'Chile\''
\set country2 '\'Argentina\''
 */
WITH PersonPairCandidates AS (
    SELECT Person1.id AS Person1Id
         , Person2.id AS Person2Id
         , City1.id AS cityId
         , City1.name AS cityName
      FROM Country Country1
      JOIN City City1
        ON City1.PartOfCountryId = Country1.id
      JOIN Person Person1
        ON Person1.LocationCityId = City1.id
      JOIN Person_knows_Person
        ON Person_knows_Person.Person1Id = Person1.id
      JOIN Person Person2
        ON Person2.id = Person_knows_Person.Person2Id
      JOIN City City2
        ON Person2.LocationCityId = City2.id
      JOIN Country Country2
        ON Country2.id = City2.PartOfCountryId
     WHERE Country1.name = :country1
       AND Country2.name = :country2
)
,  PPC(Person1Id, Person2Id, Flipped) AS (
   SELECT Person1Id AS Person1Id, Person2Id AS Person2Id, false AS Flipped FROM PersonPairCandidates
   UNION ALL
   SELECT Person2Id AS Person1Id, Person1Id AS Person2Id, true As Flipped FROM PersonPairCandidates
)
,  pair_scores AS (
    SELECT CASE WHEN Flipped THEN Person2Id ELSE Person1Id END AS Person1Id,
           CASE WHEN Flipped THEN Person1Id ELSE Person2Id END AS Person2Id,
        (
        CASE WHEN EXISTS (SELECT 1 FROM Message m, Message r WHERE m.MessageId = r.ParentMessageId AND Person1Id = r.CreatorPersonId AND Person2Id = m.CreatorPersonId AND EXISTS (SELECT 1 FROM PPC x WHERE x.Person1Id = r.CreatorPersonId)) THEN (CASE WHEN Flipped THEN 1 ELSE 4 END) ELSE 0 END +
        CASE WHEN EXISTS (SELECT 1 FROM Message m, Person_likes_Message l WHERE Person2Id = m.CreatorPersonId AND m.MessageId = l.MessageId AND l.PersonId = Person1Id AND EXISTS (SELECT 1 FROM PPC x WHERE x.Person1Id = l.PersonId)) THEN (CASE WHEN Flipped THEN 1 ELSE 10 END) ELSE 0 END
        ) as score
      FROM PPC
)
,  pair_scoresX AS (
    SELECT Person1Id, Person2Id, sum(score) as score
      FROM pair_scores
      GROUP BY Person1Id, Person2Id
)
,  score_ranks AS (
    SELECT DISTINCT ON (cityId)
         PersonPairCandidates.Person1Id, PersonPairCandidates.Person2Id, cityId, cityName
         , s.score AS score
      FROM PersonPairCandidates
      LEFT JOIN pair_scoresX s
             ON s.Person1Id = PersonPairCandidates.Person1Id
            AND s.person2Id = PersonPairCandidates.Person2Id
      ORDER BY cityId, s.score DESC, PersonPairCandidates.Person1Id, PersonPairCandidates.Person2Id
)
SELECT score_ranks.Person1Id AS "person1.id"
     , score_ranks.Person2Id AS "person2.id"
     , score_ranks.cityName AS "city1.name"
     , score_ranks.score
  FROM score_ranks
 ORDER BY score_ranks.score DESC, score_ranks.Person1Id, score_ranks.Person2Id
 LIMIT 100
;
