\set country1 '\'Philippines\''
\set country2 '\'Taiwan\''
-- materialize=> \i q14.sql
--    person1.id   |   person2.id   |   city1.name   | score
-- ----------------+----------------+----------------+-------
--   2199023266161 |  2199023259897 | Cavite_City    |    16
--   4398046514165 | 30786325580485 | Makati         |    16
--   4398046516434 |  2199023259897 | Ermita         |    16
--   4398046516776 | 24189255821690 | Sampaloc       |    16
--   4398046517769 | 30786325580485 | Mandaluyong    |    16
--   4398046521222 |  2199023259897 | Malate         |    16
--   4398046521507 | 19791209308287 | Naga           |    16
--   6597069768162 |           8001 | Kalibo         |    16
--   6597069775480 |  6597069769061 | Pasay          |    16
--   6597069775831 |  2199023259897 | Santa_Rosa     |    16
--   8796093022752 |           8001 | Intramuros     |    16
--   8796093030118 | 19791209308287 | San_Miguel     |    16
--  10995116284563 |  4398046513747 | Davao_City     |    16
--  15393162793426 |  4398046513747 | Angeles        |    16
--  17592186046583 | 10995116279524 | Iligan         |    16
--  17592186047071 | 19791209308287 | Malolos        |    16
--  17592186053366 | 17592186050725 | Los_Baños      |    16
--  17592186053760 |  2199023258965 | Zamboanga_City |    16
--  19791209300166 | 13194139534446 | Cabanatuan     |    16
--  19791209304999 | 13194139534446 | Laoag          |    16
--  19791209309287 |           6456 | Biñan          |    16
--  28587302324919 |  2199023259897 | Manila         |    16
--  30786325587555 | 17592186044852 | Bacolod        |    16
--   4398046521694 | 13194139534446 | Lipa           |    15
--   8796093031783 | 10995116280964 | Tagbilaran     |    15
--  21990232557022 |           6456 | Legazpi        |    15
--  21990232561626 |  6597069769061 | Quezon_City    |    15
-- (27 rows)
--
-- Time: 16166.126 ms (00:16.166)

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
