\set tag '\'Abbas_I_of_Persia\''
\set startDate '\'2010-06-14\''::timestamp
\set endDate '\'2010-06-28\''::timestamp
-- materialize=> \i q08.sql
--    person.id    | score | friendsscore
-- ----------------+-------+--------------
--  15393162796516 |   100 |         2304
--  10995116284819 |   100 |         2202
--   4398046514439 |   101 |         2102
--   8796093028727 |   100 |         2101
--  28587302329901 |   100 |         2001
--  15393162797781 |   100 |         1902
--   8796093029396 |   100 |         1901
--   6597069771580 |   100 |         1900
--  15393162796938 |   100 |         1803
--  10995116287504 |   100 |         1801
--   8796093027051 |   100 |         1800
--  21990232558345 |   100 |         1703
--  17592186051103 |   100 |         1601
--  19791209301605 |   100 |         1600
--  28587302324075 |   100 |         1600
--   2199023255976 |   102 |         1501
--  24189255814035 |   100 |         1500
--   8796093024055 |   100 |         1402
--  32985348842801 |   100 |         1402
--  30786325582257 |   100 |         1401
--  21990232560968 |   100 |         1400
--  19791209301779 |   100 |         1301
--  28587302331208 |   100 |         1200
--  30786325582458 |   100 |         1200
--   2199023260693 |   100 |         1103
--  30786325586956 |   100 |         1101
--  28587302330254 |   100 |         1100
--   6597069769336 |   100 |         1003
--  21990232556992 |   100 |         1000
--  21990232564593 |   100 |         1000
--  24189255814629 |   100 |         1000
--  26388279073300 |   100 |         1000
--  10995116281487 |   100 |          902
--  26388279074645 |   100 |          901
--  28587302330225 |   100 |          900
--  32985348842020 |   100 |          900
--  30786325582484 |   100 |          802
--            9104 |   100 |          703
--   4398046514572 |   100 |          703
--  30786325587534 |   100 |          701
--  13194139541181 |   100 |          700
--  15393162794315 |   100 |          600
--  32985348842546 |   100 |          502
--  21990232564402 |   100 |          500
--   2199023257012 |   100 |          404
--  28587302328411 |   100 |          400
--  32985348843709 |   100 |          400
--   4398046511722 |   102 |          300
--  13194139541245 |   100 |          302
--  15393162797039 |   100 |          201
--  35184372096631 |   100 |          200
--  37383395346899 |   100 |          200
--            8698 |   101 |          100
--  24189255811121 |   100 |          101
--  28587302329837 |   100 |          101
--            4091 |   100 |          100
--   4398046517503 |   100 |          100
--  13194139537876 |   100 |          100
--  30786325582666 |   100 |          100
--  32985348841739 |   100 |          100
--  35184372095947 |   100 |          100
--            5671 |   101 |            0
--            9475 |   100 |            0
--   2199023255573 |   100 |            0
--   2199023258115 |   100 |            0
--   2199023258475 |   100 |            0
--   2199023259981 |   100 |            0
--   2199023261409 |   100 |            0
--   2199023263186 |   100 |            0
--   2199023265455 |   100 |            0
--   4398046519791 |   100 |            0
--   4398046519912 |   100 |            0
--   4398046519918 |   100 |            0
--   4398046519949 |   100 |            0
--   6597069769432 |   100 |            0
--   6597069770566 |   100 |            0
--  13194139543653 |   100 |            0
--  15393162795159 |   100 |            0
--  15393162798002 |   100 |            0
--  21990232558087 |   100 |            0
--  21990232559362 |   100 |            0
--  24189255815070 |   100 |            0
--  26388279069740 |   100 |            0
--  26388279074274 |   100 |            0
--  28587302331312 |   100 |            0
--  30786325584433 |   100 |            0
--  32985348836171 |   100 |            0
--  32985348839448 |   100 |            0
--  35184372097346 |   100 |            0
--  37383395348360 |   100 |            0
--  37383395353132 |   100 |            0
-- (91 rows)
--
-- Time: 10117.007 ms (00:10.117)

/* Q8. Central person for a tag
\set tag '\'Che_Guevara\''
\set startDate '\'2011-07-20\''::timestamp
\set endDate '\'2011-07-25\''::timestamp
 */
WITH Person_interested_in_Tag AS (
    SELECT Person.id AS PersonId
      FROM Person
      JOIN Person_hasInterest_Tag
        ON Person_hasInterest_Tag.PersonId = Person.id
      JOIN Tag
        ON Tag.id = Person_hasInterest_Tag.TagId
       AND Tag.name = :tag
)
   , Person_Message_score AS (
    SELECT Person.id AS PersonId
         , count(*) AS message_score
      FROM Tag
      JOIN Message_hasTag_Tag
        ON Message_hasTag_Tag.TagId = Tag.id
      JOIN Message
        ON Message_hasTag_Tag.MessageId = Message.MessageId
       AND :startDate < Message.creationDate
      JOIN Person
        ON Person.id = Message.CreatorPersonId
     WHERE Tag.name = :tag
       AND Message.creationDate < :endDate
     GROUP BY Person.id
)
   , Person_score AS (
    SELECT coalesce(Person_interested_in_Tag.PersonId, pms.PersonId) AS PersonId
         , CASE WHEN Person_interested_in_Tag.PersonId IS NULL then 0 ELSE 100 END -- scored from interest in the given tag
         + coalesce(pms.message_score, 0) AS score
      FROM Person_interested_in_Tag
           FULL JOIN Person_Message_score pms
                  ON Person_interested_in_Tag.PersonId = pms.PersonId
)
SELECT p.PersonId AS "person.id"
     , p.score AS score
     , coalesce(sum(f.score), 0) AS friendsScore
  FROM Person_score p
  LEFT JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = p.PersonId
  LEFT JOIN Person_score f -- the friend
    ON f.PersonId = Person_knows_Person.Person2Id
 GROUP BY p.PersonId, p.score
 ORDER BY p.score + coalesce(sum(f.score), 0) DESC, p.PersonId
 LIMIT 100
;
