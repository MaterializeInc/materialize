\set tag '\'Fyodor_Dostoyevsky\''
-- materialize=> \i q18.sql
--    person1.id   |   person2.id   | mutualfriendcount
-- ----------------+----------------+-------------------
--   2199023256539 | 24189255821523 |                31
--  24189255821523 |  2199023256539 |                31
--  15393162796970 | 24189255820514 |                25
--  24189255820514 | 15393162796970 |                25
--   4398046511410 | 13194139543082 |                23
--  10995116284091 | 24189255815677 |                23
--  13194139543082 |  4398046511410 |                23
--  24189255815677 | 10995116284091 |                23
--  24189255819695 | 24189255821523 |                20
--  24189255821523 | 24189255819695 |                20
--  24189255820514 | 24189255821523 |                18
--  24189255821523 | 24189255820514 |                18
--   2199023256539 | 15393162796970 |                17
--   2199023256539 | 24189255819695 |                17
--  13194139536963 | 13194139543082 |                17
--  13194139543082 | 13194139536963 |                17
--  15393162796970 |  2199023256539 |                17
--  17592186050431 | 26388279069225 |                17
--  24189255819695 |  2199023256539 |                17
--  26388279069225 | 17592186050431 |                17
-- (20 rows)
--
-- Time: 2097.830 ms (00:02.098)

/* Q18. Friend recommendation
\set tag '\'Frank_Sinatra\''
 */
WITH
PersonWithInterest AS (
SELECT pt.PersonId AS PersonId
FROM Person_hasInterest_Tag pt, Tag t
WHERE t.name = :tag AND pt.TagId = t.id
),
FriendsOfInterested AS (
SELECT k.Person1Id AS InterestedId, k.Person2Id AS FriendId
FROM PersonWithInterest p, Person_knows_Person k
WHERE p.PersonId = k.Person1Id
)
SELECT k1.InterestedId AS "person1.id", k2.InterestedId AS "person2.id", count(k1.FriendId) AS mutualFriendCount
FROM FriendsOfInterested k1
JOIN FriendsOfInterested k2
  ON k1.FriendId = k2.FriendId -- pattern: mutualFriend
 -- negative edge
WHERE k1.InterestedId != k2.InterestedId
  AND NOT EXISTS (SELECT 1
         FROM Person_knows_Person k3
        WHERE k3.Person1Id = k2.InterestedId -- pattern: person2
          AND k3.Person2Id = k1.InterestedId -- pattern: person1
      )
GROUP BY k1.InterestedId, k2.InterestedId
ORDER BY mutualFriendCount DESC, k1.InterestedId ASC, k2.InterestedId ASC
LIMIT 20
;
