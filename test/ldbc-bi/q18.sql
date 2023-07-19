\set tag '\'Fyodor_Dostoyevsky\''
-- materialize=> \i q18.sql
-- ^CCancel request sent
-- psql:q18.sql:31: ERROR:  canceling statement due to user request
-- Time: 450409.218 ms (07:30.409)

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
