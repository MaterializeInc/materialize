\set country '\'India\''
\set endDate '\'2012-11-09\''::timestamp
-- materialize=> \i q13.sql
--    zombie.id    | zombielikecount | totallikecount |     zombiescore
-- ----------------+-----------------+----------------+----------------------
--   4398046515583 |               1 |             28 |  0.03571428571428571
--  15393162797343 |               1 |             32 |              0.03125
--   6597069766996 |               1 |             59 |  0.01694915254237288
--  28587302331214 |               1 |             70 | 0.014285714285714285
--  13194139540014 |               4 |            349 | 0.011461318051575931
--   6597069773020 |               1 |            209 | 0.004784688995215311
--            1032 |               0 |              0 |                    0
--            2238 |               0 |              0 |                    0
--            3302 |               0 |              1 |                    0
--            6353 |               0 |              0 |                    0
--            9542 |               0 |             10 |                    0
--   2199023255606 |               0 |              0 |                    0
--   2199023257681 |               0 |              2 |                    0
--   2199023259422 |               0 |              1 |                    0
--   2199023260507 |               0 |              0 |                    0
--   2199023260849 |               0 |              0 |                    0
--   2199023261845 |               0 |             30 |                    0
--   2199023263793 |               0 |              0 |                    0
--   2199023264195 |               0 |              0 |                    0
--   4398046511273 |               0 |              1 |                    0
--   4398046521177 |               0 |              0 |                    0
--   6597069768899 |               0 |              0 |                    0
--   6597069770651 |               0 |              1 |                    0
--   6597069771254 |               0 |              0 |                    0
--   6597069771658 |               0 |              0 |                    0
--   6597069775228 |               0 |              0 |                    0
--   8796093023336 |               0 |              0 |                    0
--   8796093029248 |               0 |              2 |                    0
--   8796093029298 |               0 |              0 |                    0
--   8796093029959 |               0 |              0 |                    0
--   8796093031385 |               0 |              0 |                    0
--   8796093032614 |               0 |              0 |                    0
--  10995116278425 |               0 |              1 |                    0
--  10995116279003 |               0 |              2 |                    0
--  10995116279401 |               0 |              1 |                    0
--  10995116284530 |               0 |              0 |                    0
--  10995116284647 |               0 |              0 |                    0
--  13194139534872 |               0 |              0 |                    0
--  13194139537381 |               0 |              0 |                    0
--  13194139541490 |               0 |              0 |                    0
--  13194139542083 |               0 |             21 |                    0
--  15393162789626 |               0 |              1 |                    0
--  15393162790005 |               0 |              0 |                    0
--  15393162790303 |               0 |              1 |                    0
--  15393162791561 |               0 |              0 |                    0
--  15393162795607 |               0 |              0 |                    0
--  15393162796167 |               0 |              0 |                    0
--  15393162797457 |               0 |              0 |                    0
--  15393162797711 |               0 |              0 |                    0
--  15393162798239 |               0 |              0 |                    0
--  17592186045794 |               0 |              0 |                    0
--  17592186050665 |               0 |              0 |                    0
--  17592186051978 |               0 |             79 |                    0
--  19791209302580 |               0 |              0 |                    0
--  19791209310126 |               0 |              0 |                    0
--  19791209310408 |               0 |              0 |                    0
--  21990232558127 |               0 |              0 |                    0
--  21990232560358 |               0 |              0 |                    0
--  21990232561810 |               0 |              0 |                    0
--  21990232563609 |               0 |              0 |                    0
--  24189255811996 |               0 |              0 |                    0
--  24189255812829 |               0 |              0 |                    0
--  24189255812892 |               0 |              0 |                    0
--  24189255813751 |               0 |              0 |                    0
--  24189255819454 |               0 |              0 |                    0
--  24189255820910 |               0 |              0 |                    0
--  26388279066976 |               0 |              0 |                    0
--  26388279074008 |               0 |              0 |                    0
--  26388279075800 |               0 |              0 |                    0
--  28587302322826 |               0 |              0 |                    0
--  28587302323140 |               0 |              0 |                    0
--  28587302323831 |               0 |              0 |                    0
--  28587302332315 |               0 |              0 |                    0
--  28587302332388 |               0 |              0 |                    0
--  30786325580674 |               0 |              0 |                    0
--  30786325582071 |               0 |              0 |                    0
--  32985348836171 |               0 |              0 |                    0
--  32985348840421 |               0 |              0 |                    0
--  35184372090806 |               0 |              0 |                    0
--  35184372091151 |               0 |              0 |                    0
--  35184372093248 |               0 |              0 |                    0
--  35184372095848 |               0 |              0 |                    0
--  35184372096349 |               0 |              0 |                    0
--  35184372097346 |               0 |              0 |                    0
--  35184372097978 |               0 |              1 |                    0
--  35184372098113 |               0 |              0 |                    0
--  35184372098845 |               0 |              0 |                    0
--  35184372098880 |               0 |              0 |                    0
-- (88 rows)
--
-- Time: 17709.626 ms (00:17.710)

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
