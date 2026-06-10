\set date '\'2010-02-12\''::timestamp
-- materialize=> \i q04.sql
-- CREATE MATERIALIZED VIEW
-- Time: 595.780 ms
-- CREATE INDEX
-- Time: 523.990 ms
--    person.id    | person.firstName | person.lastName |        creationdate        | messagecount
-- ----------------+------------------+-----------------+----------------------------+--------------
--   4398046519825 | Ahmad Rafiq      | Akbar           | 2010-05-23 13:24:43.393+00 |         1195
--   6597069774064 | Ayesha           | Ahmed           | 2010-08-25 11:58:50.853+00 |          948
--  15393162797132 | Shweta           | Kapoor          | 2011-04-17 02:52:48.739+00 |          835
--   2199023265231 | Wei              | Wang            | 2010-04-21 01:27:43.705+00 |          738
--  24189255819865 | Sombat           | Amarttayakul    | 2011-11-12 21:04:25.195+00 |          435
--   2199023260815 | Chen             | Li              | 2010-04-27 14:01:48.888+00 |          402
--  10995116284819 | John             | Khan            | 2010-12-24 17:41:09.731+00 |          382
--            2866 | Eve-Mary Thai    | Hoang           | 2010-02-05 03:07:01.196+00 |          345
--   6597069770739 | Abdul Haris      | Gallagher       | 2010-08-29 01:13:55.834+00 |          292
--  28587302329901 | Shweta           | Khan            | 2012-03-30 09:33:32.228+00 |          261
--  15393162799092 | Ge               | Wang            | 2011-03-25 19:48:01.039+00 |          151
--  32985348840714 | John             | Singh           | 2012-07-23 00:06:08.096+00 |           64
--  10995116279311 | Abhishek         | Kumar           | 2010-12-04 19:27:01.714+00 |           54
--  28587302327043 | A.               | Kumar           | 2012-04-19 07:03:33.413+00 |           48
--  32985348841684 | Jie              | Zhu             | 2012-09-02 10:37:26.12+00  |           48
--   4398046518418 | Yang             | Li              | 2010-05-23 19:28:52.655+00 |           46
--  10995116285596 | Taufik           | Budjana         | 2010-12-25 15:16:21.391+00 |           46
--  26388279068970 | Amit             | Reddy           | 2012-02-08 20:18:06.166+00 |           46
--   8796093028172 | Karan            | Khan            | 2010-10-10 03:24:24.86+00  |           44
--  28587302328636 | Anh              | Ha              | 2012-04-12 19:20:15.578+00 |           44
--  28587302329156 | A.               | Singh           | 2012-04-24 09:01:46.435+00 |           44
--   6597069775034 | Abdul Jamil      | Qureshi         | 2010-08-26 20:49:21.329+00 |           43
--  19791209304987 | John             | Singh           | 2011-08-09 05:18:25.45+00  |           43
--  26388279071390 | Rahul            | Khan            | 2012-01-21 02:24:23.843+00 |           43
--  32985348843490 | Yang             | Li              | 2012-07-12 22:09:43.58+00  |           42
--            7937 | Rahul            | Kumar           | 2010-01-23 14:07:41.044+00 |           41
--   2199023261398 | Nguyen Huu       | Nguyen          | 2010-03-07 00:09:13.471+00 |           41
--  26388279074760 | A.               | Nair            | 2012-02-22 23:18:47.392+00 |           41
--   6597069768174 | Jun              | He              | 2010-08-06 22:04:49.717+00 |           40
--  24189255820986 | Bichang          | Zhang           | 2011-11-17 20:02:46.129+00 |           38
--  13194139541713 | Deepak           | Singh           | 2011-01-23 15:36:38.709+00 |           37
--  17592186048064 | V.               | Joshi           | 2011-05-13 06:34:40.153+00 |           37
--  21990232556039 | Naresh           | Kumar           | 2011-10-26 19:55:13.777+00 |           37
--  21990232559111 | Karan            | Sharma          | 2011-10-12 23:23:49.411+00 |           37
--  30786325582458 | R.               | Kapoor          | 2012-07-05 05:01:47.042+00 |           37
--   4398046512214 | Zheng            | Li              | 2010-06-22 01:39:52.845+00 |           36
--   6597069773793 | Jun              | Wang            | 2010-07-17 12:38:41.01+00  |           36
--  15393162788923 | Rahul            | Sharma          | 2011-03-10 15:49:01.365+00 |           36
--  17592186054998 | Manuel           | Cosio           | 2011-05-10 02:03:12.983+00 |           36
--  32985348841739 | Anupam           | Rao             | 2012-08-24 03:56:50.79+00  |           36
--            2208 | Shweta           | Sharma          | 2010-01-16 16:40:35.139+00 |           35
--   2199023263560 | Wuttichai        | Boy             | 2010-03-28 18:55:23.334+00 |           35
--   6597069768442 | Abhishek         | Kapoor          | 2010-09-01 18:11:31.748+00 |           35
--  17592186051684 | Abhishek         | Singh           | 2011-07-02 11:05:24.354+00 |           35
--  19791209309552 | Abdul Haris      | Anwar           | 2011-07-27 16:59:54.637+00 |           35
--  26388279073421 | Mirza Kalich     | Ahmed           | 2012-02-16 05:07:07.084+00 |           35
--  28587302327725 | Aama             | Shrestha        | 2012-04-09 00:22:34.226+00 |           35
--  30786325580960 | Duleep           | Banda           | 2012-05-15 15:04:55.025+00 |           35
--  30786325581241 | Jean             | Arnaud          | 2012-05-21 14:14:11.353+00 |           35
--  15393162798428 | Chen             | Zhang           | 2011-03-12 01:23:21.165+00 |           34
--  17592186051872 | John             | Sharma          | 2011-06-11 07:51:02.867+00 |           34
--  28587302329829 | Oleg             | Chezhina        | 2012-05-02 11:31:39.054+00 |           34
--            5452 | Agustiar         | Balawan         | 2010-01-04 19:14:27.142+00 |           33
--   2199023264344 | R.               | Kapoor          | 2010-04-30 05:24:51.797+00 |           33
--   8796093023211 | Amir             | Chen            | 2010-10-31 01:03:17.379+00 |           33
--  13194139542917 | Lei              | Li              | 2011-01-15 02:09:59.71+00  |           33
--  19791209305377 | John             | Singh           | 2011-07-31 15:48:27.794+00 |           33
--  19791209306643 | John             | Sen             | 2011-07-27 17:48:01.487+00 |           33
--  35184372090954 | Arun             | Chopra          | 2012-10-25 02:12:07.425+00 |           33
--   6597069771115 | Ami              | Chatterjee      | 2010-08-30 16:00:52.716+00 |           32
--   6597069774644 | Zhang            | Chen            | 2010-08-02 13:39:20.452+00 |           32
--   6597069777069 | Wei              | Wang            | 2010-08-06 01:42:36.282+00 |           32
--  13194139543607 | A.               | Khan            | 2011-02-06 23:23:19.378+00 |           32
--  21990232559935 | Meera            | Kapoor          | 2011-09-23 11:55:58.941+00 |           32
--  21990232565605 | Prakash          | Price           | 2011-11-06 07:17:04.122+00 |           32
--  26388279076723 | Deepak           | Kumar           | 2012-02-10 18:26:21.335+00 |           32
--  28587302331910 | Isabel           | Reyes           | 2012-04-27 03:16:45.436+00 |           32
--  30786325588075 | Anupam           | Rao             | 2012-06-23 10:16:17.317+00 |           32
--  32985348843444 | Wei              | Chen            | 2012-07-30 09:52:43.39+00  |           32
--           10281 | James            | Raghu           | 2010-01-13 12:20:03.403+00 |           31
--   6597069768018 | Rahul            | Singh           | 2010-07-24 22:08:17.051+00 |           31
--  13194139534180 | Arjun            | Khan            | 2011-01-27 17:05:31.134+00 |           31
--  13194139539554 | K.               | Sharma          | 2011-01-17 09:20:04.865+00 |           31
--  13194139543101 | Shweta           | Kapoor          | 2011-02-09 09:48:05.32+00  |           31
--  19791209302082 | K.               | Kumar           | 2011-09-01 10:14:27.166+00 |           31
--  19791209304789 | Amit             | Rao             | 2011-08-13 06:55:48.125+00 |           31
--  26388279071096 | Prakash          | Khan            | 2012-02-01 00:18:37.691+00 |           31
--  30786325582257 | Ashok            | Sharma          | 2012-05-12 07:08:40.547+00 |           31
--            2435 | Bibit            | Balawan         | 2010-01-23 23:37:20.444+00 |           30
--   2199023261346 | Peng             | Li              | 2010-04-28 01:12:38.381+00 |           30
--   4398046517801 | Jun              | Wang            | 2010-06-01 17:59:47.303+00 |           30
--   8796093029212 | Jie              | Zhang           | 2010-11-01 23:57:40.37+00  |           30
--  15393162789525 | Albaye Papa      | Faye            | 2011-03-22 05:03:56.918+00 |           30
--  21990232561857 | Paul             | Clerc           | 2011-10-22 00:12:31.924+00 |           30
--  26388279076501 | Chen             | Li              | 2012-01-12 20:31:19.036+00 |           30
--  28587302327643 | Deepak           | Reddy           | 2012-03-19 22:38:23.233+00 |           30
--  30786325579031 | Amir             | Lee             | 2012-05-22 16:12:22.233+00 |           30
--  30786325580998 | John             | Singh           | 2012-05-20 13:27:29.263+00 |           30
--  30786325581320 | Deepak           | Roy             | 2012-05-17 11:23:42.909+00 |           30
--  30786325583499 | Ashok            | Reddy           | 2012-05-18 16:26:08.414+00 |           30
--  35184372094081 | Faisal           | Malik           | 2012-09-28 16:29:51.372+00 |           30
--            3903 | Wei              | Li              | 2010-02-03 08:12:07.624+00 |           29
--   2199023258884 | Peng             | Chen            | 2010-03-18 21:51:12.03+00  |           29
--   6597069767028 | Grigore          | Bologan         | 2010-08-19 23:12:33.496+00 |           29
--   6597069772286 | Arun             | Rao             | 2010-08-22 17:43:58.802+00 |           29
--   6597069775770 | Pol              | Nath            | 2010-08-06 17:28:25.712+00 |           29
--   8796093032553 | Priyanka         | Singh           | 2010-10-14 18:38:00.87+00  |           29
--  10995116282430 | Batong           | Tran            | 2011-01-02 03:43:55.824+00 |           29
--  10995116282865 | Ashok            | Khan            | 2010-11-26 06:29:03.99+00  |           29
--  10995116286396 | Yang             | Li              | 2010-12-25 12:16:40.949+00 |           29
-- (100 rows)
--
-- Time: 40116.090 ms (00:40.116)
--
-- BEFORE REWRITING:
-- materialize=> \i q04.sql
-- CREATE MATERIALIZED VIEW
-- Time: 791.930 ms
-- CREATE INDEX
-- Time: 349.982 ms
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-20 18:17:02.072258+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-20 18:17:07.113753+00
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-20 22:19:42.544537+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-20 22:19:47.566627+00
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-21 01:54:04.832211+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-21 01:54:09.850194+00
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-21 05:49:35.058072+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-21 05:49:40.109144+00
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-21 08:42:54.897804+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-21 08:42:59.928996+00
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-21 12:02:54.517557+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-21 12:02:59.559505+00
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "offline" at 2023-07-21 13:30:02.559734+00
-- HINT:  The cluster replica may be restarting or going offline.
-- psql:q04.sql:63: NOTICE:  cluster replica default.r1 changed status to "online" at 2023-07-21 13:30:07.566586+00
-- ^CCancel request sent
-- psql:q04.sql:63: ERROR:  canceling statement due to user request
-- Time: 93951684.641 ms (1 d 02:05:51.685)

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
          MAX(numberOfMembers) AS maxNumberOfMembers
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

WITH
  Top100_Popular_Forums AS (
    SELECT id, creationDate, maxNumberOfMembers
    FROM Top100PopularForumsQ04
    WHERE creationDate > :date
    ORDER BY maxNumberOfMembers DESC, id
    LIMIT 100
  ),
  au AS (
    SELECT *
    FROM Person
    WHERE EXISTS (SELECT 1
          FROM Top100_Popular_Forums
          INNER JOIN Forum_hasMember_Person
                  ON Forum_hasMember_Person.ForumId = Top100_Popular_Forums.id
          WHERE Forum_hasMember_Person.PersonId = Person.id)
  ),
  Top100_Message AS (
    SELECT MessageId,
           CreatorPersonId
    FROM Message
    WHERE Message.ContainerForumId IN (SELECT id FROM Top100_Popular_Forums)
  )

SELECT au.id AS "person.id"
     , au.firstName AS "person.firstName"
     , au.lastName AS "person.lastName"
     , au.creationDate
     -- a single person might be member of more than 1 of the top100 forums, so their messages should be DISTINCT counted
     , COUNT(Top100_Message.MessageId) AS messageCount
FROM      au
LEFT JOIN Top100_Message
       ON au.id = Top100_Message.CreatorPersonId
GROUP BY au.id, au.firstName, au.lastName, au.creationDate
ORDER BY messageCount DESC, au.id
LIMIT 100
;
