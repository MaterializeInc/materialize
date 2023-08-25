\set datetime '\'2010-06-11T09:21:46.000+00:00\'::TIMESTAMP'
-- materialize=> \i q01.sql
--  messageyear | iscomment | lengthcategory | messagecount | averagemessagelength | summessagelength |           percentageofmessages
-- -------------+-----------+----------------+--------------+----------------------+------------------+-------------------------------------------
--         2010 | f         |              2 |         5906 |   105.26684727395869 |           621706 | 0.198301044219856965382936574555954739281
--         2010 | f         |              3 |          688 |    202.6627906976744 |           139432 | 0.023100426417755095188530369673975086459
--         2010 | t         |              0 |          334 |     4.12874251497006 |             1379 | 0.011214451196991572373501662021958835577
--         2010 | t         |              1 |           57 |    76.59649122807018 |             4366 | 0.001913843467750058758352080045663633616
--         2010 | t         |              2 |           94 |    94.35106382978724 |             8869 | 0.003156162911728167075177114461269851929
--         2010 | t         |              3 |            5 |                171.4 |              857 | 0.000167881005942987610381761407514353826
-- (6 rows)
--
-- Time: 1637056.825 ms (27:17.057)

/* Q1. Posting summary
\set datetime '\'2011-12-01T00:00:00.000+00:00\''::timestamp
 */
WITH
  message_count AS (
    SELECT 0.0 + count(*) AS cnt
      FROM Message
     WHERE creationDate < :datetime
)
, message_prep AS (
    SELECT extract(year from creationDate) AS messageYear
         , ParentMessageId IS NOT NULL AS isComment
         , CASE
             WHEN length <  40 THEN 0 -- short
             WHEN length <  80 THEN 1 -- one liner
             WHEN length < 160 THEN 2 -- tweet
             ELSE                   3 -- long
           END AS lengthCategory
         , length
      FROM Message
     WHERE creationDate < :datetime
       AND content IS NOT NULL
)
SELECT messageYear, isComment, lengthCategory
     , count(*) AS messageCount
     , avg(length::bigint) AS averageMessageLength
     , sum(length::bigint) AS sumMessageLength
     , count(*) / mc.cnt AS percentageOfMessages
  FROM message_prep
     , message_count mc
 GROUP BY messageYear, isComment, lengthCategory, mc.cnt
 ORDER BY messageYear DESC, isComment ASC, lengthCategory ASC
;
