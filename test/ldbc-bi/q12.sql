\set startDate '\'2012-06-03\''::timestamp
\set lengthThreshold '120'
\set languages '\'{es, ta, pt}\''::varchar[]
-- materialize=> \i q12.sql
-- ^CCancel request sent
-- psql:q12.sql:30: ERROR:  canceling statement due to user request
-- Time: 1030151.977 ms (17:10.152)

/* Q12. How many persons have a given number of messages
\set startDate '\'2010-07-22\''::timestamp
\set lengthThreshold '20'
\set languages '\'{"ar", "hu"}\''::varchar[]
 */
WITH person_w_posts AS (
    SELECT Person.id, count(Message.MessageId) as messageCount
      FROM Person
      LEFT JOIN Message
        ON Person.id = Message.CreatorPersonId
       AND Message.content IS NOT NULL
       AND Message.length < :lengthThreshold
       AND Message.creationDate > :startDate
       AND Message.RootPostLanguage = ANY (:languages) -- MZ change to use postgres containment check
     GROUP BY Person.id
)
, message_count_distribution AS (
    SELECT pp.messageCount, count(*) as personCount
      FROM person_w_posts pp
     GROUP BY pp.messageCount
     ORDER BY personCount DESC, messageCount DESC
)
SELECT *
  FROM message_count_distribution
ORDER BY personCount DESC, messageCount DESC
;
