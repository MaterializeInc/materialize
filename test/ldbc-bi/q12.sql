-- TODO(mgree) we're using a different parameter format (raw table for IN expression) instead of an array
\set startDate '\'2012-06-03\''::timestamp
\set lengthThreshold '120'
\set languages '(\'es\', \'ta\', \'pt\')'
-- materialize=> \i q12.sql
--  messagecount | personcount
-- --------------+-------------
--             0 |        3833
--             1 |         622
--             2 |         583
--             3 |         446
--             4 |         425
--             5 |         365
--             6 |         332
--             7 |         272
--             8 |         246
--             9 |         237
--            11 |         190
--            10 |         187
--            12 |         167
--            13 |         166
--            14 |         157
--            16 |         135
--            15 |         119
--            18 |         105
--            19 |          96
--            17 |          92
--            20 |          90
--            22 |          80
--            21 |          78
--            23 |          76
--            24 |          63
--            25 |          58
--            28 |          57
--            26 |          47
--            29 |          45
--            30 |          43
--            32 |          41
--            31 |          41
--            27 |          41
--            35 |          37
--            34 |          37
--            33 |          35
--            39 |          31
--            43 |          23
--            38 |          23
--            37 |          23
--            36 |          23
--            42 |          22
--            55 |          17
--            41 |          17
--            49 |          16
--            45 |          16
--            40 |          16
--            52 |          15
--            46 |          15
--            54 |          14
--            47 |          14
--            57 |          13
--            61 |          12
--            44 |          12
--            70 |          11
--            53 |          10
--            51 |          10
--            62 |           9
--            50 |           9
--            48 |           9
--            77 |           8
--            64 |           8
--            56 |           8
--            87 |           7
--            75 |           7
--            71 |           7
--            68 |           7
--            67 |           7
--            63 |           7
--            60 |           7
--            58 |           7
--            73 |           6
--            59 |           6
--            96 |           5
--            89 |           5
--            76 |           5
--           119 |           4
--            95 |           4
--            79 |           4
--            74 |           4
--           138 |           3
--           137 |           3
--           125 |           3
--           124 |           3
--           106 |           3
--           103 |           3
--           101 |           3
--            97 |           3
--            94 |           3
--            93 |           3
--            92 |           3
--            90 |           3
--            85 |           3
--            82 |           3
--            81 |           3
--            78 |           3
--            72 |           3
--            69 |           3
--            66 |           3
--            65 |           3
--           249 |           2
--           173 |           2
--           154 |           2
--           146 |           2
--           141 |           2
--           133 |           2
--           123 |           2
--           116 |           2
--           111 |           2
--           108 |           2
--           105 |           2
--           100 |           2
--            91 |           2
--            83 |           2
--           526 |           1
--           474 |           1
--           416 |           1
--           406 |           1
--           385 |           1
--           364 |           1
--           335 |           1
--           310 |           1
--           277 |           1
--           262 |           1
--           247 |           1
--           245 |           1
--           218 |           1
--           217 |           1
--           216 |           1
--           215 |           1
--           214 |           1
--           206 |           1
--           205 |           1
--           197 |           1
--           181 |           1
--           179 |           1
--           171 |           1
--           170 |           1
--           166 |           1
--           164 |           1
--           161 |           1
--           160 |           1
--           157 |           1
--           156 |           1
--           155 |           1
--           152 |           1
--           151 |           1
--           150 |           1
--           147 |           1
--           144 |           1
--           143 |           1
--           139 |           1
--           136 |           1
--           134 |           1
--           131 |           1
--           130 |           1
--           129 |           1
--           128 |           1
--           126 |           1
--           122 |           1
--           121 |           1
--           118 |           1
--           117 |           1
--           115 |           1
--           113 |           1
--           112 |           1
--           110 |           1
--           107 |           1
--           102 |           1
--            99 |           1
--            88 |           1
--            86 |           1
--            84 |           1
--            80 |           1
-- (174 rows)
--
-- Time: 5061.933 ms (00:05.062)

/* Q12. How many persons have a given number of messages
\set startDate '\'2010-07-22\''::timestamp
\set lengthThreshold '20'
\set languages '\'{"ar", "hu"}\''::varchar[]
 */
WITH
  matching_message AS (
    SELECT MessageId,
           CreatorPersonId
    FROM Message
    WHERE Message.content IS NOT NULL
      AND Message.length < :lengthThreshold
      AND Message.creationDate > :startDate
      AND Message.RootPostLanguage IN :languages -- MZ change to use postgres containment check
  ),
  person_w_posts AS (
    SELECT Person.id, count(matching_message.MessageId) as messageCount
      FROM Person
      LEFT JOIN matching_message
        ON Person.id = matching_message.CreatorPersonId
     GROUP BY Person.id
  ),
  message_count_distribution AS (
    SELECT pp.messageCount, count(*) as personCount
      FROM person_w_posts pp
     GROUP BY pp.messageCount
     ORDER BY personCount DESC, messageCount DESC
  )
SELECT *
  FROM message_count_distribution
ORDER BY personCount DESC, messageCount DESC
;

-- materialize=> \i q12.sql
-- ^CCancel request sent
-- psql:q12.sql:30: ERROR:  canceling statement due to user request
-- Time: 1030151.977 ms (17:10.152)
-- WITH person_w_posts AS (
--     SELECT Person.id, count(Message.MessageId) as messageCount
--       FROM Person
--       LEFT JOIN Message
--         ON Person.id = Message.CreatorPersonId
--        AND Message.content IS NOT NULL
--        AND Message.length < :lengthThreshold
--        AND Message.creationDate > :startDate
--        AND Message.RootPostLanguage = ANY (:languages) -- MZ change to use postgres containment check
--      GROUP BY Person.id
-- )
-- , message_count_distribution AS (
--     SELECT pp.messageCount, count(*) as personCount
--       FROM person_w_posts pp
--      GROUP BY pp.messageCount
--      ORDER BY personCount DESC, messageCount DESC
-- )
-- SELECT *
--   FROM message_count_distribution
-- ORDER BY personCount DESC, messageCount DESC
-- ;
