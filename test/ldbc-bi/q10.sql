\set personId 6597069770479
\set country '\'Italy\''
\set tagClass '\'Thing\''
\set minPathDistance 3 -- fixed value
\set maxPathDistance 4 -- fixed value
-- materialize=> \i q10.sql
--    person.id    |             tag.name              | messagecount
-- ----------------+-----------------------------------+--------------
--   2199023257601 | Ralph_Vaughan_Williams            |            3
--   2199023257601 | Aristophanes                      |            2
--  15393162799165 | Aristophanes                      |            2
--  19791209309107 | Aristophanes                      |            2
--            6079 | Augustine_of_Hippo                |            2
--  30786325585300 | Bob_Hope                          |            2
--   2199023257601 | Canada                            |            2
--  15393162799165 | Dmitry_Medvedev                   |            2
--   2199023259874 | Franklin_D._Roosevelt             |            2
--  15393162793184 | Interpol                          |            2
--   2199023257601 | Jean-Baptiste_Lamarck             |            2
--   2199023261968 | Johannes_Brahms                   |            2
--  19791209309107 | John_Cage                         |            2
--  19791209309107 | Leonard_Bernstein                 |            2
--   2199023259874 | Ludwig_van_Beethoven              |            2
--   4398046512280 | Muammar_Gaddafi                   |            2
--   2199023257601 | Mughal_Empire                     |            2
--  30786325581417 | Plácido_Domingo                   |            2
--   2199023257601 | Slavoj_Žižek                      |            2
--  30786325581417 | Wayne_Gretzky                     |            2
--   2199023261968 | Wolfgang_Amadeus_Mozart           |            2
--            9043 | 14th_Dalai_Lama                   |            1
--   2199023257601 | 14th_Dalai_Lama                   |            1
--   4398046512280 | 14th_Dalai_Lama                   |            1
--             217 | 99_Luftballons                    |            1
--  30786325585300 | A_Day_Without_Rain                |            1
--  28587302322352 | A_Kind_of_Magic                   |            1
--   2199023259874 | A_Little_Bit_Me,_a_Little_Bit_You |            1
--            7684 | A_Whiter_Shade_of_Pale            |            1
--  15393162793184 | Aaron_Copland                     |            1
--  17592186053088 | Abbasid_Caliphate                 |            1
--  32985348842018 | Adolf_Hitler                      |            1
--             217 | Alan_Moore                        |            1
--  19791209309107 | Albert_Einstein                   |            1
--             217 | Alexander_Downer                  |            1
--  19791209310154 | Alexander_Pushkin                 |            1
--   4398046517288 | Alfred,_Lord_Tennyson             |            1
--   2199023259874 | Alfred_Hitchcock                  |            1
--            9043 | Ali                               |            1
--   2199023257601 | Ali                               |            1
--   4398046512280 | Ali                               |            1
--  21990232560354 | Ali                               |            1
--  21990232564061 | All_the_Roadrunning               |            1
--  32985348842085 | Ammon                             |            1
--            6079 | Andrew_Jackson                    |            1
--  19791209309107 | Angola                            |            1
--  30786325581056 | Another_One_Bites_the_Dust        |            1
--   2199023257601 | Anton_Chekhov                     |            1
--   4398046512280 | Are_You_Experienced               |            1
--  21990232564061 | Arenberg                          |            1
--             217 | Aristophanes                      |            1
--   2199023259874 | Aristophanes                      |            1
--  30786325585300 | Aristophanes                      |            1
--            6079 | Aristotle                         |            1
--  10995116279128 | Arnold_Schwarzenegger             |            1
--  32985348842085 | Arthur_William_Baden_Powell       |            1
--  19791209309107 | Ashikaga_shogunate                |            1
--  30786325585300 | At_Last                           |            1
--  28587302322352 | At_War_with_the_Mystics           |            1
--  30786325581417 | Augustin_Pyramus_de_Candolle      |            1
--  10995116285979 | Augustine_of_Hippo                |            1
--  32985348842085 | Augustine_of_Hippo                |            1
--            9043 | Austrian_Empire                   |            1
--  19791209310154 | Banned_in_the_U.S.A.              |            1
--   2199023257601 | Barbra_Streisand                  |            1
--   2199023257601 | Barrio_Fino                       |            1
--   2199023259874 | Barry_Manilow                     |            1
--   2199023259874 | Before_You_Were_Punk              |            1
--  30786325581417 | Benjamin_Harrison                 |            1
--  30786325585300 | Bertolt_Brecht                    |            1
--  19791209309107 | Bill_Clinton                      |            1
--  30786325585300 | Bill_Clinton                      |            1
--  32985348842018 | Blood_Sugar_Sex_Magik             |            1
--   2199023257601 | Bob_Dole                          |            1
--  19791209309107 | Bob_Dylan                         |            1
--  32985348842085 | Botswana                          |            1
--  10995116279128 | Break_Like_the_Wind               |            1
--  30786325585300 | Breathe_Again                     |            1
--  21990232564061 | British_Ceylon                    |            1
--   2199023261968 | British_Empire                    |            1
--   2199023257601 | Burma                             |            1
--  19791209309107 | Busta_Rhymes                      |            1
--  30786325581417 | Busta_Rhymes                      |            1
--  30786325585300 | Béla_Bartók                       |            1
--  19791209309107 | C._S._Lewis                       |            1
--  21990232564061 | Cameroon                          |            1
--  35184372093958 | Cannot_Buy_My_Soul                |            1
--   2199023257601 | Cape_Verde                        |            1
--  19791209310154 | Carl_Gustaf_Emil_Mannerheim       |            1
--   4398046518779 | Chaka_Khan                        |            1
--   2199023259874 | Champa                            |            1
--            7684 | Charles_Dickens                   |            1
--  32985348842085 | Charles_II_of_England             |            1
--  30786325585300 | Charles_I_of_England              |            1
--  15393162793184 | Charlton_Heston                   |            1
--   4398046517288 | Che_Guevara                       |            1
--  30786325581417 | Christopher_Columbus              |            1
--   2199023257601 | Chrome_Dreams_II                  |            1
--  10995116279128 | Claude_Debussy                    |            1
--  10995116285979 | Claude_Debussy                    |            1
-- (100 rows)
--
-- Time: 22849.938 ms (00:22.850)

/* Q10. Experts in social circle
\set personId 30786325588624
\set country '\'China\''
\set tagClass '\'MusicalArtist\''
\set minPathDistance 3 -- fixed value
\set maxPathDistance 4 -- fixed value
 */
WITH friends AS
  (SELECT Person2Id
     FROM Person_knows_Person
    WHERE Person1Id = :personId
  )
  , friends_of_friends AS
  (SELECT knowsB.Person2Id AS Person2Id
     FROM friends
     JOIN Person_knows_Person knowsB
       ON friends.Person2Id = knowsB.Person1Id
  )
  , friends_and_friends_of_friends AS
  (SELECT Person2Id
     FROM friends
    UNION -- using plain UNION to eliminate duplicates
   SELECT Person2Id
     FROM friends_of_friends
  )
  , friends_between_3_and_4_hops AS (
    -- people reachable through 1..4 hops
    (SELECT DISTINCT knowsD.Person2Id AS Person2Id
      FROM friends_and_friends_of_friends ffoaf
      JOIN Person_knows_Person knowsC
        ON knowsC.Person1Id = ffoaf.Person2Id
      JOIN Person_knows_Person knowsD
        ON knowsD.Person1Id = knowsC.Person2Id
    )
    -- removing people reachable through 1..2 hops, yielding the ones reachable through 3..4 hops
    EXCEPT
    (SELECT Person2Id
      FROM friends_and_friends_of_friends
    )
  )
  , friend_list AS (
    SELECT f.person2Id AS friendId
      FROM friends_between_3_and_4_hops f
      JOIN Person tf -- the friend's person record
        ON tf.id = f.person2Id
      JOIN City
        ON City.id = tf.LocationCityId
      JOIN Country
        ON Country.id = City.PartOfCountryId
       AND Country.name = :country
  )
  , messages_of_tagclass_by_friends AS (
    SELECT DISTINCT f.friendId
         , Message.MessageId AS messageid
      FROM friend_list f
      JOIN Message
        ON Message.CreatorPersonId = f.friendId
      JOIN Message_hasTag_Tag
        ON Message_hasTag_Tag.MessageId = Message.MessageId
      JOIN Tag
        ON Tag.id = Message_hasTag_Tag.TagId
      JOIN TagClass
        ON TagClass.id = Tag.TypeTagClassId
      WHERE TagClass.name = :tagClass
  )
SELECT m.friendId AS "person.id"
     , Tag.name AS "tag.name"
     , count(*) AS messageCount
  FROM messages_of_tagclass_by_friends m
  JOIN Message_hasTag_Tag
    ON Message_hasTag_Tag.MessageId = m.MessageId
  JOIN Tag
    ON Tag.id = Message_hasTag_Tag.TagId
 GROUP BY m.friendId, Tag.name
 ORDER BY messageCount DESC, Tag.name, m.friendId
 LIMIT 100
;
