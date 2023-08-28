\set tag '\'Slovenia\''
-- materialize=> \i q07.sql
--              relatedTag.name              | count
-- ------------------------------------------+-------
--  Humayun                                  |    10
--  Genghis_Khan                             |     9
--  Muammar_Gaddafi                          |     9
--  Sammy_Sosa                               |     8
--  Thomas_Hardy                             |     7
--  Dalai_Lama                               |     6
--  Fernando_González                        |     6
--  Haile_Selassie_I                         |     6
--  Mariano_Rivera                           |     6
--  Peter_Hain                               |     6
--  Ronald_Reagan                            |     6
--  Bertrand_Russell                         |     5
--  Bill_Clinton                             |     5
--  Bob_Dole                                 |     5
--  Diana,_Princess_of_Wales                 |     5
--  Dimitri_Tiomkin                          |     5
--  Duchy_of_Warsaw                          |     5
--  Dustin_Hoffman                           |     5
--  Gerald_Ford                              |     5
--  Indonesia                                |     5
--  Julius_Caesar                            |     5
--  Monaco                                   |     5
--  Netherlands                              |     5
--  Nicaragua                                |     5
--  Poland                                   |     5
--  Sierra_Leone                             |     5
--  Silvio_Berlusconi                        |     5
--  Soviet_Union                             |     5
--  Sudan                                    |     5
--  Tiger_Woods                              |     5
--  Albert_Einstein                          |     4
--  Arthur_Wellesley,_1st_Duke_of_Wellington |     4
--  Australia                                |     4
--  Axis_powers                              |     4
--  Batavian_Republic                        |     4
--  Bosnia_and_Herzegovina                   |     4
--  Clark_Gable                              |     4
--  Colin_Powell                             |     4
--  Edward_IV_of_England                     |     4
--  Elizabeth_II                             |     4
--  Etruscan_civilization                    |     4
--  Fidel_Castro                             |     4
--  Freddie_Mercury                          |     4
--  George_Gershwin                          |     4
--  German_Empire                            |     4
--  Ghana                                    |     4
--  Greece                                   |     4
--  Gustav_Mahler                            |     4
--  Imelda_Marcos                            |     4
--  India                                    |     4
--  James_Buchanan                           |     4
--  Joseph_Stalin                            |     4
--  Leo_Tolstoy                              |     4
--  Lewis_Carroll                            |     4
--  Louis_Philippe_I                         |     4
--  Martin_Luther                            |     4
--  Martin_Luther_King,_Jr.                  |     4
--  Montenegro                               |     4
--  Monty_Python                             |     4
--  Moon_River                               |     4
--  Nicolas_Sarkozy                          |     4
--  Puerto_Rico                              |     4
--  Raphael                                  |     4
--  Richard_Rodgers                          |     4
--  Rwanda                                   |     4
--  Saint_Pierre_and_Miquelon                |     4
--  Samuel_Pepys                             |     4
--  The_Bahamas                              |     4
--  Tori_Amos                                |     4
--  Turks_and_Caicos_Islands                 |     4
--  Under_Pressure                           |     4
--  Vatican_City                             |     4
--  Vladimir_Lenin                           |     4
--  Wales                                    |     4
--  William_McKinley                         |     4
--  Abraham_Lincoln                          |     3
--  Al_Gore                                  |     3
--  Alfred,_Lord_Tennyson                    |     3
--  Algeria                                  |     3
--  Andy_Roddick                             |     3
--  Aristophanes                             |     3
--  Azad_Hind                                |     3
--  Baby,_I_Love_Your_Way                    |     3
--  Bangladesh                               |     3
--  Batman                                   |     3
--  Bill_Cosby                               |     3
--  Bing_Crosby                              |     3
--  Bolivia                                  |     3
--  British_North_America                    |     3
--  British_Overseas_Territories             |     3
--  Burma                                    |     3
--  Carl_Gustaf_Emil_Mannerheim              |     3
--  Cecil_B._DeMille                         |     3
--  Charles,_Prince_of_Wales                 |     3
--  Charles_de_Gaulle                        |     3
--  Charlton_Heston                          |     3
--  Christina_Aguilera                       |     3
--  Christopher_Lee                          |     3
--  Chuck_Berry                              |     3
--  Chulalongkorn                            |     3
-- (100 rows)
--
-- Time: 17272.877 ms (00:17.273)

/* Q7. Related topics
\set tag '\'Enrique_Iglesias\''
 */
WITH MyMessage AS (
  SELECT m.MessageId
  FROM Message_hasTag_Tag m, Tag
  WHERE Tag.name = :tag and m.TagId = Tag.Id
)
SELECT RelatedTag.name AS "relatedTag.name"
     , count(*) AS count
  FROM MyMessage ParentMessage_HasTag_Tag
  -- as an optimization, we don't need message here as it's ID is in ParentMessage_HasTag_Tag
  -- so proceed to the comment directly
  INNER JOIN Message Comment
          ON ParentMessage_HasTag_Tag.MessageId = Comment.ParentMessageId
  -- comment's tag
  LEFT  JOIN Message_hasTag_Tag ct
          ON Comment.MessageId = ct.MessageId
  INNER JOIN Tag RelatedTag
          ON RelatedTag.id = ct.TagId
 WHERE TRUE
  -- comment doesn't have the given tag
   AND Comment.MessageId NOT In (SELECT MessageId FROM MyMessage)
   AND Comment.ParentMessageId IS NOT NULL
 GROUP BY RelatedTag.Name
 ORDER BY count DESC, RelatedTag.name
 LIMIT 100
;
