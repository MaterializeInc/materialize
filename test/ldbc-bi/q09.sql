\set startDate '\'2012-08-29\''::timestamp
\set endDate '\'2012-11-24\''::timestamp
-- materialize=> \i q09.sql
--    person.id    | person.firstName  | person.lastName | threadcount | messagecount
-- ----------------+-------------------+-----------------+-------------+--------------
--   8796093032299 | Robert            | Brown           |         144 |         1984
--  26388279073672 | Andrei            | Condariuc       |         185 |         1314
--  24189255815615 | Dan               | Andersson       |         125 |         1182
--  10995116284458 | Mark              | Coetzee         |          97 |         1112
--  24189255811663 | Chris             | Hall            |         197 |         1067
--  28587302329167 | Samir             | Al-Fayez        |         267 |         1033
--   6597069774064 | Ayesha            | Ahmed           |         117 |          961
--  15393162797132 | Shweta            | Kapoor          |         109 |          829
--  30786325579605 | Jorge             | Gonzalez        |          69 |          816
--  28587302323380 | Alexander         | Hleb            |          96 |          735
--  10995116284554 | Ali               | Lo              |          63 |          732
--   4398046519825 | Ahmad Rafiq       | Akbar           |         103 |          711
--  13194139535893 | Benedict          | Wang            |          55 |          697
--  15393162792602 | Francisco         | Gonzalez        |          92 |          697
--  28587302326380 | Sam               | Carter          |          77 |          687
--  17592186045301 | Rudolf            | Engel           |          51 |          674
--   8796093030850 | Monja             | Tsiranana       |          95 |          662
--  32985348835002 | Bingbing          | Liu             |          88 |          643
--  19791209304449 | Peng              | Li              |          44 |          642
--  19791209306158 | Bobby             | Garcia          |          58 |          641
--   2199023265458 | Peter             | Hall            |          74 |          632
--   8796093030826 | K.                | Sharma          |          92 |          618
--  21990232559659 | Baruch            | Dego            |         108 |          594
--  26388279070147 | George            | Nagy            |          74 |          594
--  24189255814889 | Jun               | Zhang           |          78 |          585
--            8478 | James             | Moore           |          44 |          535
--  24189255812073 | Anh               | Pham            |          38 |          510
--  28587302329901 | Shweta            | Khan            |          60 |          510
--  24189255819865 | Sombat            | Amarttayakul    |          69 |          500
--   8796093023472 | K.                | Bose            |          82 |          497
--  21990232563252 | Avraham           | Alhouthi        |         100 |          480
--  10995116283619 | Maria             | Kibona          |         101 |          474
--  24189255812627 | Edward            | Ouma            |          43 |          474
--  26388279066798 | Helen             | Carr            |          54 |          462
--   2199023258646 | Abdul Rahman      | Budjana         |          63 |          455
--  28587302326092 | John              | Sen             |          47 |          434
--   8796093032304 | Eli               | Chihab          |          62 |          433
--  19791209308287 | Sam               | Huang           |          33 |          432
--  15393162789959 | Alfred            | Hoffmann        |          81 |          422
--  24189255814904 | Abdulwahab        | Gillett         |          56 |          422
--   2199023263408 | Piotr             | Dobrowolski     |          57 |          419
--  15393162795695 | Robert            | Bratianu        |          32 |          419
--  26388279074683 | Adrian            | Popescu         |          59 |          419
--   8796093023326 | Joseph            | Sharma          |          30 |          414
--  15393162791403 | Aaron             | Cruz            |          70 |          409
--  26388279073393 | Rajiv             | Singh           |          36 |          409
--  19791209308584 | Huu               | Ho              |          29 |          402
--  21990232563874 | Howard            | Ha              |          29 |          397
--  24189255820411 | Jose              | Sotto           |          33 |          396
--  17592186054998 | Manuel            | Cosio           |          43 |          395
--  32985348843490 | Yang              | Li              |          38 |          394
--  24189255811542 | Fatmir            | Mojsov          |          73 |          388
--  17592186052709 | Babar             | Hussain         |          80 |          379
--  10995116283889 | Joe               | Wong            |          26 |          373
--  21990232557614 | Hoang Yen         | Nguyen          |          69 |          370
--  21990232561742 | Abhishek          | Roy             |          59 |          370
--  28587302327742 | Jun               | Li              |          33 |          359
--  17592186052516 | Yang              | Yang            |          26 |          358
--   2199023260815 | Chen              | Li              |          32 |          352
--  19791209309084 | Adriaan           | Dijk            |          48 |          345
--  17592186049517 | Natalia Barbu     | Goma            |         105 |          343
--  28587302326226 | Mohamed           | Achiou          |          70 |          338
--  17592186046498 | Andrea            | Blanco          |         141 |          330
--  19791209302061 | Manuel            | Martins         |          81 |          330
--   2199023256462 | Emperor of Brazil | Dom Pedro II    |          69 |          329
--  17592186050645 | Ning              | Zhang           |          76 |          329
--  35184372092691 | Carlos            | Escobar         |          41 |          327
--  10995116279521 | Rahul             | Nair            |          54 |          326
--  15393162796126 | Agustiar          | Irama           |          29 |          313
--  15393162799448 | Fritz             | Muller          |          47 |          310
--   6597069770739 | Abdul Haris       | Gallagher       |          28 |          302
--  28587302326788 | Carlos            | Rodriguez       |          65 |          301
--  26388279072682 | Abderrahmane      | Ferrer          |          96 |          300
--   6597069767028 | Grigore           | Bologan         |          65 |          299
--  26388279067483 | Aditya            | Khan            |          26 |          298
--  10995116285747 | Koji              | Tanaka          |          25 |          292
--  26388279068136 | Koji              | Kato            |          20 |          289
--   8796093023499 | Bruna             | Costa           |          93 |          285
--  17592186049899 | Ivan Ignatyevich  | Aleksandrov     |          42 |          279
--  15393162789362 | K.                | Kumar           |          24 |          278
--  17592186050809 | Pablo             | Cosio           |          29 |          278
--  10995116281358 | Kunal             | Kumar           |          20 |          277
--  17592186051871 | Ashok             | Sharma          |          56 |          276
--  19791209305198 | Martin            | Muller          |          39 |          275
--  19791209300230 | Benhalima         | Ferrer          |          42 |          273
--   4398046520667 | Charles           | Bona            |          51 |          272
--  32985348835600 | Isabel            | Diaz            |          45 |          272
--  24189255815184 | K.                | Kumar           |          31 |          270
--   8796093028247 | Jie               | Wang            |          19 |          268
--   2199023260487 | Bing              | Yang            |          50 |          266
--  17592186054199 | Fritz             | Fischer         |          61 |          264
--            2662 | Priyanka          | Nair            |          43 |          260
--            2783 | Rafael            | Alonso          |          65 |          260
--  24189255815100 | Albert            | Jong            |          51 |          260
--  17592186047024 | Wei               | Zhu             |          62 |          259
--  21990232563483 | Zhong             | Wang            |          89 |          253
--  24189255817213 | Álvaro            | Rojas           |          48 |          252
--           10349 | Ahmad             | Arief           |          45 |          250
--   2199023265231 | Wei               | Wang            |          82 |          249
--  19791209301504 | Jose              | Costa           |          16 |          241
-- (100 rows)
--
-- Time: 3765.033 ms (00:03.765)

/* Q9. Top thread initiators
\set startDate '\'2011-10-01\''::timestamp
\set endDate '\'2011-10-15\''::timestamp
 */
WITH
MPP AS (SELECT RootPostId, count(*) as MessageCount FROM Message WHERE Message.creationDate BETWEEN :startDate AND :endDate GROUP BY RootPostId)
SELECT Person.id AS "person.id"
     , Person.firstName AS "person.firstName"
     , Person.lastName AS "person.lastName"
     , count(Post.id) AS threadCount
     , sum(MPP.MessageCount) AS messageCount
  FROM Person
  JOIN Post_View Post
    ON Person.id = Post.CreatorPersonId
  JOIN MPP
    ON Post.id = MPP.RootPostId
 WHERE Post.creationDate BETWEEN :startDate AND :endDate
 GROUP BY Person.id, Person.firstName, Person.lastName
 ORDER BY messageCount DESC, Person.id
 LIMIT 100
;
