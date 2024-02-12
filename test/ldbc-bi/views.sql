-- static entity views (Umbra's create-static-materialized-views.sql)
CREATE OR REPLACE VIEW Country AS
    SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
    FROM Place
    WHERE type = 'Country'
;
CREATE INDEX Country_id ON Country (id);

CREATE OR REPLACE VIEW City AS
    SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
    FROM Place
    WHERE type = 'City'
;
CREATE INDEX City_id ON City (id);
CREATE INDEX City_PartOfCountryId ON City (PartOfCountryId);

CREATE OR REPLACE VIEW Company AS
    SELECT id, name, url, LocationPlaceId AS LocatedInCountryId
    FROM Organisation
    WHERE type = 'Company'
;

CREATE INDEX Company_id ON Company (id);

CREATE OR REPLACE VIEW University AS
    SELECT id, name, url, LocationPlaceId AS LocatedInCityId
    FROM Organisation
    WHERE type = 'University'
;
CREATE INDEX University_id ON University (id);

-- Umbra manually materializes this using load_mht (queries.py)
CREATE OR REPLACE VIEW Message_hasTag_Tag AS
  (SELECT creationDate, CommentId as MessageId, TagId FROM Comment_hasTag_Tag)
  UNION
  (SELECT creationDate, PostId as MessageId, TagId FROM Post_hasTag_Tag);

CREATE INDEX Message_hasTag_Tag_MessageId ON Message_hasTag_Tag (MessageId);
CREATE INDEX Message_hasTag_Tag_TagId ON Message_hasTag_Tag (TagId);

-- Umbra manually materializes this using load_plm (queries.py)
CREATE OR REPLACE VIEW Person_likes_Message AS
  (SELECT creationDate, PersonId, CommentId as MessageId FROM Person_likes_Comment)
  UNION
  (SELECT creationDate, PersonId, PostId as MessageId FROM Person_likes_Post);

CREATE INDEX Person_likes_Message_PersonId ON Person_likes_Message (PersonId);
CREATE INDEX Person_likes_Message_MessageId ON Person_likes_Message (MessageId);

-- A recursive materialized view containing the root Post of each Message (for Posts, themselves, for Comments, traversing up the Message thread to the root Post of the tree)
CREATE OR REPLACE VIEW Message AS
WITH MUTUALLY RECURSIVE
  -- compute the transitive closure (with root information) using minimnal info
  roots (MessageId bigint, RootPostId bigint, RootPostLanguage text, ContainerForumId bigint, ParentMessageId bigint) AS
    (      SELECT id AS MessageId, id AS RootPostId, language AS RootPostLanguage, ContainerForumId, NULL::bigint AS ParentMessageId FROM Post
     UNION SELECT
              Comment.id AS MessageId,
         ParentPostId AS RootPostId,
         language AS RootPostLanguage,
         Post.ContainerForumId AS ContainerForumId,
         ParentPostId AS ParentMessageId
           FROM Comment
       JOIN Post
       ON Comment.ParentPostId = Post.id),
  ms (MessageId bigint, RootPostId bigint, RootPostLanguage text, ContainerForumId bigint, ParentMessageId bigint) AS
    (      SELECT *
           FROM roots
     UNION SELECT
              Comment.id AS MessageId,
         ms.RootPostId AS RootPostId,
         ms.RootPostLanguage AS RootPostLanguage,
         ms.ContainerForumId AS ContainerForumId,
         ParentCommentId AS ParentMessageId
           FROM Comment
       JOIN ms
       ON ParentCommentId = ms.MessageId)
  -- now do the late materialization
  (     SELECT
          creationDate,
          id AS MessageId,
          id AS RootPostId,
          language AS RootPostLanguage,
          content,
          imageFile,
          locationIP,
          browserUsed,
          length,
          CreatorPersonId,
          ContainerForumId,
          LocationCountryId,
          NULL::bigint AS ParentMessageId
        FROM Post
  UNION (SELECT
          Comment.creationDate AS creationDate,
          Comment.id AS MessageId,
          ms.RootPostId AS RootPostId,
          ms.RootPostLanguage AS RootPostLanguage,
          Comment.content AS content,
          NULL::text AS imageFile,
          Comment.locationIP AS locationIP,
          Comment.browserUsed AS browserUsed,
          Comment.length AS length,
          Comment.CreatorPersonId AS CreatorPersonId,
          ms.ContainerForumId AS ContainerForumId,
          Comment.LocationCountryId AS LocationCityId,
          ms.ParentMessageId AS ParentMessageId
    FROM Comment
    JOIN ms
    ON Comment.id = ms.MessageId));

-- every message accounts for exactly one post or comment:
--
-- materialize=> select count(*) from Message;
--   count
-- ---------
--  2860664
-- (1 row)
--
-- Time: 1005301.819 ms (16:45.302)
-- materialize=> select (select count(*) as num_posts from Post) + (select count(*) as num_comments from Comment);
--  ?column?
-- ----------
--   2860664
-- (1 row)
--
-- Time: 18313.558 ms (00:18.314)


CREATE INDEX Message_MessageId ON Message (MessageId);
CREATE INDEX Message_ContainerForumId ON Message (ContainerForumId);
CREATE INDEX Message_ParentMessageId ON Message (ParentMessageId);
CREATE INDEX Message_CreatorPersonId ON Message (CreatorPersonId);
CREATE INDEX Message_RootPostLanguage ON Message (RootPostLanguage);

-- views

CREATE OR REPLACE VIEW Comment_View AS
    SELECT creationDate, MessageId AS id, locationIP, browserUsed, content, length, CreatorPersonId, LocationCountryId, ParentMessageId
    FROM Message
    WHERE ParentMessageId IS NOT NULL;

CREATE OR REPLACE VIEW Post_View AS
    SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed, RootPostLanguage, content, length, CreatorPersonId, ContainerForumId, LocationCountryId
    FROM Message
    WHERE ParentMessageId IS NULL;
