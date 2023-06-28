-- static entity views (Umbra's create-static-materialized-views.sql)
CREATE OR REPLACE MATERIALIZED VIEW Country AS
    SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
    FROM Place
    WHERE type = 'Country'
;
CREATE INDEX Country_id ON Country (id);

CREATE OR REPLACE MATERIALIZED VIEW City AS
    SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
    FROM Place
    WHERE type = 'City'
;
CREATE INDEX City_id ON City (id);

CREATE OR REPLACE MATERIALIZED VIEW Company AS
    SELECT id, name, url, LocationPlaceId AS LocatedInCountryId
    FROM Organisation
    WHERE type = 'Company'
;

CREATE INDEX Company_id ON Company (id);

CREATE OR REPLACE MATERIALIZED VIEW University AS
    SELECT id, name, url, LocationPlaceId AS LocatedInCityId
    FROM Organisation
    WHERE type = 'University'
;
CREATE INDEX University_id ON University (id);

-- Umbra manually materializes this using load_mht (queries.py)
CREATE OR REPLACE MATERIALIZED VIEW Message_hasTag_Tag AS
  (SELECT creationDate, CommentId as MessageId, TagId FROM Comment_hasTag_Tag)
  UNION
  (SELECT creationDate, PostId as MessageId, TagId FROM Post_hasTag_Tag);

-- Umbra manually materializes this using load_plm (queries.py)
CREATE OR REPLACE MATERIALIZED VIEW Person_likes_Message AS
  (SELECT creationDate, PersonId, CommentId as MessageId FROM Person_likes_Comment)
  UNION
  (SELECT creationDate, PersonId, PostId as MessageId FROM Person_likes_Post);

-- A recursive materialized view containing the root Post of each Message (for Posts, themselves, for Comments, traversing up the Message thread to the root Post of the tree)
CREATE OR REPLACE MATERIALIZED VIEW AllMessages AS
WITH MUTUALLY RECURSIVE
  -- compute the transitive closure (with root information) using minimnal info
  roots (MessageId bigint, RootId bigint, RootPostLanguage text, ContainerForumId bigint, ParentMessageId bigint) AS
    (      SELECT id AS MessageId, id AS RootId, language AS RootPostLanguage, ContainerForumId, NULL::bigint AS ParentMessageId FROM Post
     UNION SELECT
     	     id AS MessageId,
	     ParentPostId AS RootId,
	     language AS RootPostLanguage,
	     ContainerForumId,
	     ParentPostId as ParentMessageId,
           FROM Comment
	   JOIN Post
	   ON Comment.ParentPostId = Post.id)
  ms (MessageId bigint, RootId bigint, RootPostLanguage text, ContainerForumId bigint, ParentMessageId bigint) AS
    (      SELECT * from roots
     UNION SELECT id AS MessageId, ms.RootId, ms.RootPostLanguage, ms.ContainerForumId, ParentCommentId as ParentMessageId FROM Comment JOIN ms ON ParentCommentId = ms.MessageId)
  -- now do the late materialization
        SELECT
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
  UNION SELECT
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
	ON Comment.id = ms.MessageId;


CREATE INDEX Message_MessageId ON Message (MessageId);

-- views

CREATE OR REPLACE VIEW Comment_View AS
    SELECT creationDate, MessageId AS id, locationIP, browserUsed, content, length, CreatorPersonId, LocationCountryId, ParentMessageId
    FROM Message
    WHERE ParentMessageId IS NOT NULL;

CREATE OR REPLACE VIEW Post_View AS
    SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed, RootPostLanguage, content, length, CreatorPersonId, ContainerForumId, LocationCountryId
    FROM Message
    WHERE ParentMessageId IS NULL;
