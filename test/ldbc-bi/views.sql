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
CREATE OR REPLACE MATERIALIZED VIEW Message AS
    WITH MUTUALLY RECURSIVE
      Message_CTE(creationDate timestamp with time zone,
                  MessageId bigint,
                  RootPostId bigint,
                  RootPostLanguage text,
                  content text,
                  imageFile text,
                  locationIP text,
                  browserUsed text,
                  length int,
                  CreatorPersonId bigint,
                  ContainerForumId bigint,
                  LocationCountryId bigint,
                  ParentMessageId bigint) AS (
        -- base of the union: every post is a message
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
	UNION
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::text AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
        FROM Comment
        JOIN Message_CTE
          ON Message_CTE.MessageId = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
        UNION
        -- second half of the union: transitive closure, following Comments
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::text AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
        FROM Comment
        JOIN Message_CTE
          ON Comment.ParentCommentId = Message_CTE.MessageId -- FORCEORDER?
      )
    -- returning...
    SELECT
    *
    FROM Message_CTE
    WHERE creationDate IS NOT NULL
      AND MessageId IS NOT NULL
      AND RootPostId IS NOT NULL
      AND locationIP IS NOT NULL
      AND browserUsed IS NOT NULL
      AND length IS NOT NULL
      AND CreatorPersonId IS NOT NULL
      AND LocationCountryId IS NOT NULL;
;

CREATE OR REPLACE MATERIALIZED VIEW AllMessages AS
WITH MUTUALLY RECURSIVE
  ms (MessageId bigint, RootId bigint) AS
    (SELECT id AS MessageId, id AS RootId FROM Post
    UNION
     SELECT id AS MessageId, ParentPostId AS RootId FROM Comment WHERE ParentPostId IS NOT NULL AND ParentCommentId IS NULL
    UNION
     SELECT id AS MessageId, ms.RootId FROM Comment JOIN ms ON ParentCommentId = ms.MessageId
    )
  SELECT * FROM ms;

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
