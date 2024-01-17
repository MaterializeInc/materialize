-- clear out tables

DROP TABLE IF EXISTS Organisation CASCADE;
DROP TABLE IF EXISTS Place CASCADE;
DROP TABLE IF EXISTS Tag CASCADE;
DROP TABLE IF EXISTS TagClass CASCADE;
DROP TABLE IF EXISTS Comment CASCADE;
DROP TABLE IF EXISTS Forum CASCADE;
DROP TABLE IF EXISTS Post CASCADE;
DROP TABLE IF EXISTS Person CASCADE;
DROP TABLE IF EXISTS Comment_hasTag_Tag CASCADE;
DROP TABLE IF EXISTS Post_hasTag_Tag CASCADE;
DROP TABLE IF EXISTS Forum_hasMember_Person CASCADE;
DROP TABLE IF EXISTS Forum_hasTag_Tag CASCADE;
DROP TABLE IF EXISTS Person_hasInterest_Tag CASCADE;
DROP TABLE IF EXISTS Person_likes_Comment CASCADE;
DROP TABLE IF EXISTS Person_likes_Post CASCADE;
DROP TABLE IF EXISTS Person_studyAt_University CASCADE;
DROP TABLE IF EXISTS Person_workAt_Company CASCADE;
DROP TABLE IF EXISTS Person_knows_Person CASCADE;

\! echo OLD TABLES DROPPED
\! date -Iseconds

-- static tables

CREATE TABLE Organisation (
    id bigint NOT NULL,
    type text NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    LocationPlaceId bigint NOT NULL
);

CREATE INDEX Organisation_id ON Organisation (id);

CREATE TABLE Place (
    id bigint NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    type text NOT NULL,
    PartOfPlaceId bigint -- null for continents
);

CREATE INDEX Place_id ON Place (id);

CREATE TABLE Tag (
    id bigint NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    TypeTagClassId bigint NOT NULL
);

CREATE INDEX Tag_id ON Tag (id);
CREATE INDEX Tag_name ON Tag (name);
CREATE INDEX Tag_TypeTagClassId ON Tag (TypeTagClassId);

CREATE TABLE TagClass (
    id bigint NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
);

CREATE INDEX TagClass_id ON TagClass (id);
CREATE INDEX TagClass_name ON TagClass (name);

-- dynamic tables

CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    content text NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
);

CREATE INDEX Comment_id ON Comment (id);

CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL,
    title text NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);

CREATE INDEX Forum_id ON Forum (id);
CREATE INDEX Forum_ModeratorPersonId on Forum (ModeratorPersonId);

CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL,
    imageFile text,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    language text,
    content text,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
);

CREATE INDEX Post_id ON Post (id);

CREATE TABLE Person (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL,
    firstName text NOT NULL,
    lastName text NOT NULL,
    gender text NOT NULL,
    birthday date NOT NULL,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks text NOT NULL,
    email text NOT NULL
);

CREATE INDEX Person_id ON Person (id);
CREATE INDEX Person_LocationCityId ON Person (LocationCityId);

-- edges

CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

CREATE INDEX Forum_hasMember_Person_ForumId ON Forum_hasMember_Person (ForumId);
CREATE INDEX Forum_hasMember_Person_PersonId ON Forum_hasMember_Person (PersonId);

CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE INDEX Person_hasInterest_Tag_TagId ON Person_hasInterest_Tag (TagId);

CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);

CREATE INDEX Person_studyAt_University_PersonId ON Person_studyAt_University (PersonId);
CREATE INDEX Person_studyAt_University_UniversityId ON Person_studyAt_University (UniversityId);

CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);

CREATE INDEX Person_workAt_Company_PersonId ON Person_workAt_Company (PersonId);
CREATE INDEX Person_workAt_Company_CompanyId ON Person_workAt_Company (CompanyId);

CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);

CREATE INDEX Person_knows_Person_Person1id ON Person_knows_Person (Person1id);
CREATE INDEX Person_knows_Person_Person2id ON person_knows_person (Person2id);
CREATE INDEX Person_knows_Person_Person1id_Person2id ON Person_knows_Person (Person1id, Person2id);
