# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from textwrap import dedent
from urllib.parse import quote

import psycopg

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.dbt import Dbt
from materialize.mzcompose.services.testdrive import Testdrive

# The actual values are stored as Pulumi secrets in the i2 repository
MATERIALIZE_PROD_SANDBOX_HOSTNAME = os.getenv("MATERIALIZE_PROD_SANDBOX_HOSTNAME")
MATERIALIZE_PROD_SANDBOX_USERNAME = os.getenv("MATERIALIZE_PROD_SANDBOX_USERNAME")
MATERIALIZE_PROD_SANDBOX_APP_PASSWORD = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD"
)

MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME"
)
MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD"
)

MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME"
)
MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD = os.getenv(
    "MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD"
)

CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER = os.getenv(
    "CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER"
)
CONFLUENT_CLOUD_FIELDENG_CSR_URL = os.getenv("CONFLUENT_CLOUD_FIELDENG_CSR_URL")

CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME = os.getenv(
    "CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME"
)
CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD = os.getenv(
    "CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD"
)
CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME = os.getenv(
    "CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME"
)
CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD = os.getenv(
    "CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD"
)


SERVICES = [
    Dbt(
        environment=[
            "MATERIALIZE_PROD_SANDBOX_HOSTNAME",
            "MATERIALIZE_PROD_SANDBOX_USERNAME",
            "MATERIALIZE_PROD_SANDBOX_APP_PASSWORD",
        ]
    ),
    Testdrive(
        no_reset=True,
        no_consistency_checks=True,  # No access to HTTP for coordinator check
    ),
]

POSTGRES_RANGE = 1024
POSTGRES_RANGE_FUNCTION = "FLOOR(RANDOM() * (SELECT MAX(id) FROM people))"
MYSQL_RANGE = 1024
MYSQL_RANGE_FUNCTION = (
    "FLOOR(RAND() * (SELECT MAX(id) FROM (SELECT * FROM people) AS p))"
)


def workflow_create(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up(Service("dbt", idle=True), Service("testdrive", idle=True))

    assert MATERIALIZE_PROD_SANDBOX_USERNAME is not None
    assert MATERIALIZE_PROD_SANDBOX_APP_PASSWORD is not None
    assert MATERIALIZE_PROD_SANDBOX_HOSTNAME is not None
    materialize_url = f"postgres://{quote(MATERIALIZE_PROD_SANDBOX_USERNAME)}:{quote(MATERIALIZE_PROD_SANDBOX_APP_PASSWORD)}@{quote(MATERIALIZE_PROD_SANDBOX_HOSTNAME)}:6875"

    with c.override(
        Testdrive(
            default_timeout="1200s",
            materialize_url=materialize_url,
            no_reset=True,  # Required so that admin port 6877 is not used
            no_consistency_checks=True,  # No access to HTTP for coordinator check
        )
    ):
        c.testdrive(
            input=dedent(
                f"""
            > SET DATABASE=qa_canary_environment
            > CREATE SECRET IF NOT EXISTS kafka_username AS '{CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME}'
            > CREATE SECRET IF NOT EXISTS kafka_password AS '{CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (
              BROKER '{CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER}',
              SASL MECHANISMS = 'PLAIN',
              SASL USERNAME = SECRET kafka_username,
              SASL PASSWORD = SECRET kafka_password
              )

            > CREATE SECRET IF NOT EXISTS csr_username AS '{CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME}'
            > CREATE SECRET IF NOT EXISTS csr_password AS '{CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS csr_connection TO CONFLUENT SCHEMA REGISTRY (
              URL '{CONFLUENT_CLOUD_FIELDENG_CSR_URL}',
              USERNAME = SECRET csr_username,
              PASSWORD = SECRET csr_password
              )
        """
            )
        )

        c.testdrive(
            input=dedent(
                f"""
            > SET DATABASE=qa_canary_environment
            $ mysql-connect name=mysql url=mysql://admin@{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME} password={MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD}
            $ mysql-execute name=mysql
            DROP DATABASE IF EXISTS public;
            CREATE DATABASE public;
            USE public;

            CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT, incarnation INTEGER DEFAULT 1);
            CREATE TABLE IF NOT EXISTS relationships (a INTEGER, b INTEGER, incarnation INTEGER DEFAULT 1, PRIMARY KEY (a,b));

            CREATE EVENT insert_people ON SCHEDULE EVERY 1 SECOND DO INSERT INTO people (id, name) VALUES (FLOOR(RAND() * {MYSQL_RANGE}), 'aaaaaaaaaaaaaaaa'), (FLOOR(RAND() * {MYSQL_RANGE}), 'aaaaaaaaaaaaaaaa') ON DUPLICATE KEY UPDATE incarnation = people.incarnation + 1;
            CREATE EVENT update_people_name ON SCHEDULE EVERY 1 SECOND DO UPDATE people SET name = REPEAT(id, 16) WHERE id = {MYSQL_RANGE_FUNCTION};
            CREATE EVENT update_people_incarnation ON SCHEDULE EVERY 1 SECOND DO UPDATE people SET incarnation = incarnation + 1 WHERE id = {MYSQL_RANGE_FUNCTION};
            CREATE EVENT delete_people ON SCHEDULE EVERY 1 SECOND DO DELETE FROM people WHERE id = {MYSQL_RANGE_FUNCTION};

            -- MOD() is used to prevent truly random relationships from being created, as this overwhelms WMR
            -- See https://materialize.com/docs/sql/recursive-ctes/#queries-with-update-locality
            CREATE EVENT insert_relationships ON SCHEDULE EVERY 1 SECOND DO INSERT INTO relationships (a, b) VALUES (MOD({MYSQL_RANGE_FUNCTION}, 10), {MYSQL_RANGE_FUNCTION}), (MOD({MYSQL_RANGE_FUNCTION}, 10), {MYSQL_RANGE_FUNCTION}) ON DUPLICATE KEY UPDATE incarnation = relationships.incarnation + 1;
            CREATE EVENT update_relationships_incarnation ON SCHEDULE EVERY 1 SECOND DO UPDATE relationships SET incarnation = incarnation + 1 WHERE a = {MYSQL_RANGE_FUNCTION} and b = {MYSQL_RANGE_FUNCTION};
            CREATE EVENT delete_relationships ON SCHEDULE EVERY 1 SECOND DO DELETE FROM relationships WHERE a = {MYSQL_RANGE_FUNCTION} AND b = {MYSQL_RANGE_FUNCTION};

            > CREATE SECRET IF NOT EXISTS mysql_password AS '{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS mysql TO MYSQL (
              HOST '{MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME}',
              USER admin,
              PASSWORD SECRET mysql_password
              )

            $ postgres-execute connection=postgres://postgres:{MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD}@{MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME}
            -- ALTER USER postgres WITH replication;

            DROP SCHEMA IF EXISTS public CASCADE;
            CREATE SCHEMA public;

            CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, name TEXT DEFAULT REPEAT('a', 16), incarnation INTEGER DEFAULT 1);
            ALTER TABLE people REPLICA IDENTITY FULL;

            CREATE TABLE IF NOT EXISTS relationships (a INTEGER, b INTEGER, incarnation INTEGER DEFAULT 1, PRIMARY KEY (a,b));
            ALTER TABLE relationships REPLICA IDENTITY FULL;

            DROP PUBLICATION IF EXISTS mz_source;
            CREATE PUBLICATION mz_source FOR ALL TABLES;

            CREATE EXTENSION IF NOT EXISTS pg_cron;

            SELECT cron.schedule('insert-people', '1 seconds', 'INSERT INTO people (id) SELECT FLOOR(RANDOM() * {POSTGRES_RANGE}) FROM generate_series(1,2) ON CONFLICT (id) DO UPDATE SET incarnation = people.incarnation + 1');
            SELECT cron.schedule('update-people-name', '1 seconds', 'UPDATE people SET name = REPEAT(id::text, 16) WHERE id = {POSTGRES_RANGE_FUNCTION}');
            SELECT cron.schedule('update-people-incarnation', '1 seconds', 'UPDATE people SET incarnation = incarnation + 1 WHERE id = {POSTGRES_RANGE_FUNCTION}');
            SELECT cron.schedule('delete-people', '1 seconds', 'DELETE FROM people WHERE id = {POSTGRES_RANGE_FUNCTION}');

            -- MOD() is used to prevent truly random relationships from being created, as this overwhelms WMR
            -- See https://materialize.com/docs/sql/recursive-ctes/#queries-with-update-locality
            SELECT cron.schedule('insert-relationships', '1 seconds', 'INSERT INTO relationships (a,b) SELECT MOD({POSTGRES_RANGE_FUNCTION}::INTEGER, 10), {POSTGRES_RANGE_FUNCTION} FROM generate_series(1,2) ON CONFLICT (a, b) DO UPDATE SET incarnation = relationships.incarnation + 1');
            SELECT cron.schedule('update-relationships-incarnation', '1 seconds', 'UPDATE relationships SET incarnation = incarnation + 1 WHERE a = {POSTGRES_RANGE_FUNCTION} AND b = {POSTGRES_RANGE_FUNCTION}');

            SELECT cron.schedule('delete-relationships', '1 seconds', 'DELETE FROM relationships WHERE a = {POSTGRES_RANGE_FUNCTION} AND b = {POSTGRES_RANGE_FUNCTION}');

            > CREATE SECRET IF NOT EXISTS pg_password AS '{MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD}'

            > CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (
              HOST '{MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME}',
              DATABASE postgres,
              USER postgres,
              PASSWORD SECRET pg_password,
              SSL MODE 'require'
              )
            """
            )
        )

        c.testdrive(
            input=dedent(
                """
            > SET DATABASE=qa_canary_environment

            > CREATE SCHEMA IF NOT EXISTS public_table;

            # create the tables here because dbt creates it as materialized view, which will not allow inserts
            > CREATE TABLE IF NOT EXISTS public_table.table (c INT);
            > GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public_table.table TO "infra+qacanaryload@materialize.io"

            > CREATE SCHEMA IF NOT EXISTS public_loadgen;
            > CREATE TABLE IF NOT EXISTS public_loadgen.product_category (
                category_id INT,
                category_name VARCHAR(255) NOT NULL,
                category_description TEXT
              );
            > GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public_loadgen.product_category TO "infra+qacanaryload@materialize.io"

            > DELETE FROM public_loadgen.product_category;
            > INSERT INTO public_loadgen.product_category (category_id, category_name, category_description) VALUES
              (1, 'Soccer Balls', 'A variety of soccer balls for every type of play'),
              (2, 'Soccer Cleats', 'High-quality soccer cleats for all ages and skill levels'),
              (3, 'Goalkeeper Gear', 'Everything a goalkeeper needs to protect the net'),
              (4, 'Soccer Apparel', 'Soccer clothing for players and fans alike'),
              (5, 'Training Equipment', 'Tools and equipment to improve your soccer skills'),
              (6, 'Soccer Accessories', 'Accessories for every soccer player and fan'),
              (7, 'Referee Equipment', 'All the essentials for soccer referees'),
              (8, 'Soccer Books and Media', 'Books, DVDs, and online resources about soccer'),
              (9, 'Soccer Fan Gear', 'Show your team spirit with these fan favorites'),
              (10, 'Field Equipment', 'Everything you need to set up a soccer field');

            > CREATE TABLE IF NOT EXISTS public_loadgen.product (
                  product_id INT,
                  category_id INT,
                  product_name VARCHAR(255) NOT NULL,
                  product_description TEXT,
                  product_price DECIMAL(10, 2) NOT NULL
              );
            > GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public_loadgen.product TO "infra+qacanaryload@materialize.io";

            > DELETE FROM public_loadgen.product;
            > INSERT INTO public_loadgen.product (product_id, category_id, product_name, product_description, product_price) VALUES
              (1, 1, 'Pro League Match Soccer Ball', 'Professional standard match soccer ball', 59.99),
              (2, 1, 'Junior Training Soccer Ball', 'Perfect for young players learning the game', 19.99),
              (3, 1, 'Indoor Futsal Ball', 'Designed for indoor soccer games', 29.99),
              (4, 1, 'Mini Skill Development Ball', 'Small ball for skill development', 14.99),
              (5, 1, 'Classic Black and White Soccer Ball', 'Classic design soccer ball', 24.99),
              (6, 1, 'Thermal-Bonded Professional Soccer Ball', 'High-quality ball for professional play', 79.99),
              (7, 1, 'Glow in the Dark Soccer Ball', 'Soccer ball that glows in the dark', 34.99),
              (8, 1, 'Beach Soccer Ball', 'Soccer ball designed for beach play', 29.99),
              (9, 1, 'Vintage Leather Soccer Ball', 'Vintage style leather soccer ball', 49.99),
              (10, 1, 'Smart Soccer Ball with Bluetooth', 'Soccer ball with Bluetooth for tracking performance', 99.99),
              (11, 2, 'Men''s High-Performance Soccer Cleats', 'Designed for speed and control on the field', 89.99),
              (12, 2, 'Women''s Comfort Fit Soccer Cleats', 'Combines performance with comfort', 79.99),
              (13, 2, 'Kids'' Beginner Soccer Cleats', 'Perfect for young players starting out', 39.99),
              (14, 2, 'Pro Elite Soccer Cleats', 'Elite level soccer cleats for professional play', 129.99),
              (15, 2, 'Indoor Soccer Shoes', 'Soccer shoes designed for indoor play', 69.99),
              (16, 2, 'Turf Soccer Shoes', 'Soccer shoes designed for turf fields', 74.99),
              (17, 2, 'Lightweight Speed Soccer Cleats', 'Lightweight soccer cleats for maximum speed', 99.99),
              (18, 2, 'Classic Leather Soccer Cleats', 'Classic leather soccer cleats for comfort and durability', 79.99),
              (19, 2, 'Customizable Soccer Cleats', 'Soccer cleats that can be customized to your liking', 109.99),
              (20, 2, 'Eco-Friendly Soccer Cleats', 'Soccer cleats made from eco-friendly materials', 89.99),
              (21, 3, 'Pro Goalkeeper Gloves', 'Professional standard goalkeeper gloves', 59.99),
              (22, 3, 'Junior Goalkeeper Gloves', 'Goalkeeper gloves for young players', 29.99),
              (23, 3, 'Goalkeeper Jersey', 'Jersey for goalkeepers', 39.99),
              (24, 3, 'Padded Goalkeeper Shorts', 'Goalkeeper shorts with extra padding', 34.99),
              (25, 3, 'Goalkeeper Helmet', 'Protective helmet for goalkeepers', 49.99),
              (26, 3, 'Full-Length Goalkeeper Pants', 'Full-length pants for goalkeepers', 44.99),
              (27, 3, 'Goalkeeper Shin Guards', 'Shin guards designed for goalkeepers', 24.99),
              (28, 3, 'Finger Protection Goalkeeper Gloves', 'Goalkeeper gloves with finger protection', 64.99),
              (29, 3, 'Goalkeeper Training Equipment Set', 'Set of training equipment for goalkeepers', 99.99),
              (30, 3, 'Goalkeeper Face Mask', 'Protective face mask for goalkeepers', 54.99),
              (31, 4, 'Men''s Soccer Jersey', 'Comfortable and breathable soccer jersey for men', 39.99),
              (32, 4, 'Women''s Soccer Jersey', 'Stylish and comfortable soccer jersey for women', 39.99),
              (33, 4, 'Kids'' Soccer Jersey', 'Durable and comfortable soccer jersey for kids', 29.99),
              (34, 4, 'Soccer Training Tracksuit', 'Tracksuit for soccer training and warm-ups', 79.99),
              (35, 4, 'Soccer Shorts', 'Comfortable and breathable soccer shorts', 24.99),
              (36, 4, 'Soccer Socks', 'Socks designed for comfort and performance during soccer games', 9.99),
              (37, 4, 'Soccer Scarf', 'Show your team spirit with this soccer scarf', 14.99),
              (38, 4, 'Soccer Team Jacket', 'Stay warm with this stylish soccer team jacket', 69.99),
              (39, 4, 'Soccer Training T-Shirt', 'T-shirt designed for comfort during soccer training', 19.99),
              (40, 4, 'Soccer Rain Jacket', 'Stay dry during rainy soccer games with this rain jacket', 49.99),
              (41, 5, 'Speed and Agility Ladder', 'Improve your speed and agility with this training ladder', 29.99),
              (42, 5, 'Soccer Cones Set', 'Set of cones for soccer training exercises', 19.99),
              (43, 5, 'Soccer Training Net', 'Training net for practicing soccer shots', 79.99),
              (44, 5, 'Rebounder Net', 'Net for practicing soccer passes and shots', 99.99),
              (45, 5, 'Soccer Passing Arcs', 'Arcs for practicing soccer passes', 24.99),
              (46, 5, 'Resistance Parachute', 'Parachute for resistance training to improve speed', 29.99),
              (47, 5, 'Soccer Ball Juggling Trainer', 'Tool for practicing juggling the soccer ball', 14.99),
              (48, 5, 'Soccer Training Mannequins', 'Mannequins for practicing soccer free kicks', 99.99),
              (49, 5, 'Soccer Training Hurdles', 'Hurdles for soccer agility training', 34.99),
              (50, 5, 'Soccer Ball Pump', 'Pump for inflating soccer balls', 9.99),
              (51, 6, 'Soccer Ball Bag', 'Bag for carrying multiple soccer balls', 24.99),
              (52, 6, 'Soccer Water Bottle', 'Water bottle for staying hydrated during soccer games', 9.99),
              (53, 6, 'Soccer Captain Armband', 'Armband for the soccer team captain', 4.99),
              (54, 6, 'Soccer Whistle', 'Whistle for soccer coaches and referees', 4.99),
              (55, 6, 'Soccer Headband', 'Headband for keeping hair and sweat out of your eyes during soccer games', 9.99),
              (56, 6, 'Soccer Ball Air Pressure Gauge', 'Gauge for checking the air pressure of soccer balls', 14.99),
              (57, 6, 'Soccer Boot Bag', 'Bag for carrying soccer cleats', 19.99),
              (58, 6, 'Soccer Shin Guard Sleeves', 'Sleeves for holding shin guards in place', 9.99),
              (59, 6, 'Soccer Ball Keychain', 'Show your love for soccer with this soccer ball keychain', 4.99),
              (60, 6, 'Soccer Team Flag', 'Flag for showing your support for your favorite soccer team', 19.99),
              (61, 7, 'Referee Whistle', 'Whistle for soccer referees', 4.99),
              (62, 7, 'Referee Jersey', 'Official jersey for soccer referees', 29.99),
              (63, 7, 'Referee Shorts', 'Comfortable shorts for soccer referees', 24.99),
              (64, 7, 'Referee Socks', 'Official socks for soccer referees', 9.99),
              (65, 7, 'Referee Card Set (Red and Yellow)', 'Set of red and yellow cards for soccer referees', 4.99),
              (66, 7, 'Referee Flag Set', 'Set of flags for soccer referees', 14.99),
              (67, 7, 'Referee Watch with Stopwatch', 'Watch with stopwatch function for soccer referees', 49.99),
              (68, 7, 'Referee Notebook and Pen', 'Notebook and pen for soccer referees', 9.99),
              (69, 7, 'Referee Coin for Toss', 'Coin for the pre-game toss in soccer matches', 4.99),
              (70, 7, 'Referee Kit Bag', 'Bag for carrying referee equipment', 24.99),
              (71, 8, 'The History of Soccer Book', 'Book about the history of soccer', 19.99),
              (72, 8, 'Soccer Skills and Drills DVD', 'DVD with soccer skills and drills', 14.99),
              (73, 8, 'Famous Soccer Players Biography Collection', 'Collection of biographies of famous soccer players', 29.99),
              (74, 8, 'Soccer Tactics E-book', 'E-book about soccer tactics', 9.99),
              (75, 8, 'Women in Soccer Documentary', 'Documentary about the role of women in soccer', 14.99),
              (76, 8, 'Soccer for Beginners Audiobook', 'Audiobook for beginners learning about soccer', 19.99),
              (77, 8, 'Soccer Coaching Online Course', 'Online course about soccer coaching', 99.99),
              (78, 8, 'Soccer Fitness Training Video', 'Training video about soccer fitness', 14.99),
              (79, 8, 'Soccer Refereeing Instructional Guide', 'Instructional guide about soccer refereeing', 19.99),
              (80, 8, 'World Cup Highlights Blu-ray', 'Blu-ray of World Cup highlights', 24.99),
              (81, 9, 'Team Scarves', 'Scarves featuring various soccer teams', 14.99),
              (82, 9, 'Team Jerseys', 'Jerseys of various soccer teams', 59.99),
              (83, 9, 'Team Flags', 'Flags featuring various soccer teams', 19.99),
              (84, 9, 'Team Mugs', 'Mugs featuring various soccer teams', 9.99),
              (85, 9, 'Team Keychains', 'Keychains featuring various soccer teams', 4.99),
              (86, 9, 'Team Posters', 'Posters of various soccer teams', 14.99),
              (87, 9, 'Team Stickers', 'Stickers featuring various soccer teams', 2.99),
              (88, 9, 'Team Backpacks', 'Backpacks featuring various soccer teams', 39.99),
              (89, 9, 'Team Caps', 'Caps featuring various soccer teams', 19.99),
              (90, 9, 'Team Wristbands', 'Wristbands featuring various soccer teams', 4.99),
              (91, 10, 'Portable Soccer Goals', 'Portable goals for soccer games', 99.99),
              (92, 10, 'Corner Flags', 'Flags for marking the corners of a soccer field', 29.99),
              (93, 10, 'Line Marking Machine', 'Machine for marking lines on a soccer field', 149.99),
              (94, 10, 'Field Maintenance Kit', 'Kit for maintaining a soccer field', 79.99),
              (95, 10, 'Soccer Goal Nets', 'Nets for soccer goals', 49.99),
              (96, 10, 'Soccer Goal Wheels', 'Wheels for moving soccer goals', 59.99),
              (97, 10, 'Soccer Goal Anchors', 'Anchors for securing soccer goals', 19.99),
              (98, 10, 'Field Divider', 'Divider for splitting a soccer field into sections', 39.99),
              (99, 10, 'Bench Canopy', 'Canopy for providing shade to the bench area', 99.99),
              (100, 10, 'Team Shelter', 'Shelter for the team during games', 199.99);
            """
            )
        )

    c.exec("dbt", "dbt", "run", "--threads", "8", workdir="/workdir")


def workflow_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up(Service("dbt", idle=True))

    con = psycopg.connect(
        host=MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME,
        user="postgres",
        dbname="postgres",
        password=MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD,
        sslmode="require",
    )
    try:
        with con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pg_replication_slots")
            result = cur.fetchall()
        assert (
            result[0][0] < 2
        ), f"""RDS Postgres has wrong number of pg_replication_slots {result[0][0]}, please fix manually to prevent Postgres from going out of disk from stalled Materialize connections:
$ psql postgres://postgres:$MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD@$MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME
postgres=> SELECT * FROM pg_replication_slots;
postgres=> SELECT pg_drop_replication_slot('...');
"""
    finally:
        con.close()

    c.exec("dbt", "dbt", "test", workdir="/workdir")


def workflow_clean(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "clean", workdir="/workdir")
