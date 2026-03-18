CREATE VIEW items AS SELECT 1 AS item_id, 'test'::text AS name;

CREATE PRIMARY KEY NOT ENFORCED items_pk ON items (item_id);
