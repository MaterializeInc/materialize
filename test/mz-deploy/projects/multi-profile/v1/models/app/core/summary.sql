CREATE MATERIALIZED VIEW summary
    IN CLUSTER compute
    AS
    SELECT id, :'env_label' AS env FROM app.public.my_table;
