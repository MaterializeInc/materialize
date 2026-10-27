-- PRAGMA WARN_ON_MISSING_VARIABLES;
CREATE VIEW ambiguous AS
SELECT
    arr[:b] AS arr_slice,
    b
FROM (
    VALUES
        (LIST[10,20,30,40,50], 2),
        (LIST[5,6,7,8], 3),
        (LIST[100,200,300], 1)
) AS t(arr, b);
