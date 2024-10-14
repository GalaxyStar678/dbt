WITH SRC1 AS (
    SELECT *
    FROM {{source ('pos','truck')}}
),

TRANSFORMED_TRUCK AS (
    SELECT
        TRUCK_ID,
        MAKE AS TRUCK_BRAND_NAME,
        MAKE AS CAR_BRAND,
        MODEL,
        YEAR,
    FROM SRC1
)

SELECT *
FROM TRANSFORMED_TRUCK