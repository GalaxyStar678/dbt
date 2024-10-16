WITH SRC AS (
    SELECT *
    FROM {{source ('pos','truck')}}
),

TRANSFORMED_PRODUCT AS (
    SELECT
        TRUCK_ID,
        YEAR,
        MODEL,
        MAKE AS CAR_BRAND,
        MENU_TYPE_ID,
        PRIMARY_CITY AS CITY,
        REGION,
        ISO_REGION AS ISO_REGION_CODE,
        COUNTRY,
        ISO_COUNTRY_CODE,
        FRANCHISE_ID,
        FRANCHISE_FLAG,
        EV_FLAG,
        TRUCK_OPENING_DATE,
    FROM SRC
)

SELECT *
FROM TRANSFORMED_PRODUCT