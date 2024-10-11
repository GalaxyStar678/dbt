WITH SRC AS (
    SELECT *
    FROM {{source ('pos','franchise')}}
),

TRANSFORMED_FRANCHISE AS (
    SELECT
        FRANCHISE_ID,
        FIRST_NAME AS OWNER_FIRST_NAME,
        LAST_NAME AS OWNER_LAST_NAME,
        CITY AS OWNER_CITY,
        COUNTRY AS OWNER_COUNTRY,
        LOWER(E_MAIL) AS EMAIL,
        PHONE_NUMBER,
    FROM SCR
)

SELECT *
FROM TRANSFORMED_FRANCHISE