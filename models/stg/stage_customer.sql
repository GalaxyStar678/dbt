WITH SRC AS (
    SELECT *
    FROM {{source ('customer','customer_loyalty')}}
),

TRANSFORMED_CUSTOM AS (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME,
        COUNTRY,
        CITY,
        GENDER,
        LOWER(E_MAIL) AS EMAIL,
        PHONE_NUMBER,
    FROM SRC
)

SELECT *
FROM TRANSFORMED_CUSTOM