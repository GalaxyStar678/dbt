WITH SRC AS (
    SELECT *
    FROM {{source ('customer','customer_loyalty')}}
),

TRANSFORMED_CUSTOM AS (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        COUNTRY AS CUSTOMER_COUNTRY,
        CITY AS CUSTOMER_CITY,
        PREFERRED_LANGUAGE,
        FAVOURITE_BRAND AS FAV_BRAND,
        GENDER,
        MARITAL_STATUS,
        CHILDREN_COUNT,
        BIRTHDAY_DATE,
        LOWER(E_MAIL) AS EMAIL,
        PHONE_NUMBER,
        POSTAL_CODE,
        SIGN_UP_DATE,

    FROM SRC
)

SELECT *
FROM TRANSFORMED_CUSTOM