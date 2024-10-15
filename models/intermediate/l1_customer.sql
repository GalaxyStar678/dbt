WITH staging AS (
    SELECT *
    FROM {{ ref('stage_customer') }}
),

CUSTOMERS AS (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME,
        CUSTOMER_COUNTRY,
        CUSTOMER_CITY,
        GENDER,
        REPLACE(CHILDREN_COUNT, 'Undisclosed', 0) AS CHILDREN_COUNT,
        EMAIL,
        PHONE_NUMBER,
    FROM staging
)

SELECT *
FROM CUSTOMERS
