WITH l1_mod AS (
    SELECT *
    FROM {{ ref('l1_customer') }}
),

DEDUPED AS (
    SELECT* 
    FROM l1_mod
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY CUSTOMER_ID ORDER BY CUSTOMER_CITY) = 1
)

SELECT *
FROM DEDUPED