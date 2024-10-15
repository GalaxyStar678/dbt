WITH staging AS (
    SELECT *
    FROM {{ ref('stage_customer') }}
),

DEDUPED AS (
    SELECT* 
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY CUSTOMER_ID ORDER BY CITY) = 1
)

SELECT *
FROM DEDUPED