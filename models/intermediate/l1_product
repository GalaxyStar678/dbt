WITH staging AS (
    SELECT *
    FROM {{ ref('stage_product') }}
),

DEDUPED AS (
    SELECT* 
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY PRODUCT_ID ORDER BY OWNER_CITY) = 1
)

SELECT *
FROM DEDUPED