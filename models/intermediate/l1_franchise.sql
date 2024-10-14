WITH staging AS (
    SELECT *
    FROM {{ ref('stage_franchise') }}
),

DEDUPED AS (
    SELECT* 
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY FRANCHISE_ID ORDER BY OWNER_CITY) = 1
)

SELECT *
FROM DEDUPED