WITH staging AS (
    SELECT *
    FROM {{ ref('stage_truck') }}
),

DEDUPED AS (
    SELECT* 
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY TRUCK_ID ORDER BY TRUCK_OPENING_DATE) = 1
)

SELECT
FROM DEDUPED