WITH staging AS (
    SELECT *
    FROM {{ ref('stage_truck') }}
),

DEDUPED AS (
    SELECT* 
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY TRUCK_ID ORDER BY ORDER_ID) = 1
),

EDIT_FORD AS (
    SELECT 
        TRUCK_ID,
        TRUCK_BRAND_NAME,
        REPLACE(CAR_BRAND, 'Ford_', 'Ford') AS CAR_BRAND,
        MODEL,
        YEAR,
    FROM DEDUPED
)

SELECT *
FROM EDIT_FORD