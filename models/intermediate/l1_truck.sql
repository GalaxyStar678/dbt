WITH staging AS (
    SELECT *
    FROM {{ ref('stage_truck') }}
),


EDIT_FORD AS (
    SELECT 
        TRUCK_ID,
        REPLACE(TRUCK_BRAND_NAME, 'Ford_', 'Ford') AS TRUCK_BRAND_NAME,
        REPLACE(CAR_BRAND, 'Ford_', 'Ford') AS CAR_BRAND,
        MODEL,
        YEAR,
    FROM staging
)

SELECT *
FROM EDIT_FORD