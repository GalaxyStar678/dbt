WITH staging AS (
    SELECT *
    FROM {{ ref('stage_truck') }}
),

EDIT_FORD AS (
    SELECT 
        TRUCK_ID,
        REPLACE(CAR_BRAND, 'Ford_', 'Ford') AS CAR_BRAND,
        MODEL,
        YEAR,
        MENU_TYPE_ID,
        TRUCK_OPENING_DATE,
    FROM staging
)

SELECT *
FROM EDIT_FORD