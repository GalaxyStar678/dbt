WITH SRC1 AS (
    SELECT *
    FROM {{ ref('l1_truck') }}
),

SRC2 AS (
    SELECT *
    FROM {{ ref('l1_product') }}
),

CONECT_MENU_TYPE AS (
    SELECT
        SRC1.TRUCK_ID,
        SRC1.CAR_BRAND,
        SRC1.MODEL,
        SRC1.YEAR,
        SRC1.TRUCK_OPENING_DATE,
        SRC2.MENU_TYPE_ID,
        SRC2.TRUCK_BRAND_NAME,

    FROM 
        SRC1
        LEFT JOIN SRC2
        ON SRC1.MENU_TYPE_ID = SRC2.MENU_TYPE_ID
    
),

DEDUPED AS (
    SELECT *
    FROM CONECT_MENU_TYPE,
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY TRUCK_ID ORDER BY TRUCK_OPENING_DATE) = 1
)

SELECT 
    TRUCK_ID,
    TRUCK_BRAND_NAME,
    CAR_BRAND,
    MODEL,
    YEAR,
FROM DEDUPED
