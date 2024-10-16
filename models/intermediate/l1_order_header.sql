WITH staging AS (
    SELECT *
    FROM {{ ref('stage_order_header') }}
),

DEDUPED AS (
    SELECT
        ORDER_ID,
        ORDER_DATE,
        QUANTITY,
        TRUCK_ID,
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY ORDER_ID ORDER BY ORDER_DATE) = 1
)

SELECT *
FROM DEDUPED