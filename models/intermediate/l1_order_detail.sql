WITH staging AS (
    SELECT *
    FROM {{ ref('stage_order_detail') }}
),

DEDUPED AS (
    SELECT
        ORDER_DETAIL_ID,
        ORDER_ID,
        PRODUCT_ID,
        QUANTITY,
        UNIT_PRICE,
        TOTAL_PRICE,
        DISCOUNT_ID,
        ORDER_PRODUC_DISC_AMOUNT,
        LINE_NUMBER,
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY ORDER_DETAIL_ID ORDER BY ORDER_ID) = 1
)

SELECT *
FROM DEDUPED