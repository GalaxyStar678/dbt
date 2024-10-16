WITH staging AS (
    SELECT *
    FROM {{ ref('stage_order_header') }}
),

DEDUPED AS (
    SELECT
        ORDER_ID,
        ORDER_DATE,
        QUANTITY,
        CHANNEL,
        CURRENCY,
        TOTAL_ORDER_AMOUNT,
        CUSTOMER_ID,
        DISCOUNT_ID,
        ORDER_DISCOUNT_AMOUNT,
        ORDER_TAX_AMOUNT,
        LOCATION_ID,
        TRUCK_ID,
        SHIFT_ID,
        SHIFT_START_TIME,
        SHIFT_END_TIME,
        SERVED_TS,
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY ORDER_ID ORDER BY ORDER_DATE) = 1
)

SELECT *
FROM DEDUPED