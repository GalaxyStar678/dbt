WITH SRC AS (
    SELECT *
    FROM {{source ('pos','order_header')}}
),

TRANSFORMED_ORDER_HEADER AS (
    SELECT
        ORDER_ID,
        ORDER_TS AS ORDER_DATE,
        ORDER_AMOUNT AS QUANTITY,
        ORDER_CHANNEL AS CHANNEL,
        ORDER_CURRENCY AS CURRENCY,
        ORDER_TOTAL AS TOTAL_ORDER_AMOUNT,
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
    FROM SRC
)

SELECT *
FROM TRANSFORMED_ORDER_HEADER