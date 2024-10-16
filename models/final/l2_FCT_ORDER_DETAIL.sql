WITH l1_detail AS (
    SELECT *
    FROM {{ ref('l1_order_detail') }}
),

TRANSFORM_DETAIL AS (
    SELECT
        ORDER_DETAIL_ID,
        ORDER_ID,
        PRODUCT_ID ,
        QUANTITY,
        UNIT_PRICE,
        QUANTITY * UNIT_PRICE AS TOTAL_SALE,
    FROM l1_detail
)

SELECT *
FROM TRANSFORM_DETAIL