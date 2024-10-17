WITH l1_detail AS (
    SELECT *
    FROM {{ ref('l1_order_detail') }}
),

l1_order AS (
    SELECT *
    FROM {{ ref('l1_order_header') }}
),

ELIMINATE_NULLS AS (
    SELECT 
        l1_detail.ORDER_DETAIL_ID,
        l1_detail.ORDER_ID,
        l1_detail.PRODUCT_ID,
        l1_detail.QUANTITY,
        l1_detail.UNIT_PRICE,
        l1_detail.TOTAL_PRICE,
        l1_detail.DISCOUNT_ID,
        l1_detail.ORDER_PRODUC_DISC_AMOUNT,
        l1_detail.LINE_NUMBER,
        l1_order.CUSTOMER_ID,
    FROM 
        l1_detail
        LEFT JOIN l1_order
        ON l1_detail.ORDER_ID = l1_order.ORDER_ID

    WHERE (PRODUCT_ID IS NOT NULL) AND (TRUCK_ID IS NOT NULL) AND (CUSTOMER_ID IS NOT NULL)
),

TRANSFORM_DETAIL AS (
    SELECT
        ORDER_DETAIL_ID,
        ORDER_ID,
        PRODUCT_ID ,
        QUANTITY,
        UNIT_PRICE,
        QUANTITY * UNIT_PRICE AS TOTAL_SALE,
    FROM ELIMINATE_NULLS
)

SELECT *
FROM TRANSFORM_DETAIL