WITH S1 AS (
    SELECT *
    FROM {{ ref('l1_customer') }}
),

S2 AS (
    SELECT *
    FROM {{ ref('l1_order_detail') }}
),

S3 AS (
    SELECT *
    FROM {{ ref('l1_order_header') }}
),

GET_ORDERS AS (
    SELECT
        S2.ORDER_DETAIL_ID,
        S2.ORDER_ID,
        S2.PRODUCT_ID,
        S2.QUANTITY,
        S2.UNIT_PRICE,
        S3.CUSTOMER_ID,
        S3.TRUCK_ID,

    FROM 
        S2
        LEFT JOIN S3
        ON S2.ORDER_ID = S3.ORDER_ID
),

GET_CUSTOMER AS (
    SELECT
        GET_ORDERS.ORDER_DETAIL_ID,
        GET_ORDERS.ORDER_ID,
        GET_ORDERS.PRODUCT_ID,
        GET_ORDERS.QUANTITY,
        GET_ORDERS.UNIT_PRICE,
        GET_ORDERS.CUSTOMER_ID,
        GET_ORDERS.TRUCK_ID,
        S1.CUSTOMER_ID AS CUSTOMER,

    FROM 
        GET_ORDERS
        LEFT JOIN S1
        ON GET_ORDERS.CUSTOMER_ID = S1.CUSTOMER_ID
    WHERE CUSTOMER IS NOT NULL
)

SELECT *
FROM GET_CUSTOMER