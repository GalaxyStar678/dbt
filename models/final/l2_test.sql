WITH S1 AS (
    SELECT *
    FROM {{ ref('l2_DIM_CUSTOMER') }}
),

S2 AS (
    SELECT *
    FROM {{ ref('l2_FCT_ORDER_DETAIL') }}
),

S3 AS (
    SELECT *
    FROM {{ ref('l1_order_header') }}
),

TEST_CUSTOMER AS (
    SELECT *
    FROM 
        S2
        LEFT JOIN S3
        ON S2.ORDER_ID = S3.ORDER_ID
)

SELECT *
FROM TEST_CUSTOMER