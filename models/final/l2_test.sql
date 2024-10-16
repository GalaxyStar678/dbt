WITH S1 AS (
    SELECT *
    FROM {{ ref('l2_DIM_CUSTOMER') }}
),

S2 AS (
    SELECT *
    FROM {{ ref('l2_FCT_ORDER_DETAIL') }}
),

TEST_CUSTOMER AS (
    SELECT *
    FROM 
        S2
        LEFT JOIN S1
        ON S2.CUSTOMER_ID = S1.CUSTOMER_ID
)

SELECT *
FROM TEST_CUSTOMER