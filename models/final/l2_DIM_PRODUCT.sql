WITH l1_product AS (
    SELECT *
    FROM {{ ref('l1_product') }}
),

TRANSFORM_PRODUCT AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_CATEGORY,
        PRODUCT_SUBCATEGORY,
        SALE_PRICE_USD - COST_GOODS_USD AS MARGINAL_COST ,
    FROM l1_product
)

SELECT *
FROM TRANSFORM_PRODUCT