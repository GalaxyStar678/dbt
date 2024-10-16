WITH staging AS (
    SELECT *
    FROM {{ ref('stage_product') }}
),

DEDUPED AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_CATEGORY,
        PRODUCT_SUBCATEGORY,
        MENU_TYPE_ID,
        MENU_TYPE,
        SALE_PRICE_USD,
        COST_GOODS_USD,
        TRUCK_BRAND_NAME,
    FROM staging
    QUALIFY ROW_NUMBER() 
    OVER (PARTITION BY PRODUCT_ID ORDER BY MENU_TYPE_ID) = 1
)

SELECT *
FROM DEDUPED