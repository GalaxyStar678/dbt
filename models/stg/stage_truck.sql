WITH SRC1 AS (
    SELECT *
    FROM {{source ('pos','truck')}}
),

SRC2 AS (
    SELECT *
    FROM {{source ('pos','order_header')}}
),

SRC3 AS (
    SELECT *
    FROM {{source ('pos','order_detail')}}
),

SRC4 AS (
    SELECT *
    FROM {{source ('pos','menu')}}
),

CONECT_ORDER AS (
    SELECT
        SRC1.TRUCK_ID,
        SRC1.MAKE AS CAR_BRAND,
        SRC1.MODEL,
        SRC1.YEAR,
        SRC2.ORDER_ID,

    FROM 
        SRC1
        LEFT JOIN SRC2
        ON SRC1.TRUCK_ID = SRC2.TRUCK_ID
    
),

CONECT_ORDER_DETAIL AS (
    SELECT
        SC1.TRUCK_ID,
        SC1.CAR_BRAND,
        SC1.MODEL,
        SC1.YEAR,
        SC1.ORDER_ID,
        SRC3.MENU_ITEM_ID AS PRODUCT_ID,
    FROM 
        CONECT_ORDER SC1
        LEFT JOIN SRC3
        ON SC1.ORDER_ID = SRC3.ORDER_ID

),

TRANSFORMED_TRUCK AS (
    SELECT
        S1.TRUCK_ID,
        SRC4.TRUCK_BRAND_NAME,
        S1.CAR_BRAND,
        S1.MODEL,
        S1.YEAR,
        S1.ORDER_ID,
        S1.PRODUCT_ID,
    FROM 
        CONECT_ORDER_DETAIL S1
        LEFT JOIN SRC4
        ON S1.PRODUCT_ID = SRC4.MENU_ITEM_ID

)

SELECT *
FROM TRANSFORMED_TRUCK
