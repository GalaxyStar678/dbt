with child as (
    select 
        CUSTOMER_ID as from_field
    FROM {{ ref('l2_DIM_CUSTOMER') }}
    where from_field is not null
),

parent as (
    select CUSTOMER_ID as to_field
    FROM {{ ref('l2_FCT_ORDER_DETAIL') }}
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null