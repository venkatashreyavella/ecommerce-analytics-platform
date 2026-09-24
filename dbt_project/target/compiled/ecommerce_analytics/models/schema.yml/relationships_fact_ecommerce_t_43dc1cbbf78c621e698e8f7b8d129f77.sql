
    
    

with child as (
    select product_id as from_field
    from "warehouse"."analytics"."fact_ecommerce_transactions"
    where product_id is not null
),

parent as (
    select product_id as to_field
    from "warehouse"."analytics"."dim_products"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


