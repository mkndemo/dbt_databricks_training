with customers as (
    select * from {{ ref('stg_jaffle_shop__raw_customers') }}
),
orders as (
    select * from {{ ref('stg_jaffle_shop__raw_orders') }}
),
customer_orders as (

    select
        customer_id,
        order_total,
        min(ordered_at) as first_order_date
    from orders
    group by 1,2
),
final as (
    select
        customers.customer_id,
        customers.full_name,
        customer_orders.first_order_date,
        customer_orders.order_total,
        case
            when customer_orders.order_total > 40 then 'Gold'
            when customer_orders.order_total between 20 and 40 then 'Silver'
            else 'Bronze'
        end as customer_level
    from customers
    left join customer_orders using (customer_id)
)
select * from final