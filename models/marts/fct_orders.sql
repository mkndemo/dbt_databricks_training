with customers as (
    select * from {{ ref('stg_jaffle_shop__raw_customers') }}
),
orders as (
    select * from {{ ref('stg_jaffle_shop__raw_orders') }}
),
customer_orders as (

    select
        customer_id,
        min(ordered_at) as first_order_date
    from orders
    group by 1
),
final as (
    select
        customers.customer_id,
        customers.full_name,
        customer_orders.first_order_date
    from customers
    left join customer_orders using (customer_id)
)
select * from final