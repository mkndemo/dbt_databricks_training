select
    order_id,
    sum(order_total) as order_total
from {{ ref('stg_jaffle_shop__raw_orders') }}
group by 1
having order_total < 0