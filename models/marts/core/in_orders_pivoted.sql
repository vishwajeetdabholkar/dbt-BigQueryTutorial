with payments as (

    select * from {{ ref('stg_payments')}}
),

pivoted as (
    select 
     order_id,
     {% set payment_methods = ['bank_transfer','credit_card','coupon','gift_card'] %}
     {% for payment_method in payment_methods %}

     {% if not loop.last%}
     sum(case when payment_method = {{ payment_method }} then amount else 0 end ) as {{payment_method}}_amount,
     {% endif %}
     sum(case when payment_method = {{ payment_method }} then amount else 0 end ) as {{payment_method}}_amount 
     {% endfor % }
     from payments
     where status = 'success'
     group by order_id
)

select * from pivoted;