{% snapshot orders_snapshot %}
    {{ config(
        target_schema='dbt',  
        target_database='supermarketsales',  
        unique_key='invoice_id', 
        strategy='check',  
        check_cols=['product_id', 'customer_id']
    ) }}

    SELECT * 
    FROM {{ ref('orders') }}  

{% endsnapshot %}

