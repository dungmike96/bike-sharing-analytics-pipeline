{{ config(
    materialized='view'
) }}

WITH source_orders AS (
    SELECT * FROM {{ source('rawdata', 'raw_orders_all') }}
),

renamed_and_casted AS (
    SELECT
        order_id,
        sap_contract_type,
        sap_id,
        status,
        service_type,
        service_name,
        payment_method,
        pickup_city,
        store_name,
        pickup_address,
        dropoff_address,

        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', create_time) AS created_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_time) AS ordered_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', completed_time) AS completed_at,

        -- 🌟 SỬA ĐOẠN NÀY: Hãy gọi đúng tên cột chứa giá tiền gốc
        total_fee_display,         
        
        -- Gọi đúng tên cột chứa số tiền thực trả 
        total_pay_display 

    FROM source_orders
)

SELECT * FROM renamed_and_casted