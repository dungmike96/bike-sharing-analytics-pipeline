{{ config(
    materialized='table'
) }}

WITH raw_orders AS (
    SELECT * FROM {{ source('rawdata', 'raw_orders_all') }}
    WHERE pickup_city IS NOT NULL
      -- 🌟 TỐI ƯU GỐC: Chỉ quét và lấy dữ liệu của hai khu vực này
      AND (LOWER(pickup_city) LIKE '%đà nẵng%' OR LOWER(pickup_city) LIKE '%quảng nam%')
),

raw_mapping AS (
    SELECT * FROM {{ source('rawdata', 'raw_mapping_all') }}
),

renamed_and_casted AS (
    SELECT
        o.order_id,
        o.sap_contract_type,
        o.sap_id AS sap_id,
        o.status,
        o.service_type,
        o.service_name,
        o.payment_method,
        -- 🌟 GỘP CHUNG: Biến đổi tất cả các biến thể chữ hoa/thường của Quảng Nam và Đà Nẵng thành "Thành Phố Đà Nẵng"
        CASE 
            WHEN LOWER(o.pickup_city) LIKE '%quảng nam%' THEN 'Thành Phố Đà Nẵng'
            WHEN LOWER(o.pickup_city) LIKE '%đà nẵng%' THEN 'Thành Phố Đà Nẵng'
            ELSE 'Thành Phố Đà Nẵng' -- Đảm bảo chặn mọi trường hợp ngoại lệ lọt lưới
        END AS pickup_city,
        o.pickup_address,
        o.dropoff_address,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.create_time) AS created_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.order_time) AS ordered_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.completed_time) AS completed_at,
        CAST(o.total_fee_display AS NUMERIC) AS gmv_raw,         
        CAST(o.total_pay_display AS NUMERIC) AS real_pay_raw
    FROM raw_orders o
)

SELECT 
    o.*,
    m.platform,
    m.discount,
    m.tip_amount,
    m.cancel_key,
    m.cancel_by,
    m.cancel_reason,
    m.customer_phone_number 
FROM renamed_and_casted o
LEFT JOIN raw_mapping m ON o.order_id = m.order_id