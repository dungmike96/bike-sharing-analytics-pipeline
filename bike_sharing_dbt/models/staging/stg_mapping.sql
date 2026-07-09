{{ config(
    materialized='view'
) }}

WITH source_mapping AS (
    SELECT * FROM {{ source('rawdata', 'raw_mapping_all') }}
),

clean_mapping AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        platform,
        customer_phone_number,
        CAST(discount AS NUMERIC) AS discount_amount,
        CAST(tip_amount AS NUMERIC) AS tip_amount,
        cancel_reason,
        cancel_key,
        cancel_by,
        -- 🌟 SỬA LỖI: Sử dụng cancel_reason làm tiêu chí sắp xếp lọc trùng an toàn
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY cancel_reason DESC) as rn

    FROM source_mapping
    QUALIFY rn = 1
)

SELECT * FROM clean_mapping