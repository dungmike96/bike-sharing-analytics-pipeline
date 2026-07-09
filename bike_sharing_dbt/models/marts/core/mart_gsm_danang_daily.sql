{{ config(
    materialized='table' 
) }}

WITH raw_orders AS (
    SELECT * FROM {{ source('rawdata', 'raw_orders_all') }}
    WHERE pickup_city IS NOT NULL
      -- 🌟 TỐI ƯU 1: Lọc thô ngay từ đầu để BigQuery quét ít dung lượng nhất, tiết kiệm bộ nhớ
      AND (LOWER(pickup_city) LIKE '%đà nẵng%' OR LOWER(pickup_city) LIKE '%quảng nam%')
),

raw_mapping AS (
    SELECT * FROM {{ source('rawdata', 'raw_mapping_all') }}
),

-- 1. CLEANING & FILTERING: Lọc riêng Đà Nẵng + Quảng Nam và chuẩn hóa tên thành phố
cleaned_data AS (
    SELECT 
        o.order_id,
        -- 🌟 TỐI ƯU 2: Dùng LOWER() đồng bộ cấu trúc đầu ra viết chuẩn chữ Hoa
        CASE 
            WHEN LOWER(o.pickup_city) LIKE '%quảng nam%' THEN 'Thành Phố Đà Nẵng'
            WHEN LOWER(o.pickup_city) LIKE '%đà nẵng%' THEN 'Thành Phố Đà Nẵng'
            ELSE o.pickup_city 
        END AS standardized_city,
        o.status,
	o.service_name,
        o.sap_id AS driver_id,
        m.customer_phone_number, 
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', o.create_time)) AS order_date,
        CAST(o.total_fee_display AS NUMERIC) AS gmv
    FROM raw_orders o
    LEFT JOIN raw_mapping m ON o.order_id = m.order_id
)

SELECT 
    order_date,
    standardized_city AS pickup_city,
    service_name,

    -- [Total Request]
    COUNT(DISTINCT order_id) AS total_requests,

    -- [Total Trip / Total COMPLETED]
    COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN order_id END) AS total_completed_trips,

    -- [%Orders_COMPLETED]
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN order_id END), 
        COUNT(DISTINCT order_id)
    ) AS pct_orders_completed,

    -- [%Orders_Canceled]
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN status = 'CANCELLED' THEN order_id END), 
        COUNT(DISTINCT order_id)
    ) AS pct_orders_canceled,

    -- [UCR - Unique Completed Request dựa trên SĐT khách hàng]
    COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN customer_phone_number END) AS unique_completed_requests,

    -- [GMV Tổng theo ngày]
    SUM(gmv) AS total_gmv,

    -- [Tính toán phục vụ chỉ số hiệu suất tài xế: TpD_Chuan_Orders]
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN order_id END),
        COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN driver_id END)
    ) AS tpd_chuan_orders, -- 🌟 ĐÃ SỬA: Thêm dấu phẩy hợp lệ ở đây để nối tiếp trường tiếp theo

    -- [AOV - Tính giá trị đơn hàng trung bình dựa trên các đơn hàng COMPLETED]
    SAFE_DIVIDE(
        SUM(CASE WHEN status = 'COMPLETED' THEN gmv ELSE 0 END),
        COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN order_id END)
    ) AS aov_completed

FROM cleaned_data
WHERE standardized_city = 'Thành Phố Đà Nẵng' 
GROUP BY 1, 2