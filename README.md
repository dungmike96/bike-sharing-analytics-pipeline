# 🚲 Thống Kê & Tối Ưu Hóa Vận Hành Hệ Thống Xe Máy Điện (End-to-End Analytics Engineering Pipeline)

## 1. Tổng Overview Dự Án
Dự án này xây dựng một hệ thống pipeline dữ liệu tự động hóa hoàn toàn từ khâu **Nạp dữ liệu thô (Data Ingestion)**, **Biến đổi dữ liệu (Data Transformation)** cho đến **Trực quan hóa (Data Visualization)** nhằm phân tích hiệu suất vận hành, dòng tiền (GMV, Doanh thu thuần) và lý do hủy chuyến của dịch vụ xe máy điện thông minh.
## 📂 Cấu Trúc Thư Mục Dự Án (Project Structure)

```text
D:\Project\
├── dags/                         # Thư mục chứa file code Python điều phối (Airflow DAGs)
│   └── driver_supply_pipeline.py
├── data/                         # Thư mục lưu trữ các tệp tin dữ liệu thô (.csv)
│   ├── raw_orders/               # Bỏ các file order gốc vào đây
│   ├── raw_mapping/              # Bỏ các file order cần mapping vào đây
│   ├── driver_info/              # Bỏ file thông tin tài xế vào đây
│   ├── shift_data/               # Bỏ file phân ca sáng trưa tối khuya vào đây
│   ├── shift_registration/       # Bỏ file đăng ký ca của tài xế vào đây
│   └── hex_code/                 # Bỏ file mã hex vào đây
└── bike_sharing_dbt/             # Thư mục chứa toàn bộ dự án dbt Core (Transformation)
    ├── models/
    │   ├── staging/
    │   └── marts/
    └── dbt_project.yml

## 2. Kiến Trúc Hệ Thống (Data Architecture)
Hệ thống pipeline được vận hành qua các công nghệ cốt lõi bao gồm:
1. **Dữ liệu thô (Raw Data):** Các tệp tin CSV ghi nhận lịch sử chuyến xe và thông tin mapping vận hành lưu tại hệ thống local.
2. **Luồng điều phối (Orchestration - Apache Airflow):** Tự động hóa việc quét thư mục, xử lý dọn dẹp dữ liệu theo Chunk bằng `Pandas` và nạp dữ liệu an toàn lên Google BigQuery dưới dạng `JSON Lines` để tối ưu RAM và tránh lỗi ép kiểu nhị phân.
3. **Kho dữ liệu (Data Warehouse - Google BigQuery):** Nơi lưu trữ tập trung dữ liệu tầng thô (Raw) và tầng sản xuất (Production).
4. **Biến đổi dữ liệu (Transformation - dbt Core):** Áp dụng mô hình thiết kế **Staging - Marts** để tách biệt khâu làm sạch/đổi tên cột và khâu tính toán chỉ số kinh doanh.
5. **Trực quan hóa (BI Dashboard - Power BI):** Kết nối trực tiếp vào BigQuery tầng Marts để vẽ biểu đồ theo dõi các chỉ số sức khỏe vận hành theo thời gian thực.
<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/17f51ca3-b758-4501-88d1-b778c23a177e" />

## 3. Các Bài Toán Kỹ Thuật Đã Giải Quyết (Technical Challenges & Fixes)
Trong quá trình xây dựng pipeline, dự án đã giải quyết thành công các bài toán tối ưu hạ tầng:
* **Lỗi Lệch Độ Dài Byte của PyArrow:** Khắc phục triệt để lỗi `ArrowInvalid (Got bytestring of length 8, expected 16)` trên môi trường ảo `PythonVirtualenvOperator` bằng cách chuyển dịch chiến thuật nạp dữ liệu từ định dạng DataFrame trực tiếp sang `Newline Delimited JSON` thông qua API `load_table_from_json`.
* **Mất mát dòng tiền (Dữ liệu bị gán về 0.0):** Xử lý bẫy logic ép kiểu của Pandas khi đọc chuỗi thô dính khoảng trắng ẩn (`" 24000 "`) bằng cách cô lập luồng vào DataFrame sạch độc lập và cạo sạch ký tự rác/khoảng trắng trước khi đưa vào hàm tính toán.
* **Tối ưu chi phí truy vấn BigQuery:** Chuyển đổi toàn bộ các câu lệnh quét diện rộng (`SELECT *`) sang gọi đích danh các cột phân cụm và tận dụng tab Preview, giúp giảm dung lượng quét từ **1.87 GB xuống mức thấp nhất (vài MB)**.

## 4. Mô Hình Dữ Liệu & Chỉ Số Kinh Doanh (Data Modeling & Metrics)
Dữ liệu được tổ chức theo cấu trúc hình sao (Star Schema) tại tầng Marts thông qua bảng trung tâm `fact_bike_orders.sql`:
* **GMV (Gross Merchandise Volume):** Tổng phí dịch vụ thô của cuốc xe khi chưa áp khuyến mãi (`total_fee_display`).
* **Discount Amount:** Số tiền công ty trợ giá / khuyến mãi: `total_fee_display - total_pay_display`.
* **Net Revenue (Doanh thu thuần):** Dòng tiền thực tế giữ lại sau khi tối ưu chi phí.
* **Trip per day (Số chuyến xe trong ngày):** Số chuyến trung bình của tài xế hoạt động trong ngày.
## 5. Kết Quả & Dashboard

### Trực quan hóa tổng quan
<img width="4150" height="2400" alt="GSM-images-0" src="https://github.com/user-attachments/assets/d4a52235-b94e-41eb-8344-927b0763f573" />
<img width="4150" height="2400" alt="GSM-images-1" src="https://github.com/user-attachments/assets/4fc37232-7090-458c-9e2c-b24b0642ed14" />
<img width="4150" height="2400" alt="GSM-images-2" src="https://github.com/user-attachments/assets/b104d728-4ec6-47e7-aaab-fea9a7db35ca" />


###  Trải nghiệm tương tác thực tế
👉 Bạn có thể bấm vào [**Link Báo Cáo Tương Tác Của Power BI Tại Đây**]
(https://app.powerbi.com/view?r=eyJrIjoiZGM5MDlkMzEtZWRkMy00M2E0LTk1MDMtZDBkOGYyZWM0MjE3IiwidCI6ImNhYTU4MjFkLWFmMTctNDc2NC05MTM2LWE5NGRjZWVkNzM0ZiIsImMiOjEwfQ%3D%3D) để trực tiếp sử dụng các bộ lọc (Filter) theo Khung giờ, Tỉnh thành và xem dữ liệu vận hành chi tiết.
