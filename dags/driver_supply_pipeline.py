import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator

# 1. CẤU HÌNH HỆ THỐNG
PROJECT_ID = 'bike-sharing-operation'     
DATASET_ID = 'rawdata'              

# 2. HÀM XỬ LÝ CHÍNH ĐỂ LOAD FILE THÔ LÊN THÀNH CÁC BẢNG RAW ĐỘC LẬP
def load_folder_to_bigquery_env(folder_name, table_name, project_id, dataset_id):
    import os
    import glob
    import pandas as pd
    import numpy as np
    from google.cloud import bigquery
    
    # Đọc key service account trực tiếp từ thư mục dags
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/opt/airflow/dags/google_key.json'
    
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    
    # Quét dữ liệu từ dags/data/...
    path = f'/opt/airflow/dags/data/{folder_name}/*.csv'
    all_files = glob.glob(path)
    
    if not all_files:
        print(f"❌ Không tìm thấy file CSV nào trong thư mục: /opt/airflow/dags/data/{folder_name}/")
        return
        
    print(f"🔍 Tìm thấy {len(all_files)} file. Tiến hành đọc xử lý theo Chunk để tiết kiệm RAM...")

    # Cấu hình nạp BigQuery: Lần đầu tiên ghi đè (WRITE_TRUNCATE), các chunk sau ghi nối tiếp (WRITE_APPEND)
    is_first_chunk = True

    # Định nghĩa cấu hình đổi tên cột để tái sử dụng
    column_mapping = {}
    if table_name == 'raw_orders_all':
        column_mapping = {
            'Order ID': 'order_id', 
            'Status': 'status', 
            'Service Type': 'service_type',
            'Service Name': 'service_name', 
            'Create Time': 'create_time', 
            'Order Time': 'order_time',
            'Completed Time': 'completed_time', 
            'Sap Contract Type': 'sap_contract_type', # Khớp chính xác 100% với file Excel
            'Sap Profile Id': 'sap_id',               # Khớp chính xác 100% với file Excel
            'Pickup City': 'pickup_city', 
            'Pick Up Address': 'pickup_address', 
            'Drop Off Address': 'dropoff_address',
            'Payment Method': 'payment_method', 
            'Total Fee Display': 'total_fee_display', 
            'Total Pay Display': 'total_pay_display',
            'Store Name': 'store_name'
        }
        bq_schema = [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("sap_contract_type", "STRING"),
            bigquery.SchemaField("sap_id", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("service_type", "STRING"),
            bigquery.SchemaField("service_name", "STRING"),
            bigquery.SchemaField("create_time", "STRING"),
            bigquery.SchemaField("order_time", "STRING"),
            bigquery.SchemaField("completed_time", "STRING"),
            bigquery.SchemaField("pickup_city", "STRING"),
            bigquery.SchemaField("pickup_address", "STRING"),
            bigquery.SchemaField("dropoff_address", "STRING"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("total_fee_display", "NUMERIC"),
            bigquery.SchemaField("total_pay_display", "NUMERIC"),
            bigquery.SchemaField("store_name", "STRING"),
        ]
    elif table_name == 'raw_mapping_all':
        column_mapping = {
            'Order ID': 'order_id', 
            'Platform': 'platform', 
            'Customer Phone Number': 'customer_phone_number',
            'Discount': 'discount', 
            'Tip Amount': 'tip_amount', 
            'Cancel Reason': 'cancel_reason',
            'Cancel Key': 'cancel_key',
            'Cancel By': 'cancel_by'
        }
        bq_schema = [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("customer_phone_number", "STRING"),
            bigquery.SchemaField("discount", "NUMERIC"),
            bigquery.SchemaField("tip_amount", "NUMERIC"),
            bigquery.SchemaField("cancel_reason", "STRING"),
            bigquery.SchemaField("cancel_key", "STRING"),
            bigquery.SchemaField("cancel_by", "STRING"),
        ]

    # Đọc từng file một
    for file_path in all_files:
        print(f"📖 Đang xử lý file: {os.path.basename(file_path)}")
        
        chunk_iter = pd.read_csv(
            file_path, 
            encoding='utf-8-sig', 
            chunksize=50000, 
            low_memory=False, 
            dtype=str  
        )
        
        for chunk in chunk_iter:
            # 1. Làm sạch khoảng trắng tiêu đề file thô
            chunk.columns = chunk.columns.str.strip()
            
            # 2. Xử lý Lọc trùng trên từng Chunk dựa trên cột gốc
            if 'Order ID' in chunk.columns:
                chunk = chunk.drop_duplicates(subset=['Order ID'], keep='first')
            else:
                chunk = chunk.drop_duplicates()
            
            # 3. Đổi tên cột chuẩn hóa sang snake_case
            chunk = chunk.rename(columns=column_mapping)
            
            # 4. TÁCH BIỆT LUỒNG: Tạo DataFrame mới để làm sạch dữ liệu an toàn
            clean_chunk = pd.DataFrame()
            numeric_cols = ['total_fee_display', 'total_pay_display', 'discount', 'tip_amount']
            
            for field in bq_schema:
                col = field.name
                
                if col in chunk.columns:
                    if col in numeric_cols:
                        clean_num = chunk[col].astype(str).str.strip().str.replace(',', '', regex=True)
                        clean_chunk[col] = pd.to_numeric(clean_num, errors='coerce').fillna(0.0)
                    else:
                        clean_chunk[col] = chunk[col].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
                        clean_chunk[col] = clean_chunk[col].replace(['nan', 'None', '<NA>'], '')
                else:
                    # Điền giá trị an toàn nếu cột không tồn tại
                    clean_chunk[col] = 0.0 if col in numeric_cols else ''
            
            # Sắp xếp đúng thứ tự cấu trúc schema yêu cầu
            target_cols = [field.name for field in bq_schema]
            chunk = clean_chunk[target_cols].copy()

            # Chuẩn hóa kiểu định dạng cuối cùng sang kiểu nguyên bản của Python trước khi cấu hình JSON
            for field in bq_schema:
                if field.field_type == "NUMERIC":
                    chunk[field.name] = chunk[field.name].astype(np.float64)
                else:
                    chunk[field.name] = chunk[field.name].fillna('').map(str).str.strip()

            # 5. Đẩy trực tiếp Chunk hiện tại lên BigQuery theo cấu trúc JSON Lines
            write_mode = "WRITE_TRUNCATE" if is_first_chunk else "WRITE_APPEND"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_mode,
                schema=bq_schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
                autodetect=False
            )
            
            chunk_json = chunk.to_dict(orient='records')
            
            job = client.load_table_from_json(chunk_json, table_ref, job_config=job_config)
            job.result()
            
            is_first_chunk = False

    print(f"🥇 Thành công nạp toàn bộ dữ liệu an toàn vào bảng {table_name}!")

# 3. KHỞI TẠO DAG
default_args = {
    'owner': 'dung_nguyen',
    'start_date': datetime(2026, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='bike_supply_chain_load_pipeline',
    default_args=default_args,
    schedule_interval=None,
    tags=['portfolio']
) as dag:

    REQUIREMENTS = ['pandas==1.3.5', 'google-cloud-bigquery==2.34.4', 'pyarrow==6.0.1', 'numpy==1.21.6']

    # Task 1: Load file Orders
    task_orders = PythonVirtualenvOperator(
        task_id='load_orders_all',
        python_callable=load_folder_to_bigquery_env,
        requirements=REQUIREMENTS,
        op_kwargs={'folder_name': 'raw_orders', 'table_name': 'raw_orders_all', 'project_id': PROJECT_ID, 'dataset_id': DATASET_ID}
    )

    # Task 2: Load file Mapping
    task_mapping = PythonVirtualenvOperator(
        task_id='load_mapping_all',
        python_callable=load_folder_to_bigquery_env,
        requirements=REQUIREMENTS,
        op_kwargs={'folder_name': 'raw_mapping', 'table_name': 'raw_mapping_all', 'project_id': PROJECT_ID, 'dataset_id': DATASET_ID}
    )

    [task_orders, task_mapping]