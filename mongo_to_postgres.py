import pandas as pd
import pymongo
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import json
import numpy as np
from config import MONGO_URI, MONGO_DB_NAME, POSTGRES_DSN, POSTGRES_TABLE_NAME

# --- 1. 設定 Configuration ---
# 使用 config.py 中的配置
MONGO_STAFFS_COLLECTION = "staffs"
MONGO_TICKETS_COLLECTION = "tickets"

# --- 您提供的 JSON 檔案路徑 ---
# 注意：在實際生產環境，您會直接連線 MongoDB，而不是讀取檔案。
# 這裡使用檔案是為了模擬並確保程式碼能處理您的資料格式。
STAFFS_FILE_PATH = 'tg-portal.staffs.json'
TICKETS_FILE_PATH = 'tg-portal.tickets.json'


def extract_data_from_files():
    """
    從 JSON 檔案讀取資料。
    注意：這是為了模擬您的資料格式，生產環境應使用 extract_from_mongo()
    """
    print("Step 1: Extracting data from JSON files (Simulation Mode)...")
    
    with open(TICKETS_FILE_PATH, 'r') as f:
        tickets_list = json.load(f)
        
    with open(STAFFS_FILE_PATH, 'r') as f:
        staffs_list = json.load(f)

    if not tickets_list or not staffs_list:
        raise ValueError("Source data from JSON files is empty!")

    tickets_df = pd.DataFrame(tickets_list)
    staffs_df = pd.DataFrame(staffs_list)
    
    print(f"Extracted {len(tickets_df)} tickets and {len(staffs_df)} staffs.")
    return tickets_df, staffs_df

def extract_from_mongo():
    """從 MongoDB 擷取 staffs 和 tickets 資料 (生產環境使用)"""
    print("Step 1: Extracting data from MongoDB...")
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    
    tickets_cursor = db[MONGO_TICKETS_COLLECTION].find({})
    tickets_list = list(tickets_cursor)
    
    staffs_cursor = db[MONGO_STAFFS_COLLECTION].find({})
    staffs_list = list(staffs_cursor)
    
    client.close()
    
    if not tickets_list or not staffs_list:
        raise ValueError("Source data from MongoDB is empty!")

    # 轉換 ObjectId 為字串，避免 PostgreSQL 適配問題
    for ticket in tickets_list:
        if '_id' in ticket:
            ticket['_id'] = str(ticket['_id'])
    
    for staff in staffs_list:
        if '_id' in staff:
            staff['_id'] = str(staff['_id'])

    tickets_df = pd.DataFrame(tickets_list)
    staffs_df = pd.DataFrame(staffs_list)
    
    print(f"Extracted {len(tickets_df)} tickets and {len(staffs_df)} staffs.")
    return tickets_df, staffs_df

def get_value_from_bson(field):
    """從 BSON 物件 (如 $oid, $date, $numberLong) 中提取純量值"""
    if isinstance(field, dict):
        if '$oid' in field:
            return field['$oid']
        if '$date' in field:
            # 支援兩種日期格式
            if isinstance(field['$date'], dict) and '$numberLong' in field['$date']:
                 return pd.to_datetime(int(field['$date']['$numberLong']), unit='ms')
            return pd.to_datetime(field['$date'])
        if '$numberLong' in field:
            return int(field['$numberLong'])
    return field

def process_ticket_history(history):
    """從 activity_history 處理並提取關鍵資訊"""
    if not isinstance(history, list) or not history:
        return None, None, None, None

    # 排序確保事件順序正確
    history.sort(key=lambda x: get_value_from_bson(x.get('created_at')), reverse=True)
    
    last_event = history[0]
    first_event = history[-1]

    # 1. 取得目前的 staff_id (從最新到最舊找第一個有效的 staff_id)
    current_staff_id = None
    for event in history:
        staff_id = get_value_from_bson(event.get('staff_id'))
        if staff_id is not None:
            current_staff_id = int(staff_id)
            break

    # 2. 取得目前的狀態
    current_status = get_value_from_bson(last_event.get('status'))

    # 3. 取得建立時間
    created_at = get_value_from_bson(first_event.get('created_at'))

    # 4. 取得關閉時間
    closed_at = None
    closed_events = [e for e in history if get_value_from_bson(e.get('status')) == 'closed']
    if closed_events:
        # 關閉時間取最新的一筆
        closed_at = get_value_from_bson(closed_events[0].get('created_at'))

    return current_staff_id, current_status, created_at, closed_at


def transform_data(tickets_df, staffs_df):
    """轉換資料、進行合併與計算"""
    print("Step 2: Transforming data...")

    # --- 處理 Staffs DataFrame ---
    # 提取 staff_id, 處理 int 和 $numberLong 兩種格式
    staffs_df['staff_id'] = staffs_df['staff_id'].apply(get_value_from_bson).astype('Int64')
    staffs_df['mongo_staff_id'] = staffs_df['_id'].apply(get_value_from_bson)
    staffs_clean_df = staffs_df[['staff_id', 'mongo_staff_id', 'name', 'role']].copy()
    staffs_clean_df.rename(columns={'name': 'staff_name', 'role': 'staff_department'}, inplace=True)
    
    # 去除 staff_id 重複的資料，保留第一個
    staffs_clean_df = staffs_clean_df.drop_duplicates(subset=['staff_id'], keep='first')

    # --- 處理 Tickets DataFrame ---
    tickets_df['mongo_ticket_id'] = tickets_df['_id'].apply(get_value_from_bson)
    tickets_df['ticket_id'] = tickets_df['ticket_id'].apply(get_value_from_bson).astype('Int64')

    # 從 activity_history 提取資訊
    history_data = tickets_df['activity_history'].apply(process_ticket_history)
    tickets_df[['current_staff_id', 'current_status', 'ticket_created_at', 'ticket_closed_at']] = pd.DataFrame(history_data.tolist(), index=tickets_df.index)

    # 如果頂層 status 欄位更可靠，可以使用它
    # tickets_df['current_status'] = tickets_df['status']

    # --- 合併 (JOIN) tickets 和 staffs ---
    merged_df = pd.merge(
        tickets_df,
        staffs_clean_df,
        left_on='current_staff_id',
        right_on='staff_id',
        how='left' # 保留所有 tickets，即使找不到對應的 staff
    )

    # --- 計算衍生指標 ---
    # 計算處理時長 (分鐘)
    merged_df['handle_duration_mins'] = (merged_df['ticket_closed_at'] - merged_df['ticket_created_at']).dt.total_seconds() / 60
    # 將負數或無窮大的時長設為空值
    merged_df['handle_duration_mins'] = merged_df['handle_duration_mins'].apply(lambda x: x if x >= 0 and pd.notna(x) else np.nan)


    # --- 整理成最終需要的欄位 ---
    output_columns = [
        'mongo_ticket_id', 'ticket_id', 'subject', 'ticket_type', 'current_status', 
        'ticket_created_at', 'ticket_closed_at', 'current_staff_id', 
        'staff_name', 'staff_department', 'handle_duration_mins'
    ]
    final_df = merged_df[output_columns].copy()
    
    # 將 NaN (Not a Number) 轉換為 None，以符合資料庫的 NULL
    final_df = final_df.where(pd.notna(final_df), None)
    
    # 轉換 numpy 類型為 Python 原生類型，避免 PostgreSQL 適配問題
    for column in final_df.columns:
        if final_df[column].dtype == 'int64':
            final_df[column] = final_df[column].astype('Int64')  # pandas nullable integer
        elif final_df[column].dtype == 'float64':
            final_df[column] = final_df[column].astype('float')
        elif final_df[column].dtype == 'object':
            # 對於 object 類型，確保是字串或 None
            final_df[column] = final_df[column].astype('string')
    
    # 將 pandas 的 Int64 類型轉換為 Python 原生類型
    final_df = final_df.astype(object)
    
    # 處理 None 值，確保不會有 numpy.nan
    final_df = final_df.replace({np.nan: None})
    
    # 清理字串中的 NUL 字符和其他不可見字符
    for column in final_df.columns:
        if final_df[column].dtype == 'string' or final_df[column].dtype == 'object':
            final_df[column] = final_df[column].astype(str).str.replace('\x00', '', regex=False)
            final_df[column] = final_df[column].str.replace('\u0000', '', regex=False)
            # 移除其他控制字符
            final_df[column] = final_df[column].str.replace(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', regex=True)
            # 將空字串轉換為 None
            final_df[column] = final_df[column].replace('', None)
    
    # 最終類型轉換，確保所有資料都是安全的
    final_df = final_df.astype(object)
    
    # 最後檢查，將任何包含 NUL 的資料設為 None
    for column in final_df.columns:
        if final_df[column].dtype == 'object':
            final_df[column] = final_df[column].apply(lambda x: None if isinstance(x, str) and '\x00' in x else x)
    
    # 將字串 "None" 轉換為真正的 None 值
    for column in final_df.columns:
        if final_df[column].dtype == 'object':
            final_df[column] = final_df[column].replace('None', None)
            final_df[column] = final_df[column].replace('null', None)
            final_df[column] = final_df[column].replace('NULL', None)
            final_df[column] = final_df[column].replace('', None)
    
    # 確保時間欄位是正確的格式
    timestamp_columns = ['ticket_created_at', 'ticket_closed_at']
    for col in timestamp_columns:
        if col in final_df.columns:
            # 將無效的時間值設為 None
            final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
            # 將 NaT (Not a Time) 轉換為 None
            final_df[col] = final_df[col].replace({pd.NaT: None})

    print(f"Transformation complete. Final dataset has {len(final_df)} rows.")
    return final_df

def load_to_postgres(df):
    """將處理好的資料載入到 PostgreSQL"""
    print("Step 3: Loading data into PostgreSQL...")

    conn = None
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        cur = conn.cursor()

        # --- 建立目標表格 (如果不存在) ---
        cur.execute(f"""
            DROP TABLE IF EXISTS {POSTGRES_TABLE_NAME};
            CREATE TABLE {POSTGRES_TABLE_NAME} (
                mongo_ticket_id VARCHAR(50) PRIMARY KEY,
                ticket_id BIGINT,
                subject TEXT,
                ticket_type VARCHAR(100),
                current_status VARCHAR(50),
                ticket_created_at TIMESTAMP,
                ticket_closed_at TIMESTAMP,
                current_staff_id BIGINT,
                staff_name VARCHAR(100),
                staff_department VARCHAR(100),
                handle_duration_mins NUMERIC
            );
        """)
        
        # --- 使用 execute_values 高效寫入資料 ---
        data_tuples = [tuple(row) for row in df.itertuples(index=False)]
        
        cols = ','.join(list(df.columns))
        insert_query = f"INSERT INTO {POSTGRES_TABLE_NAME} ({cols}) VALUES %s"
        
        execute_values(cur, insert_query, data_tuples)
        
        conn.commit()
        print(f"Successfully loaded {len(df)} rows into {POSTGRES_TABLE_NAME}.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

# --- 主程式執行區 ---
if __name__ == "__main__":
    try:
        start_time = datetime.now()
        print(f"ETL process started at {start_time}")
        
        # 1. Extract
        # 在生產環境中，請註解掉下面這行，並取消註解 extract_from_mongo()
        # tickets_df, staffs_df = extract_data_from_files()
        tickets_df, staffs_df = extract_from_mongo()
        
        # 2. Transform
        final_data = transform_data(tickets_df, staffs_df)
        
        # 3. Load
        load_to_postgres(final_data)
        
        end_time = datetime.now()
        print(f"ETL process finished at {end_time}")
        print(f"Total duration: {end_time - start_time}")

    except Exception as e:
        print(f"ETL process failed: {e}")