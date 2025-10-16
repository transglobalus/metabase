# -*- coding: utf-8 -*-
"""
資料庫連線配置檔案
根據 docker-compose.yml 的設定配置連線參數
"""

import os
from dotenv import load_dotenv

# 載入環境變數 (如果有的話)
load_dotenv()

# --- MongoDB 連線配置 ---
MONGO_CONFIG = {
    "host": "114.33.83.253",
    "port": 27017,
    "username": "pocuser",
    "password": "demopassword",
    "database": os.getenv("MONGO_DB_NAME", "your_mongo_db"),  # 從環境變數讀取，預設為 your_mongo_db
    "auth_source": "admin"  # MongoDB 認證資料庫
}

# MongoDB 連線字串
MONGO_URI = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/?authSource={MONGO_CONFIG['auth_source']}"

# 為了向後兼容，直接定義這些變數
MONGO_DB_NAME = MONGO_CONFIG['database']

# --- PostgreSQL 連線配置 ---
POSTGRES_CONFIG = {
    "host": "114.33.83.253",
    "port": 5432,
    "database": "metabaseappdb",
    "user": "pocuser",
    "password": "demopassword",  # 請替換為你的實際密碼
    "sslmode": "disable"  # 本地連線不需要 SSL
}

# PostgreSQL 連線字串
POSTGRES_DSN = f"dbname='{POSTGRES_CONFIG['database']}' user='{POSTGRES_CONFIG['user']}' password='{POSTGRES_CONFIG['password']}' host='{POSTGRES_CONFIG['host']}' port='{POSTGRES_CONFIG['port']}' sslmode='{POSTGRES_CONFIG['sslmode']}'"

# --- 其他配置 ---
POSTGRES_TABLE_NAME = "tickets_analysis"  # 目標表格名稱

# --- 連線測試配置 ---
CONNECTION_TIMEOUT = 10  # 連線超時時間 (秒)
MAX_RETRIES = 3  # 最大重試次數

# --- 資料處理配置 ---
BATCH_SIZE = 1000  # 批次處理大小
LOG_LEVEL = "INFO"  # 日誌等級 