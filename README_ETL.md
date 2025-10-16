# ETL 腳本使用說明

這個專案包含從 MongoDB 到 PostgreSQL 的 ETL (Extract, Transform, Load) 腳本。

## 📁 檔案結構

```
├── mongo_to_postgres.py    # 主要的 ETL 腳本
├── config.py               # 資料庫連線配置
├── test_connections.py     # 連線測試腳本
├── requirements.txt        # Python 依賴
└── README_ETL.md          # 本說明文件
```

## 🚀 快速開始

### 1. 安裝依賴

```bash
pip install -r requirements.txt
```

### 2. 配置資料庫連線

編輯 `config.py` 文件，設定正確的資料庫名稱：

```python
# 將 your_mongo_db 替換為你的實際 MongoDB 資料庫名稱
MONGO_DB_NAME = "your_actual_database_name"
```

### 3. 測試連線

```bash
python test_connections.py
```

### 4. 執行 ETL

```bash
python mongo_to_postgres.py
```

## ⚙️ 配置說明

### MongoDB 連線

- **Host**: localhost (Docker 容器)
- **Port**: 27017
- **Username**: metabase_user
- **Password**: StrongPassword123
- **Database**: 在 config.py 中設定

### PostgreSQL 連線

- **Host**: localhost (Docker 容器)
- **Port**: 5432
- **Database**: metabaseappdb
- **Username**: metabaseuser
- **Password**: your_strong_password

## 📊 資料流程

### 1. Extract (擷取)
- 從 MongoDB 的 `staffs` collection 擷取員工資料
- 從 MongoDB 的 `tickets` collection 擷取票據資料

### 2. Transform (轉換)
- 合併 staffs 和 tickets 資料
- 計算處理時長
- 清理和標準化資料格式

### 3. Load (載入)
- 將處理後的資料載入到 PostgreSQL 的 `tickets_analysis` 表格

## 🎯 目標表格結構

PostgreSQL 中的 `tickets_analysis` 表格包含以下欄位：

```sql
CREATE TABLE tickets_analysis (
    ticket_id VARCHAR(50) PRIMARY KEY,
    subject TEXT,
    status VARCHAR(50),
    priority VARCHAR(50),
    created_at TIMESTAMP,
    closed_at TIMESTAMP,
    staff_id VARCHAR(50),
    staff_name VARCHAR(100),
    staff_department VARCHAR(100),
    handle_duration_mins NUMERIC
);
```

## 🔧 自定義配置

### 環境變數

創建 `.env` 文件來覆蓋預設配置：

```bash
# .env
MONGO_DB_NAME=your_database_name
BATCH_SIZE=500
LOG_LEVEL=DEBUG
```

### 修改目標表格名稱

在 `config.py` 中修改：

```python
POSTGRES_TABLE_NAME = "your_custom_table_name"
```

## 🚨 故障排除

### 常見問題

#### 1. 連線失敗
```bash
# 檢查 Docker 服務狀態
docker-compose ps

# 測試連線
python test_connections.py
```

#### 2. 找不到 collections
- 確保 MongoDB 中有 `staffs` 和 `tickets` collections
- 檢查資料庫名稱是否正確

#### 3. 權限錯誤
- 確保 MongoDB 和 PostgreSQL 的用戶名和密碼正確
- 檢查 Docker 容器的網路設定

### 日誌和除錯

腳本會輸出詳細的執行日誌，包括：
- 每個步驟的執行狀態
- 資料處理的統計資訊
- 錯誤和警告訊息

## 📈 效能優化

### 批次處理
- 預設批次大小：1000 筆記錄
- 可在 config.py 中調整 `BATCH_SIZE`

### 記憶體管理
- 使用 pandas 進行高效資料處理
- 批次載入避免記憶體溢出

## 🔒 安全性注意事項

1. **密碼管理**: 不要在程式碼中硬編碼密碼
2. **網路安全**: 生產環境中應使用 SSL/TLS 連線
3. **權限控制**: 使用最小權限原則設定資料庫用戶

## 📝 開發說明

### 擴展功能

腳本設計為模組化結構，可以輕鬆擴展：
- 添加新的資料來源
- 實現更複雜的資料轉換邏輯
- 支援不同的目標資料庫

### 測試

```bash
# 測試連線
python test_connections.py

# 執行 ETL (會清空目標表格)
python mongo_to_postgres.py
```

## 📞 支援

如果遇到問題，請檢查：
1. Docker 服務狀態
2. 資料庫連線配置
3. 執行日誌輸出
4. 網路和防火牆設定 