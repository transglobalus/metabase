#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
資料庫連線測試腳本
測試 MongoDB 和 PostgreSQL 的連線是否正常
"""

import sys
import os

# 添加配置檔案路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from config import MONGO_URI, MONGO_DB_NAME, POSTGRES_DSN
except ImportError:
    print("❌ 無法載入 config.py，請確保檔案存在")
    sys.exit(1)

def test_mongodb_connection():
    """測試 MongoDB 連線"""
    print("🔍 測試 MongoDB 連線...")
    
    try:
        import pymongo
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        
        # 測試連線
        client.admin.command('ping')
        print("✅ MongoDB 連線成功！")
        
        # 列出資料庫
        databases = client.list_database_names()
        print(f"📚 可用的資料庫: {databases}")
        
        # 檢查指定的資料庫是否存在
        if MONGO_DB_NAME in databases:
            print(f"✅ 資料庫 '{MONGO_DB_NAME}' 存在")
            
            # 列出 collections
            db = client[MONGO_DB_NAME]
            collections = db.list_collection_names()
            print(f"📁 Collections: {collections}")
            
            # 檢查是否有 staffs 和 tickets collections
            if 'staffs' in collections:
                staffs_count = db.staffs.count_documents({})
                print(f"👥 staffs collection: {staffs_count} 筆資料")
            else:
                print("⚠️  staffs collection 不存在")
                
            if 'tickets' in collections:
                tickets_count = db.tickets.count_documents({})
                print(f"🎫 tickets collection: {tickets_count} 筆資料")
            else:
                print("⚠️  tickets collection 不存在")
        else:
            print(f"❌ 資料庫 '{MONGO_DB_NAME}' 不存在")
            print("請在 config.py 中設定正確的 MONGO_DB_NAME")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ MongoDB 連線失敗: {e}")
        return False

def test_postgresql_connection():
    """測試 PostgreSQL 連線"""
    print("\n🔍 測試 PostgreSQL 連線...")
    
    try:
        import psycopg2
        conn = psycopg2.connect(POSTGRES_DSN)
        
        print("✅ PostgreSQL 連線成功！")
        
        # 檢查資料庫資訊
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"📊 PostgreSQL 版本: {version[0]}")
        
        # 檢查資料庫名稱
        cur.execute("SELECT current_database();")
        db_name = cur.fetchone()
        print(f"🗄️  當前資料庫: {db_name[0]}")
        
        # 檢查表格是否存在
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        print(f"📋 現有表格: {[table[0] for table in tables]}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL 連線失敗: {e}")
        return False

def main():
    """主函數"""
    print("🚀 開始測試資料庫連線...")
    print("=" * 50)
    
    # 顯示連線資訊
    print("📋 連線配置:")
    print(f"  MongoDB: {MONGO_URI}")
    print(f"  MongoDB DB: {MONGO_DB_NAME}")
    print(f"  PostgreSQL: {POSTGRES_DSN}")
    print("=" * 50)
    
    # 測試連線
    mongo_success = test_mongodb_connection()
    postgres_success = test_postgresql_connection()
    
    print("\n" + "=" * 50)
    print("📊 測試結果:")
    print(f"  MongoDB: {'✅ 成功' if mongo_success else '❌ 失敗'}")
    print(f"  PostgreSQL: {'✅ 成功' if postgres_success else '❌ 失敗'}")
    
    if mongo_success and postgres_success:
        print("\n🎉 所有資料庫連線測試通過！")
        print("現在可以執行 ETL 腳本了。")
    else:
        print("\n⚠️  部分資料庫連線測試失敗，請檢查配置。")
        print("常見問題:")
        print("  1. Docker 服務未啟動")
        print("  2. 端口被佔用")
        print("  3. 密碼錯誤")
        print("  4. 資料庫名稱錯誤")

if __name__ == "__main__":
    main() 