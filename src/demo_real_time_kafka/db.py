# os 讀取環境變數；datetime 轉換事件時間。
import os
from datetime import datetime

# psycopg 提供 PostgreSQL 連線，Jsonb 用來傳送 JSONB 欄位。
import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from .event import validate_order

# 讀取專案根目錄的 .env；已存在的環境變數會優先保留。
load_dotenv()


def get_connection() -> psycopg.Connection[object]:
    # 用環境設定連線；預設值對應本機 Docker Compose 開發服務。
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "orders"),
        user=os.environ.get("POSTGRES_USER", "de_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "local_dev_only"),
    )


def insert_order(order: dict[str, object]) -> bool:
    # 資料庫是另一個輸入邊界，因此寫入前再次驗證。
    validate_order(order)
    # PostgreSQL TIMESTAMPTZ 接收具時區的 datetime。
    event_time = datetime.fromisoformat(str(order["event_time"]).replace("Z", "+00:00"))

    with get_connection() as connection:
        with connection.cursor() as cursor:
            # 唯一鍵衝突時不重複插入；RETURNING 只在新 row 插入時回傳 ID。
            cursor.execute(
                """
                INSERT INTO orders
                    (order_id, event_time, customer_id, items, amount, currency)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                RETURNING order_id
                """,
                # psycopg 會把這些值安全地綁定到 SQL 參數，不拼接 SQL 字串。
                (
                    order["order_id"],
                    event_time,
                    order["customer_id"],
                    Jsonb(order["items"]),
                    order["amount"],
                    order["currency"],
                ),
            )
            # 有回傳 row 代表新資料已插入；沒有則代表訂單 ID 已存在。
            return cursor.fetchone() is not None
