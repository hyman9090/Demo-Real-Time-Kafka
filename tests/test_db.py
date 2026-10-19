# 測試需要 UTC timestamp、UUID 唯一值，以及 package 中的資料庫函式。
from datetime import datetime, timezone
from uuid import uuid4

from demo_real_time_kafka.db import get_connection, insert_order


def test_insert_order_is_idempotent() -> None:
    # 使用唯一 ID，避免與其他測試或示範訂單重疊。
    order_id = f"order-test-{uuid4()}"
    order = {
        "order_id": order_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": "customer-test",
        "items": [{"sku": "item-test", "quantity": 1, "unit_price": 5.0}],
        "amount": 5.0,
        "currency": "HKD",
    }

    try:
        # 首次插入成功；相同 ID 再插入則由 primary key 防止重複。
        assert insert_order(order) is True
        assert insert_order(order) is False
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                result = cursor.fetchone()
                # 確認資料庫內同一個 order_id 只有一筆。
                assert result is not None
                assert result[0] == 1
    finally:
        # 測試後只清除本測試建立的那筆資料。
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM orders WHERE order_id = %s",
                    (order_id,),
                )
