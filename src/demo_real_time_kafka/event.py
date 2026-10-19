# UTC 時間處理及有限數值檢查會用到的標準函式庫。
from datetime import datetime, timezone
import math


def validate_order(payload: object) -> dict[str, object]:
    # 先確保訊息最外層是 JSON object，再檢查每個欄位。
    if not isinstance(payload, dict):
        raise ValueError("event must be a JSON object")

    # 一筆有效訂單必須包含以下全部欄位。
    required = ("order_id", "event_time", "customer_id", "items", "amount", "currency")
    missing = [field for field in required if field not in payload]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")

    # ID 和幣別等文字欄位不能是空字串或只有空白。
    for field in ("order_id", "customer_id", "currency"):
        value = payload[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string")

    # ISO 4217 貨幣代碼在這個練習中固定為三個字元，例如 HKD。
    if len(payload["currency"]) != 3:
        raise ValueError("currency must contain exactly 3 characters")

    # event_time 必須是字串，並能解析為帶時區的 ISO 8601 時間。
    event_time = payload["event_time"]
    if not isinstance(event_time, str):
        raise ValueError("event_time must be an ISO 8601 string")
    try:
        # Python 可將 UTC 時間尾端的 Z 解析成 +00:00。
        parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time must be a valid ISO 8601 timestamp") from exc
    if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
        raise ValueError("event_time must include a timezone")

    # 商品明細以 JSON array 表示。
    items = payload["items"]
    if not isinstance(items, list):
        raise ValueError("items must be a list")

    # 金額只接受有限的非負整數或浮點數。
    amount = payload["amount"]
    # bool 是 int 的子類別，所以要先排除 True/False。
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise ValueError("amount must be a number")
    if not math.isfinite(amount) or amount < 0:
        raise ValueError("amount must be a finite non-negative number")

    # 所有檢查通過後，原樣回傳事件供後續寫入。
    return payload


def make_order(sequence: int) -> dict[str, object]:
    # 依序號建立固定結構的模擬訂單，供 producer 發送。
    return {
        # 序號固定產生 ID，方便重播同一批資料來測試冪等性。
        "order_id": f"order-{sequence:06d}",
        # 使用 UTC 並帶時區，避免不同電腦對本地時間有不同解讀。
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"customer-{sequence % 100:03d}",
        # 商品資料以 list/dict 表示，之後會序列化成 JSON。
        "items": [
            {
                "sku": f"item-{sequence % 20:03d}",
                "quantity": 2,
                "unit_price": 19.90,
            }
        ],
        "amount": 39.80,
        "currency": "HKD",
    }
