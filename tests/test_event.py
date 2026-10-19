# deepcopy 用來避免測試修改共用資料；pytest 提供測試標記和錯誤檢查。
from copy import deepcopy

import pytest

from demo_real_time_kafka.event import make_order, validate_order


def valid_order() -> dict[str, object]:
    # 共用合法範例；各測試會複製或修改這份資料。
    return {
        "order_id": "order-test",
        "event_time": "2026-10-13T02:00:00+00:00",
        "customer_id": "customer-012",
        "items": [{"sku": "item-004", "quantity": 2, "unit_price": 19.9}],
        "amount": 39.8,
        "currency": "HKD",
    }


def test_valid_order_is_returned() -> None:
    # 合法訂單應成功通過驗證，並回傳相同物件。
    order = valid_order()
    assert validate_order(order) is order


# 將同一測試重複套用到每個必填欄位。
@pytest.mark.parametrize(
    "field",
    ["order_id", "event_time", "customer_id", "items", "amount", "currency"],
)
# 每個案例移除一個欄位，驗證缺少必填欄位會被拒絕。
def test_missing_required_field_is_rejected(field: str) -> None:
    order = valid_order()
    # 每次刪除一個必填欄位，確認驗證器指出缺漏。
    del order[field]
    with pytest.raises(ValueError, match=field):
        validate_order(order)


# 每個案例提供一種不合法欄位值及預期錯誤提示。
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("order_id", "", "order_id"),
        ("customer_id", "   ", "customer_id"),
        ("currency", "HK", "currency"),
        ("event_time", "2026-10-13T02:00:00", "timezone"),
        ("event_time", "not-a-time", "event_time"),
        ("items", "not-a-list", "items"),
        ("amount", -1, "amount"),
        ("amount", True, "amount"),
    ],
)
def test_invalid_field_is_rejected(field: str, value: object, message: str) -> None:
    order = valid_order()
    # 替換成一種無效值，並確認錯誤訊息包含預期提示。
    order[field] = value
    with pytest.raises(ValueError, match=message):
        validate_order(order)


def test_non_object_event_is_rejected() -> None:
    # JSON event 的最外層必須是 object，不能是 list。
    with pytest.raises(ValueError, match="JSON object"):
        validate_order(["not", "an", "object"])


def test_make_order_creates_utc_order() -> None:
    # 範例產生器應建立固定格式 ID、有效金額及 UTC 時間。
    order = make_order(1)
    assert order["order_id"] == "order-000001"
    assert order["currency"] == "HKD"
    assert order["amount"] >= 0
    # deepcopy 避免驗證測試意外共用或改動原訂單物件。
    assert validate_order(deepcopy(order))["event_time"] == order["event_time"]
    assert str(order["event_time"]).endswith("+00:00")
