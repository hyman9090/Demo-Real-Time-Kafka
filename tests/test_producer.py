import pytest

from demo_real_time_kafka.producer import parse_topics


def test_parse_topics_returns_comma_separated_names() -> None:
    assert parse_topics("orders, orders-audit") == ("orders", "orders-audit")


@pytest.mark.parametrize("value", ["", "orders,", ",orders", "orders,,orders-audit"])
def test_parse_topics_rejects_empty_names(value: str) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        parse_topics(value)


def test_parse_topics_rejects_duplicates() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        parse_topics("orders,orders")
