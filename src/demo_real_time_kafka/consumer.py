# json 解碼 Kafka 訊息；logging 記錄處理結果；time 用於資料庫重試等待。
import argparse
import json
import logging
import os
import time

import psycopg
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from .db import insert_order
from .event import validate_order

# 統一 consumer 的 log 格式，方便辨認錯誤和訊息處理狀態。
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("orders-consumer")


def persist_with_retries(order: dict[str, object], attempts: int = 3) -> bool:
    # 僅資料庫錯誤會重試；耗盡次數後保留錯誤給上層處理。
    for attempt in range(1, attempts + 1):
        try:
            return insert_order(order)
        except psycopg.Error:
            # 只重試 PostgreSQL 錯誤；非資料庫錯誤會直接向外拋出。
            logger.exception("Database write failed (attempt %s/%s)", attempt, attempts)
            if attempt == attempts:
                raise
            time.sleep(attempt)
    raise RuntimeError("database retry loop exited unexpectedly")


def main() -> None:
    # 載入 .env；命令列參數可為每個 consumer group 指定 topic 和 group ID。
    load_dotenv()
    parser = argparse.ArgumentParser(description="Consume orders from Kafka")
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "orders"))
    parser.add_argument(
        "--group-id",
        default=os.environ.get("KAFKA_GROUP_ID", "orders-to-postgres"),
    )
    args = parser.parse_args()
    topic = args.topic
    # 關閉自動提交，確保只有資料庫寫入成功後才提交 offset。
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    logger.info(
        "Group '%s' subscribed to topic '%s'; press Ctrl+C to stop",
        args.group_id,
        topic,
    )

    try:
        while True:
            # poll 會等待訊息；逾時回傳 None，讓迴圈繼續等待。
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                # 分區讀取到尾端是正常狀態，其他 Kafka 錯誤則停止處理。
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka message error: {message.error()}")

            location = (
                # 記錄訊息位置，出錯時可追查 topic、partition 和 offset。
                f"topic={message.topic()} "
                f"partition={message.partition()} offset={message.offset()}"
            )
            try:
                # Kafka 傳來 bytes，先解碼 JSON，再套用共用事件驗證。
                value = message.value()
                if value is None:
                    raise ValueError("message value must not be null")
                payload = json.loads(value.decode("utf-8"))
                order = validate_order(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                logger.exception(
                    "Invalid message; stopping without committing (%s)", location
                )
                raise

            # 先確保資料庫已新增或確認 duplicate，再提交 Kafka offset。
            inserted = persist_with_retries(order)
            committed = consumer.commit(message=message, asynchronous=False)
            if committed is not None:
                # 同步 commit 可能回傳各分區結果，檢查並明確報告錯誤。
                commit_errors = [
                    partition.error for partition in committed if partition.error
                ]
                if commit_errors:
                    raise RuntimeError(f"Kafka offset commit failed: {commit_errors}")

            result = "inserted" if inserted else "duplicate (already stored)"
            logger.info(
                "Saved order_id=%s (%s); offset committed (%s)",
                order["order_id"],
                result,
                location,
            )
    except KeyboardInterrupt:
        logger.info("Stopping consumer after Ctrl+C")
    finally:
        # 不論正常停止或發生錯誤，都關閉 consumer 並離開 group。
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    main()
