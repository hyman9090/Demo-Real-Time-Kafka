# argparse 處理 CLI 參數；json 編碼事件；其餘模組處理設定、輸出及等待。
import argparse
import json
import os
import sys
import time

from confluent_kafka import KafkaError, Message, Producer
from dotenv import load_dotenv

from .event import make_order


def positive_int(value: str) -> int:
    # argparse 會將輸入文字轉成整數，並拒絕零或負數。
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def non_negative_float(value: str) -> float:
    # 發送間隔可以是 0，但不可為負數。
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be 0 or greater")
    return parsed


def main() -> int:
    # 設定可由命令列調整的訂單數量和每筆發送間隔。
    parser = argparse.ArgumentParser(description="Publish sample orders to Kafka")
    parser.add_argument("--count", type=positive_int, default=10)
    parser.add_argument("--interval", type=non_negative_float, default=1.0)
    args = parser.parse_args()

    # 載入 .env，並使用預設 topic 和 broker 作為本機設定。
    load_dotenv()
    topic = os.environ.get("KAFKA_TOPIC", "orders")
    # producer 會非同步傳送；callback 負責收集 broker 回報的錯誤。
    producer = Producer(
        {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    )
    delivery_errors: list[str] = []

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        # Kafka 完成每筆非同步發送後會呼叫此函式。
        if error is not None:
            delivery_errors.append(str(error))
            print(f"Delivery failed: {error}", file=sys.stderr)
            return
        print(f"Delivered order key={message.key().decode('utf-8')} to {message.topic()}")

    for sequence in range(1, args.count + 1):
        # 產生訂單，並將 JSON 文字編碼成 Kafka 可傳輸的 bytes。
        order = make_order(sequence)
        # Kafka value 是 JSON bytes；key 用 order_id 方便識別及分區。
        encoded_order = json.dumps(order, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic,
            key=str(order["order_id"]).encode("utf-8"),
            value=encoded_order,
            callback=on_delivery,
        )
        # poll 讓 confluent-kafka 處理已完成訊息的 callback。
        producer.poll(0)
        if sequence < args.count:
            # 除最後一筆外，按指定間隔再發送下一筆。
            time.sleep(args.interval)

    # 等待佇列中的訊息完成，最多 10 秒。
    pending = producer.flush(10)
    if pending:
        print(f"Delivery timed out with {pending} message(s) still pending", file=sys.stderr)
        return 1
    if delivery_errors:
        print(f"{len(delivery_errors)} message(s) failed delivery", file=sys.stderr)
        return 1

    print(f"Successfully delivered {args.count} order(s) to topic '{topic}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
