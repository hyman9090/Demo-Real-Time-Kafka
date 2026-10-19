# Kafka 即時資料工程學習 Project 設計

## 目標與範圍

建立一個適合初學 data engineering 的本機即時資料管道，理解 producer、Kafka topic、consumer、offset、consumer group、PostgreSQL sink、錯誤處理及基本驗證。整體用模擬網店訂單事件，唔依賴外部 API 或雲端費用。

第一版唔包括 dashboard、雲端部署、Spark/Flink、schema registry 或 Kubernetes；完成核心管道後再作延伸練習。

## 架構與資料流

```text
Python order producer
        │ JSON event
        ▼
Kafka topic: orders
        │
        ▼
Python consumer ──► PostgreSQL: orders
```

- Docker Desktop + Docker Compose 啟動單一 Kafka broker（KRaft mode）及 PostgreSQL。
- Python producer / consumer 初期直接喺 Windows host 的 Python virtual environment 執行，避免一開始同時除錯 Docker networking 及 Python 容器。
- Kafka 使用明確固定版本的官方 image，不依賴 `latest`。
- Python Kafka client 採用 `confluent-kafka`；PostgreSQL 使用 `psycopg`。
- 透過 SQL 查資料、Docker logs 觀察服務及程式狀態；暫不引入 dashboard。

## 事件資料合約

每筆 `orders` 訊息為 UTF-8 JSON，至少包含：

```json
{
  "order_id": "order-000001",
  "event_time": "2026-10-13T02:00:00Z",
  "customer_id": "customer-012",
  "items": [{"sku": "item-004", "quantity": 2, "unit_price": 19.9}],
  "amount": 39.8,
  "currency": "HKD"
}
```

`order_id` 作為事件唯一識別及 PostgreSQL primary key。時間使用明確時區的 ISO 8601 格式；資料庫寫入使用參數化 SQL。無效 JSON 或必要欄位缺失要明確記錄及處理，唔可以靜默略過。

## 專案結構方向

```text
.
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── producer.py
├── consumer.py
├── db.py
├── schema.sql
├── tests/
│   ├── test_event_validation.py
│   └── test_db_idempotency.py
└── README.md
```

環境變數檔只放本機設定；`.env` 不納入版本控制，`.env.example` 提供不含秘密的欄位範例。README 記錄安裝、啟停、常見操作及驗收步驟。

## 分階段學習與驗收

1. **準備 Windows 開發環境**：安裝/確認 VS Code、Python 3、Git、Docker Desktop（使用 WSL 2 backend）；安裝 VS Code Python、Pylance、Docker extensions。SQLTools + PostgreSQL driver 可選；Kafka extension 亦可選，Kafka CLI/logs 足以完成第一版。
2. **學 Docker 與啟動服務**：建立 Compose 設定，啟動 Kafka（KRaft）和 PostgreSQL。驗收：兩個服務健康、Kafka topic `orders` 建立成功、PostgreSQL 可連線。
3. **建立 Python 環境及 producer**：建立 `.venv`、安裝依賴；producer 逐筆或定時產生符合合約的訂單 JSON。驗收：producer 有成功送出訊息的明確輸出，topic 能讀到 JSON。
4. **實作 consumer**：訂閱 `orders`，解碼 JSON，驗證必要欄位，並展示成功消費記錄。驗收：consumer 能接收 producer 新送出的訊息，關閉/重啟後行為符合設定的 consumer group 與 offset。
5. **寫入 PostgreSQL**：建立資料表，把訂單寫入資料庫；以 `order_id` 做唯一鍵及 upsert/衝突處理，避免重播造成重複資料。驗收：SQL 查詢行數、金額及欄位同已送出的事件吻合。
6. **加入錯誤處理及可觀察性**：處理連線失敗、無效 payload、資料庫失敗及有限次重試。資料庫錯誤重試耗盡後要停止 consumer 並保留未成功訊息的 offset；無效 payload 要記錄完整錯誤並停止 consumer，避免無限重播或靜默丟棄。只喺資料已成功保存後提交 offset。之後可將無效訊息改為寫入 dead-letter topic，再提交原 topic offset。驗收：人為製造錯誤時有清楚 log，修復服務後可恢復消費。
7. **測試、整理與回顧**：測試事件驗證、資料庫重複寫入及核心流程；撰寫 README 和故障排查清單。驗收：新開 terminal 按 README 可由零啟動、發送、消費、查詢及停止整個 project。

## 主要操作與完成標準

完整練習應能示範：啟動/停止 Compose；建立 topic；啟動 consumer 再啟動 producer；查詢 PostgreSQL；停止並重啟 consumer；觀察 offset/重播及冪等寫入；查看服務和程式 logs；最後清理容器。刪除 volume 會清除本機資料，操作前要辨識此差異。

## 延伸練習

- 將程式移入 Docker Compose，學習容器內服務名稱與網路。
- 加入 schema version、事件驗證更完整的測試及 dead-letter topic。
- 設定多個 consumer instance，觀察 consumer group 與 partition 分配。
- 增加簡單 SQL 聚合查詢或 Streamlit dashboard。
- 加入 CI 測試及雲端部署；只有本機流程穩定後再考慮。
