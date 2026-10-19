# Kafka 即時資料工程學習 Project Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 喺 Windows 本機建立可重複運行嘅 Kafka → Python consumer → PostgreSQL 即時訂單資料管道，並透過逐階段驗收學習核心 data engineering 概念。

**Architecture:** Docker Compose 只負責 Kafka KRaft 單 broker 同 PostgreSQL；Python producer/consumer 初期喺 Windows `.venv` 執行。Producer 發 JSON 訂單到 `orders` topic；consumer 驗證訊息、冪等寫入 PostgreSQL，成功後先提交 offset。

**Tech Stack:** Windows 11/10、VS Code、Python 3.12、Docker Desktop with WSL 2、Docker Compose、Apache Kafka 4.0.0、PostgreSQL 17、`confluent-kafka`、`psycopg[binary]`、`python-dotenv`、`pytest`。

**Spec:** [2026-10-13-kafka-learning-project-design.md](../specs/2026-10-13-kafka-learning-project-design.md)

## Global Constraints

- 第一版只做模擬網店訂單、Kafka、Python consumer 同 PostgreSQL；唔加 dashboard、Spark/Flink、Kubernetes 或雲端部署。
- Kafka broker 以 KRaft mode 喺 Docker Compose 運行；Python producer/consumer 初期喺 Windows host 運行。
- Docker images 使用固定版本，不使用 `latest`。
- Python 事件 JSON 至少有 `order_id`, `event_time`, `customer_id`, `items`, `amount`, `currency`。
- `order_id` 為 PostgreSQL primary key；使用參數化 SQL，重複事件唔可重複計入。
- 只喺資料已成功保存後提交 Kafka offset；錯誤必須清楚記錄，不可靜默忽略。
- `.env` 不提交；只提供 `.env.example` 作本機設定範本。
- 刪除 Compose volume 會永久清除本機 PostgreSQL/Kafka 資料，操作前必須提醒。

---

## 開始前：需要安裝/準備

1. **Visual Studio Code**：由 [code.visualstudio.com](https://code.visualstudio.com/) 下載安裝，作為編輯器。
2. **Python 3.12 64-bit**：由 [python.org/downloads](https://www.python.org/downloads/) 安裝，勾選 Python launcher；用 `py -3.12` 管理此 project 的 Python。
3. **Git for Windows**：由 [git-scm.com/download/win](https://git-scm.com/download/win) 安裝 Git，並在安裝設定中選擇 Git from the command line and also from 3rd-party software。建議以 Git Bash 或 VS Code terminal / PowerShell 使用。
4. **Docker Desktop for Windows**：由 [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/) 安裝並選用 WSL 2 backend；如安裝程式要求，啟用 WSL 2/Virtual Machine Platform 後重新開機。首次啟動 Docker Desktop，等 Docker Engine 顯示 Running。
5. **VS Code extensions**（VS Code → Extensions，搜尋並安裝）：
   - Python（`ms-python.python`）
   - Pylance（`ms-python.vscode-pylance`）
   - Docker（`ms-azuretools.vscode-docker`）
   - 可選：SQLTools（`mtxr.sqltools`）及 SQLTools PostgreSQL/Cockroach Driver（`mtxr.sqltools-driver-pg`）。第一版亦可直接用 `docker compose exec db psql ...`，所以唔係必要。
   - Kafka extension 唔係必要；先用 Kafka CLI 和 logs 學概念，避免依賴額外 UI。
6. **硬碟/網絡**：Docker images 會佔用數 GB；首次 pull image 需要網絡。唔需要安裝 Java，因為 Kafka 跑喺容器內。

### 安裝後先驗證

喺 VS Code 開啟 PowerShell terminal：

```powershell
py -3.12 --version
git --version
docker --version
docker compose version
docker info
```

預期前三個 version 指令成功，`docker compose version` 顯示 Compose v2，`docker info` 有 Server 資訊。若 Docker client 有但 Server 無法連線，先開 Docker Desktop 等 Engine ready；唔好繼續到 Compose 步驟。

## 預期檔案與責任

```text
.
├── .env.example                # 安全的本機開發設定範本
├── .gitignore                  # 排除 secrets、venv、cache
├── docker-compose.yml          # Kafka KRaft + PostgreSQL
├── requirements.txt            # Python dependencies
├── schema.sql                   # PostgreSQL orders table
├── event.py                     # 訂單事件型別及輸入驗證
├── db.py                        # PostgreSQL connection / idempotent insert
├── producer.py                  # 產生及發送模擬訂單
├── consumer.py                  # consume、validate、persist、commit
├── tests/
│   ├── test_event.py            # event validation unit tests
│   └── test_db.py               # PostgreSQL idempotency integration test
└── README.md                    # 安裝、運行、查詢、排錯及 teardown
```

---

### Task 1: 建立 Python 專案及本機工具設定

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create: `requirements.txt`
- Create: `README.md`

**Interfaces:**
- `.env.example` 定義 `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`。
- Python 程式只讀取 environment variables；`python-dotenv` 由本機 `.env` 載入。

- [ ] **Step 1: 初始化 Git 並建立 virtual environment**

喺 project folder 執行：

```powershell
git init
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
```

若 PowerShell 阻止 activate script，可唔改系統 execution policy，直接用 `.\.venv\Scripts\python.exe -m pip ...` 同 `.\.venv\Scripts\python.exe ...` 執行。

- [ ] **Step 2: 建立 `.gitignore`，避免提交環境及資料**

加入以下內容：

```gitignore
.venv/
.env
__pycache__/
.pytest_cache/
*.py[cod]
.vscode/
```

- [ ] **Step 3: 建立 `.env.example`**

```dotenv
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=orders
KAFKA_GROUP_ID=orders-to-postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=orders
POSTGRES_USER=de_user
POSTGRES_PASSWORD=local_dev_only
```

複製成本機設定檔：

```powershell
Copy-Item .env.example .env
```

只改 `.env`，唔將真實密碼放入 `.env.example` 或程式碼。

- [ ] **Step 4: 建立依賴清單並安裝**

`requirements.txt`：

```text
confluent-kafka==2.10.1
psycopg[binary]==3.2.9
python-dotenv==1.1.1
pytest==8.4.1
```

安裝及確認套件可 import：

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c "import confluent_kafka, psycopg, dotenv, pytest; print('dependencies OK')"
```

Expected: `dependencies OK`。如果指定版本安裝失敗，先確認 Python 係 64-bit 3.12 同網絡正常；唔好移除版本 pin 來掩蓋問題。

- [ ] **Step 5: 建立 README 初始操作說明**

README 記錄本機依賴、如何啟用 `.venv`、如何安裝 requirements、`.env` 用途及 Docker/VS Code extensions。之後各 task 再補齊完整操作。

- [ ] **Step 6: 驗證並提交**

```powershell
python -m pip check
git status --short
git add .gitignore .env.example requirements.txt README.md
git commit -m "chore: scaffold kafka learning project"
```

Expected: `pip check` 顯示 `No broken requirements found`；`git status --short` 不可列出 `.env` 或 `.venv`。

---

### Task 2: 以 Docker Compose 啟動 Kafka 同 PostgreSQL

**Files:**
- Create: `docker-compose.yml`
- Create: `schema.sql`
- Modify: `README.md`

**Interfaces:**
- Host Python 程式連 Kafka：`localhost:9092`。
- Host Python 程式連 PostgreSQL：`localhost:5432`，database `orders`。
- Kafka topic 名稱為 `orders`，單 broker/partition 先專注理解基本資料流。

- [ ] **Step 1: 寫 Compose 設定**

`docker-compose.yml` 至少設定兩個 service：

```yaml
services:
  kafka:
    image: apache/kafka:4.0.0
    hostname: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 1
    volumes:
      - kafka_data:/var/lib/kafka/data

  db:
    image: postgres:17
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-orders}
      POSTGRES_USER: ${POSTGRES_USER:-de_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-local_dev_only}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./schema.sql:/docker-entrypoint-initdb.d/001-schema.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  kafka_data:
  postgres_data:
```

Initial Compose 只映射 Kafka client port；controller port 不需要 expose 到 Windows host。

- [ ] **Step 2: 建立 PostgreSQL table schema**

`schema.sql`：

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL,
    items JSONB NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL
);
```

- [ ] **Step 3: 驗證 Compose 設定並啟動**

```powershell
docker compose config
docker compose up -d
docker compose ps
```

Expected: `kafka` 同 `db` services running，`db` health 顯示 healthy。首次執行會下載 images，可能需要幾分鐘。

- [ ] **Step 4: 建立 topic 並確認資料庫 schema**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
docker compose exec db psql -U de_user -d orders -c "\dt"
```

Expected: topic `orders` 存在且 1 partition；PostgreSQL 列出 `orders` table。

- [ ] **Step 5: 驗收初始 infra，補 README，提交**

README 加上 `docker compose up -d`, `docker compose ps`, `docker compose logs -f kafka`, `docker compose down`。註明 `docker compose down -v` 會清除 volumes，同一般 `down` 不同。

```powershell
git add docker-compose.yml schema.sql README.md
git commit -m "feat: add local kafka and postgres services"
```

---

### Task 3: 定義訂單事件格式及驗證規則

**Files:**
- Create: `event.py`
- Create: `tests/test_event.py`

**Interfaces:**
- `validate_order(payload: object) -> dict[str, object]`：驗證欄位及值；失敗 raise `ValueError`，成功回傳可序列化的原 payload。
- `make_order(sequence: int) -> dict[str, object]`：產生一筆有效的練習訂單；訂單 ID 由 sequence 決定，事件時間為呼叫時的 UTC。

- [ ] **Step 1: 先寫成功與失敗測試**

`tests/test_event.py` 要覆蓋：

1. 有效事件包含六個必需欄位並通過驗證。
2. 缺少 `order_id` 或其他必要欄位時 raise `ValueError`。
3. `amount` 為負數、`items` 不是 list、`event_time` 無時區時拒收。
4. `make_order(1)` 產生穩定唯一 ID、HKD、非負 amount、ISO 8601 UTC timestamp。

- [ ] **Step 2: 執行測試確認先失敗**

```powershell
python -m pytest tests/test_event.py -q
```

Expected: FAIL，因為 `event.py` 仲未存在。

- [ ] **Step 3: 實作驗證及 mock order generator**

事件範例：

```python
{
    "order_id": "order-000001",
    "event_time": "2026-10-13T02:00:00+00:00",
    "customer_id": "customer-012",
    "items": [{"sku": "item-004", "quantity": 2, "unit_price": 19.90}],
    "amount": 39.80,
    "currency": "HKD",
}
```

驗證必須檢查 `order_id/customer_id/currency` 係非空字串、`event_time` 有 timezone、`items` 為 list、amount 非負、currency 長度為 3。產生器用 `datetime.now(timezone.utc)`；測試只對時間格式及 timezone 作斷言，避免依賴當下秒數。

- [ ] **Step 4: 重跑測試及 commit**

```powershell
python -m pytest tests/test_event.py -v
git add event.py tests/test_event.py
git commit -m "feat: validate order events"
```

Expected: 全部 event unit tests PASS。

---

### Task 4: 實作 PostgreSQL 冪等寫入

**Files:**
- Create: `db.py`
- Create: `tests/test_db.py`
- Modify: `README.md`

**Interfaces:**
- `get_connection()` 由 `.env`/environment variables 建立 psycopg connection。
- `insert_order(order: dict[str, object]) -> bool` 寫入單筆事件；新訂單回傳 `True`，相同 `order_id` 重播回傳 `False`。
- SQL 使用 `ON CONFLICT (order_id) DO NOTHING`；資料錯誤或連線錯誤明確 raise，不可回傳成功。

- [ ] **Step 1: 建立 DB integration test**

測試需使用 Compose 中真實 PostgreSQL：建立唯一測試 order ID，第一次 insert 回傳 `True`、第二次 insert 回傳 `False`，最後 SQL 查詢只有一行；以 `finally` 清走該測試 ID，避免污染日常數據。

- [ ] **Step 2: 確認先失敗**

```powershell
python -m pytest tests/test_db.py -v
```

Expected: FAIL，因為 `db.py` 函式未實作。若錯誤係 DB 無法連線，先確認 Task 2 的 Compose health，不可當成預期失敗。

- [ ] **Step 3: 實作 parameterized insert**

SQL 格式：

```sql
INSERT INTO orders (order_id, event_time, customer_id, items, amount, currency)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO NOTHING
RETURNING order_id;
```

用 `psycopg.connect()` context manager 管理 transaction；JSONB 透過 psycopg JSON adapter 傳入，不可用字串拼接 SQL。`event_time` parse 成 aware `datetime`。若 `RETURNING` 有 row 回 `True`，否則回 `False`。

- [ ] **Step 4: 跑 DB test、手動查詢及提交**

```powershell
python -m pytest tests/test_db.py -v
docker compose exec db psql -U de_user -d orders -c "SELECT order_id, event_time, amount, currency FROM orders ORDER BY event_time DESC LIMIT 5;"
git add db.py tests/test_db.py README.md
git commit -m "feat: persist orders idempotently"
```

Expected: test PASS，查詢結果欄位型別合理，重播不增加行數。

---

### Task 5: 建立 Kafka producer 發送模擬訂單

**Files:**
- Create: `producer.py`
- Modify: `README.md`

**Interfaces:**
- CLI: `python producer.py --count 10 --interval 1` 發送 10 筆訂單，每筆間隔 1 秒。
- Kafka key 為 `order_id`，value 為 UTF-8 JSON。
- 每筆送出需等待 delivery callback 結果或於結束前 flush；失敗要以非零狀態明確報錯。

- [ ] **Step 1: 寫 producer 的操作契約**

README 先列出 command、預期會看到送出數量同 Kafka topic 名稱。明確說明 consumer 未啟動時，訊息仍會留在 Kafka。

- [ ] **Step 2: 實作 producer CLI**

`producer.py` 讀 `.env`、用 `argparse` 驗證 `--count > 0` 和 `--interval >= 0`、以 `make_order(sequence)` 產生事件、JSON encode 後呼叫 `confluent_kafka.Producer.produce(topic, key=..., value=..., callback=...)`。每個事件後呼叫 `poll(0)` 處理 delivery callback，最後 `flush(10)`；若仍有 pending messages 或 callback 報錯，明確回傳 failure。

- [ ] **Step 3: 驗證 topic 內有訊息**

```powershell
python producer.py --count 5 --interval 0
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5
```

Expected: producer 報告 5 筆 delivery success；console consumer 顯示 5 個 JSON event，包含不同 `order_id`。

- [ ] **Step 4: 提交**

```powershell
git add producer.py README.md
git commit -m "feat: publish sample orders to kafka"
```

---

### Task 6: 消費 Kafka 訊息並寫入 PostgreSQL

**Files:**
- Create: `consumer.py`
- Modify: `README.md`

**Interfaces:**
- CLI: `python consumer.py` 長時間消費；`Ctrl+C` 正常關閉。
- Kafka consumer group ID 為 `.env` 的 `KAFKA_GROUP_ID`。
- 每筆訊息流程：decode UTF-8 JSON → `validate_order` → `insert_order` → commit 該 message offset。

- [ ] **Step 1: 加入 consumer group / offset 手動提交設定**

建立 `Consumer` 時設定 `bootstrap.servers`, `group.id`, `auto.offset.reset=earliest`, `enable.auto.commit=False`。`earliest` 只影響該 group 尚未有 committed offset 時的起始位置。

- [ ] **Step 2: 實作 consume loop**

每次 `poll(1.0)`；無訊息就繼續。Kafka partition EOF 唔係 fatal error；其他 Kafka error 要記錄並明確退出/raise。每筆 payload decode/JSON parse/validate 後呼叫 `insert_order()`；DB 成功（包括 duplicate 達到冪等處理）後才 `consumer.commit(message=msg, asynchronous=False)`。使用 `try/finally` 確保 `consumer.close()` 執行。

- [ ] **Step 3: 定義錯誤行為，不靜默略過**

無效 JSON 或 schema：記錄 topic/partition/offset 和錯誤，停止 consumer，唔提交該訊息 offset。DB 連線/寫入錯誤：有限次 backoff retry；仍失敗就停止 consumer 並保留 offset。README 要說明 poison message 會令該 partition 暫停；練習者需查 log/訊息，之後先手動重設 offset 或重建練習 topic。之後的 dead-letter topic 是延伸，不可默默將失敗訊息當成功。

- [ ] **Step 4: 端到端手動驗收**

開兩個 VS Code terminals：

Terminal A:

```powershell
python consumer.py
```

Terminal B:

```powershell
python producer.py --count 10 --interval 0
```

第三個 terminal 查資料：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT COUNT(*) FROM orders;"
docker compose exec db psql -U de_user -d orders -c "SELECT order_id, event_time, amount, currency FROM orders ORDER BY event_time;"
```

Expected: 10 個不同訂單寫入；consumer log 顯示保存成功，offset 在 DB commit 後才提交。

- [ ] **Step 5: 驗證重啟、offset 及冪等**

停止 consumer，再用相同 group ID 重啟；正常應只處理新訊息。重播相同 `order_id` 測試 DB 不多一行。開第二個 consumer instance，因 topic 只有一個 partition，同一 group 只有其中一個 instance 會獲分配該 partition。

- [ ] **Step 6: 提交**

```powershell
git add consumer.py README.md
git commit -m "feat: consume orders into postgres"
```

---

### Task 7: 自動化測試、操作文件及重複性驗收

**Files:**
- Modify: `tests/test_event.py`
- Modify: `tests/test_db.py`
- Modify: `README.md`

**Interfaces:**
- Unit tests 不依賴 Docker；DB integration test 明確需要 Compose PostgreSQL。
- README 提供完整 PowerShell 指令，唔假設使用者知道 Linux shell。

- [ ] **Step 1: 完成 event validation 測試矩陣**

覆蓋 valid event、每個必需欄位缺失/空字串、invalid timestamp/timezone、負 amount、wrong items type、currency 格式錯誤；每個 invalid case 預期 `ValueError` 並檢查訊息指明欄位。

- [ ] **Step 2: 執行 unit tests**

```powershell
python -m pytest tests/test_event.py -v
```

Expected: 所有 event unit tests PASS，唔需要 Kafka/PostgreSQL。

- [ ] **Step 3: 執行 DB integration tests**

```powershell
docker compose up -d
python -m pytest tests/test_db.py -v
```

Expected: DB idempotency test PASS；失敗時先看 `docker compose ps` 和 `docker compose logs db`。

- [ ] **Step 4: 補完整 README**

README 依次包含：前置軟件/extensions、Docker Desktop 檢查、PowerShell venv 命令、安裝 requirements、複製 `.env.example`、`docker compose up -d`、topic 建立/檢查、啟動 consumer、producer 發送、psql 查詢、測試、查看 logs、停止服務、清除資料的危險提示，以及常見問題。

- [ ] **Step 5: 從乾淨 shell 做完整驗收**

開新 PowerShell terminal，按 README 由啟動環境開始重做：Compose → topic → consumer → producer → PostgreSQL query → pytest → `docker compose down`。唔用刪除 volume 的指令。每一步都要得到 README 列出的預期輸出。

- [ ] **Step 6: 完成並提交**

```powershell
git add tests README.md
git commit -m "test: document end-to-end kafka pipeline"
git status --short
```

Expected: tests PASS，工作目錄乾淨，`.env`、`.venv`、Kafka/PostgreSQL data 未納入 Git。

---

## 完成後延伸（唔納入第一版）

1. 用 Compose 加 Kafka UI 作選修視覺化。
2. 加 `orders-dlq` dead-letter topic；成功寫入 DLQ 後先 commit 原 topic offset。
3. Kafka 增加 partitions，跑多個 consumer instance，觀察 group assignment / rebalance。
4. 將 Python producer/consumer 亦容器化，學習 Compose service DNS 同 container networking。
5. 加 schema version、CI、簡單聚合查詢或 Streamlit dashboard。
6. 最後先研究雲端 Kafka、managed PostgreSQL 或 Spark/Flink。
