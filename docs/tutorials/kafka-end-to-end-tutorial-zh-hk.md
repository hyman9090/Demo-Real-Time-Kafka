# Windows 10 手動搭建 Kafka 即時訂單管道：由下載到端對端驗收

> 本教學以「你親手做一次」為目標。請逐步閱讀、自己下載、自己按安裝畫面、自己建立檔案及輸入指令。不要一次過貼上所有 commands；每個步驟都有預期結果，確認後才繼續。
>
> 本文件根據 [Kafka 學習 Project 設計](../superpowers/specs/2026-10-13-kafka-learning-project-design.md) 及 [Implementation Plan](../superpowers/plans/2026-10-13-kafka-learning-project.md) 編寫。為令新手可由零完成，以下亦提供各個練習檔案的完整範例內容。這些程式碼是你要親手建立的練習材料；本教學不會替你安裝軟件、執行 setup、建立或啟動任何服務。

## 你完成後會有甚麼

本機會運行一條簡化的即時資料管道：

```text
producer.py（Windows 上的 Python）
      │  JSON 訂單
      ▼
Kafka topic：orders（Docker container）
      │
      ▼
consumer.py（Windows 上的 Python）
      │  驗證、寫入成功後提交 offset
      ▼
PostgreSQL 資料庫：orders（Docker container）
```

你會學到 producer、topic、consumer、consumer group、offset、冪等寫入、Docker Compose、基本錯誤處理，以及用 SQL 驗證資料。

**本教學第一版不會做** dashboard、雲端部署、Spark/Flink、Kubernetes、Schema Registry 或 dead-letter topic。Kafka 和 PostgreSQL 在 Docker 中執行；Python 程式在 Windows host 執行，避免一開始同時處理容器內 Python networking。

## 重要約定及安全提醒

- 所有指令以 **VS Code 的 PowerShell terminal** 為準。不是 Command Prompt，也不是 Git Bash。
- 專案資料夾建議放在本機短路徑，例如 `C:\Users\你的Windows帳戶名稱\kafka-learning-project`；避免 OneDrive 同步目錄、空格及太深的路徑。
- 本教學固定使用 Python 3.12、Kafka image `apache/kafka:4.0.0`、PostgreSQL image `postgres:17`。不要自行改成 `latest`。
- `.env` 只供本機使用；不要將真正密碼寫入程式碼或提交到 Git。
- `docker compose down` 會停止並移除 containers，但會保留 Kafka/PostgreSQL volumes。
- **`docker compose down -v` 會刪除兩個 volumes，永久清除本機 Kafka 與 PostgreSQL 資料。** 除非你確定要由空白資料重新開始，否則不要輸入這個指令。
- 如果 Docker Desktop 顯示 Docker Engine 未啟動、Compose config 有錯，或前置軟件版本檢查失敗，先停在該步排查，不要跳過錯誤繼續。

---

## 第一部分：下載及安裝 Windows 工具

### 1. 確認 Windows 及電腦架構

1. 按 Windows **Start**。
2. 開啟 **Settings（設定）→ System（系統）→ About（關於）**。
3. 確認 **System type** 是 64-bit operating system。
4. 記下 Windows 版本。Docker Desktop 的支援要求會更新；如果 Docker 官網列出的最低 Windows build 高過你的電腦，先更新 Windows，不要嘗試繞過相容性檢查。

### 2. 下載及安裝 Visual Studio Code

1. 用瀏覽器前往 [Visual Studio Code 官方下載頁](https://code.visualstudio.com/Download)。
2. 在 Windows 一欄選 **User Installer**（一般個人電腦建議）或 **System Installer**（由管理員為整部電腦安裝）。
3. 開啟下載的 VS Code 安裝程式。
4. 閱讀並接受授權條款，按 **Next**。
5. 保留預設安裝路徑，按 **Next**。
6. 在 **Select Additional Tasks** 畫面，建議勾選：
   - **Add to PATH**（若安裝程式有提供）
   - **Add "Open with Code" action to Windows Explorer file context menu**
   - **Add "Open with Code" action to Windows Explorer directory context menu**
   - **Register Code as an editor for supported file types**
   - **Create a desktop icon**（可選）
7. 按 **Next → Install**，安裝完成後按 **Finish** 開啟 VS Code。

**為甚麼先安裝 VS Code？** 之後會用它建立及編輯 Python、YAML、SQL 檔案，並在內置 terminal 執行 PowerShell。

### 3. 下載及安裝 Python 3.12（64-bit）

1. 前往 [Python 3.12 官方 Windows 發行頁](https://www.python.org/downloads/windows/)。
2. 在 Python 3.12 的 release 區域，選擇最新的 **Python 3.12.x** 發行版本。
3. 到該 release 的 **Files** 區域，下載 **Windows installer (64-bit)**。不要選 embeddable package、source archive 或 32-bit installer。
4. 開啟下載的 installer。
5. 第一個畫面如有 **Add python.exe to PATH**，可勾選；確保 **Python Launcher** 功能會安裝，因為本教學用 `py -3.12` 指定 Python 版本。
6. 按 **Install Now**。如果 Windows 顯示 User Account Control，確認發行者為 Python Software Foundation 後按 **Yes**。
7. 安裝完成後按 **Close**。
8. 在 VS Code 選 **Terminal → New Terminal**。如 terminal 不是 PowerShell，按 terminal 右上角下拉箭頭，選 **PowerShell**。
9. 輸入：

   ```powershell
   py -3.12 --version
   ```

   預期看到 `Python 3.12.x`。如果出現「`py` is not recognized」，重新開啟 VS Code；仍然失敗就重新執行 Python installer，選 **Modify** 並確認 Python Launcher 已安裝。

**為甚麼使用 `py -3.12`？** Windows Python Launcher 可以在多個 Python 版本並存時，明確選中 3.12，而不是依賴 PATH 中剛好排第一的版本。

### 4. 下載及安裝 Git for Windows

1. 前往 [Git for Windows 官方下載頁](https://git-scm.com/download/win)。
2. 下載 64-bit Git for Windows installer 並開啟。
3. 逐頁按 **Next**，保留大部分預設值。
4. 在 **Adjusting your PATH environment** 畫面，選 **Git from the command line and also from 3rd-party software**。
5. 其餘頁面可保留預設，按 **Install**；完成後按 **Finish**。
6. 回到 VS Code PowerShell terminal，輸入：

   ```powershell
   git --version
   ```

   預期看到 `git version ...`。

**為甚麼要 Git？** 它可以記錄練習每個階段的變更。以下會初始化本機 repository；不會要求你上傳到 GitHub。

### 5. 安裝 WSL 2（如電腦尚未安裝）

Docker Desktop 在 Windows 上使用 WSL 2 backend。先在 VS Code PowerShell 輸入：

```powershell
wsl --status
```

- 如果指令成功，並顯示 WSL 狀態，繼續安裝 Docker Desktop。
- 如果顯示找不到 WSL，或提示需要安裝/更新，依次做以下操作：
  1. 開啟 Windows **Start**，搜尋 **PowerShell**。
  2. 對 **Windows PowerShell** 按右鍵，選 **Run as administrator**，並在 UAC 視窗按 **Yes**。
  3. 輸入：

     ```powershell
     wsl --install
     ```

  4. 如命令要求，重新啟動 Windows。
  5. 首次登入後如 Ubuntu 視窗要求建立 Linux username/password，按畫面指示完成。這個 Linux 帳戶不是 Windows 密碼。
  6. 開啟一般 VS Code PowerShell terminal，再輸入 `wsl --status`。確認預設版本為 2。

若 Windows 版本太舊而不支援 `wsl --install`，先按 Microsoft 文件 [Install WSL](https://learn.microsoft.com/windows/wsl/install) 更新 Windows/WSL；不要在本教學跳過 WSL 2。

### 6. 下載及安裝 Docker Desktop

1. 前往 [Docker Desktop 官方頁](https://www.docker.com/products/docker-desktop/)。
2. 按 **Download Docker Desktop**，選 Windows 的 **AMD64** 安裝程式（本教學的 Python/PC 是 64-bit x86；如你的 System type 是 ARM，先查 Docker 官方支援說明）。
3. 開啟下載的 installer；在設定畫面選用 **Use WSL 2 instead of Hyper-V** 或 **WSL 2 backend**（文字會因版本而略有不同）。
4. 按 **OK / Install**。如安裝程式要求開啟 Windows features 或重新啟動，按提示完成。
5. 重新登入/開機後，從 Windows Start 開啟 **Docker Desktop**。
6. 如出現服務條款畫面，閱讀後按 **Accept**。
7. 等左下角/狀態列的 Docker Engine 顯示 **Running**。第一次啟動可能需要幾分鐘。
8. 在 Docker Desktop 開啟 **Settings（齒輪）→ General**，確認 **Use the WSL 2 based engine** 已勾選，按 **Apply & restart**（如你有更改）。
9. 回到 VS Code PowerShell terminal，輸入：

   ```powershell
   docker --version
   docker compose version
   docker info
   ```

   預期：
   - 前兩個指令顯示 Docker Engine 和 Docker Compose v2 版本。
   - `docker info` 顯示 **Server** 資訊，而不只是 Client 資訊。

如果 `docker info` 報錯「Cannot connect to the Docker daemon」，先開 Docker Desktop 並等它顯示 **Running**，再重試。這時不要繼續 Compose 步驟。

**為甚麼不用安裝 Java 或 Kafka 到 Windows？** Kafka broker 會在 Docker container 內執行；Windows 上的 Python client 只連 `localhost:9092`。

### 7. 安裝 VS Code extensions

1. 在 VS Code 左側按 **Extensions** 圖示（四格方塊），或按 `Ctrl+Shift+X`。
2. 在搜尋框輸入 extension 名稱。
3. 對每個必要 extension 按 **Install**：
   - **Python**，publisher Microsoft，ID `ms-python.python`
   - **Pylance**，publisher Microsoft，ID `ms-python.vscode-pylance`
   - **Docker**，publisher Microsoft，ID `ms-azuretools.vscode-docker`
4. 可選安裝：
   - **SQLTools**：`mtxr.sqltools`
   - **SQLTools PostgreSQL/Cockroach Driver**：`mtxr.sqltools-driver-pg`
5. SQLTools 和 Kafka extension 都不是必要條件。本教學會用 PostgreSQL CLI 及 Kafka CLI，先用命令理解整條資料流。

---

## 第二部分：建立並開啟專案資料夾

### 8. 建立資料夾並用 VS Code 開啟

1. 開啟 Windows **File Explorer**。
2. 到 `C:\Users\你的Windows帳戶名稱\`，建立新資料夾 `kafka-learning-project`。可在位址列輸入：

   ```text
   %USERPROFILE%
   ```

   按 Enter 後，在空白位置按右鍵 **New → Folder**，輸入 `kafka-learning-project`。
3. 開啟 VS Code，按 **File → Open Folder...**。
4. 在路徑欄輸入/選擇 `C:\Users\你的Windows帳戶名稱\kafka-learning-project`，按 **Select Folder**。
5. 若 VS Code 問 **Do you trust the authors of the files in this folder?**，這是你自己新建的空資料夾，按 **Yes, I trust the authors**。
6. 按 **Terminal → New Terminal** 開 PowerShell。確認目前位置是新專案資料夾：

   ```powershell
   Get-Location
   ```

預期路徑尾部是 `kafka-learning-project`。以下所有 commands 都要在這個資料夾執行。

### 9. 初始化本機 Git repository

在 terminal 輸入：

```powershell
git init
```

預期看到 `Initialized empty Git repository ...`。這只在你的電腦建立版本記錄，並沒有建立 GitHub repository 或上傳任何檔案。

### 10. 建立 Python virtual environment

在 terminal 依次輸入：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
```

預期：

- terminal 提示字前出現 `(.venv)`。
- Python 版本為 `3.12.x`。

如果 PowerShell 顯示執行政策阻止 `Activate.ps1`，**不要改整部電腦的 Execution Policy**。可以不啟動環境，直接使用虛擬環境中的 Python：

```powershell
.\.venv\Scripts\python.exe --version
```

之後把本教學的 `python ...` 換成 `.\.venv\Scripts\python.exe ...`，把 `pip ...` 換成 `.\.venv\Scripts\python.exe -m pip ...`。這樣仍然會用到專案 `.venv`，不需要更改系統設定。

**虛擬環境是甚麼？** `.venv` 將此練習用的第三方套件隔離起來，不會混入電腦其他 Python project。它不需要手動提交 Git。

### 11. 在 VS Code 選擇 `.venv` interpreter

1. 按 `Ctrl+Shift+P` 開啟 Command Palette。
2. 搜尋並選 **Python: Select Interpreter**。
3. 在清單選路徑含有本專案 `.venv\Scripts\python.exe` 的項目。
4. 如清單沒有 `.venv`，按 **Enter interpreter path... → Find...**，選專案資料夾中的 `.venv\Scripts\python.exe`。
5. VS Code 右下角/狀態列應顯示 Python 3.12 和 `.venv`。

---

## 第三部分：親手建立設定檔與服務

### 12. 在 VS Code 建立檔案的方法

接下來每個檔案都由你在 VS Code 建立：

1. 在 Explorer 左側，按 **New File** 圖示；或按 **File → New Text File**。
2. 輸入指定檔名（包括副檔名），例如 `.gitignore`，按 Enter。
3. 將相應 code block 內容複製到編輯器。
4. 按 `Ctrl+S` 儲存。
5. 如 VS Code 問要不要新增副檔名/切換語言，保持檔名正確即可。

如果要建立 `tests` 子資料夾，在 Explorer 按 **New Folder**，輸入 `tests`，再於該資料夾中建立測試檔案。

### 13. 建立 `.gitignore`

新建檔案 `.gitignore`，貼上並儲存：

```gitignore
.venv/
.env
__pycache__/
.pytest_cache/
*.py[cod]
.vscode/
```

**解釋：** `.env` 可能有密碼；`.venv`、Python cache 和測試 cache 都是本機生成資料，不應放進 Git。

### 14. 建立 `.env.example`，再複製成 `.env`

新建 `.env.example`，貼上：

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

在 PowerShell terminal 輸入：

```powershell
Copy-Item .env.example .env
```

預期沒有錯誤，Explorer 會看到 `.env`。若看不到，按 Explorer 上方的 **Refresh**；確認檔案已建立：

```powershell
Get-ChildItem -Force .env*
```

`.env` 的本機開發密碼 `local_dev_only` 只適用於這個練習，不可用於真實服務。保持 `.env` 與 `.env.example` 的設定一致；分享專案時只分享 `.env.example`。

### 15. 建立 `requirements.txt` 並安裝 Python 套件

新建 `requirements.txt`：

```text
confluent-kafka==2.10.1
psycopg[binary]==3.2.9
python-dotenv==1.1.1
pytest==8.4.1
```

在已啟動 `.venv` 的 terminal 輸入：

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c "import confluent_kafka, psycopg, dotenv, pytest; print('dependencies OK')"
python -m pip check
```

預期最後兩個檢查分別顯示 `dependencies OK` 及 `No broken requirements found`。如果 `python` 指向錯誤版本，回到第 10–11 步，或改用 `.\.venv\Scripts\python.exe -m pip ...`。

**套件作用：**

- `confluent-kafka`：Python Kafka producer/consumer client。
- `psycopg`：PostgreSQL client；`binary` extra 包含 Windows 可用的 binary。
- `python-dotenv`：由 `.env` 載入本機設定。
- `pytest`：執行測試。

### 16. 建立 `schema.sql`

新建 `schema.sql`：

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

**欄位解釋：**

- `order_id` 是唯一訂單 ID，也是 primary key。
- `TIMESTAMPTZ` 保存有時區的事件時間。
- `items` 用 `JSONB` 保存商品清單。
- `amount` 不可為負數，最多 12 位數及 2 位小數。
- `currency` 固定為 3 個字元，例如 `HKD`。

### 17. 建立 `docker-compose.yml`

新建 `docker-compose.yml`，確認縮排使用空格而不是 Tab，貼上：

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

**Compose 設定解釋：**

- Compose 會啟動 `kafka` 和 `db` 兩個 services。
- `9092:9092` 及 `5432:5432` 將 host ports 映射到容器，讓 Windows 上的 Python 可連線。
- Kafka 用 KRaft mode，單一 node 同時做 broker/controller；練習版只有 1 broker、1 partition。
- PostgreSQL 第一次初始化資料目錄時會執行 `schema.sql`。
- named volumes 將資料保存在 container 以外，普通 `docker compose down` 不會刪除資料。
- `localhost` 是供 Windows host 程式連線的 advertised address。Python 程式不要設定成 `kafka` 或 `db`；那些名稱只適用於其他 Compose containers 互相連線。

### 18. 檢查 Compose 設定，再啟動 services

在 PowerShell 確認 terminal 目前位於專案資料夾，並且 `.env`、`docker-compose.yml`、`schema.sql` 已儲存。然後執行：

```powershell
docker compose config
```

預期：畫面輸出合併後的 Compose 設定，沒有 YAML parse error。注意輸出會顯示本機開發密碼；不要把輸出貼到公開位置。

成功後才執行：

```powershell
docker compose up -d
docker compose ps
```

第一次 `up` 會從網絡下載 `apache/kafka:4.0.0` 和 `postgres:17` images，可能要幾分鐘。`-d` 表示 services 在背景執行。

預期 `docker compose ps` 列出 `kafka` 和 `db`。PostgreSQL 的 **STATUS** 應先變成 `Up ... (healthy)`；Kafka 顯示 `Up`。如果 db 尚在 `health: starting`，等數秒再執行 `docker compose ps`。

查看 startup logs：

```powershell
docker compose logs --tail 50 kafka
docker compose logs --tail 50 db
```

要即時跟隨 logs，可用 `docker compose logs -f kafka`；按 `Ctrl+C` 只會停止跟隨 logs，不會停止 Kafka。

### 19. 確認 PostgreSQL table 及建立 Kafka topic

先確認資料表：

```powershell
docker compose exec db psql -U de_user -d orders -c "\dt"
```

預期列出 `orders` table。若 `orders` 不在清單，先看 `docker compose logs db`；PostgreSQL 的 init SQL 只會在空白資料 volume 首次初始化時執行。

建立 `orders` topic（重複執行亦安全）：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 1 --replication-factor 1
```

再檢查 topic：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

預期看到 `Topic: orders` 及 `PartitionCount: 1`。topic 是 Kafka 內訊息的分類/串流名稱；這條練習只建立一個 partition，方便先理解基本流程。

---

## 第四部分：手動建立 Python 程式及測試

### 20. 建立 `event.py`：事件格式、驗證及範例資料產生器

在 Explorer 建立 `event.py`，貼上：

```python
from datetime import datetime, timezone
import math


def validate_order(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("event must be a JSON object")

    required = ("order_id", "event_time", "customer_id", "items", "amount", "currency")
    missing = [field for field in required if field not in payload]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")

    for field in ("order_id", "customer_id", "currency"):
        value = payload[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string")

    if len(payload["currency"]) != 3:
        raise ValueError("currency must contain exactly 3 characters")

    event_time = payload["event_time"]
    if not isinstance(event_time, str):
        raise ValueError("event_time must be an ISO 8601 string")
    try:
        parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time must be a valid ISO 8601 timestamp") from exc
    if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
        raise ValueError("event_time must include a timezone")

    items = payload["items"]
    if not isinstance(items, list):
        raise ValueError("items must be a list")

    amount = payload["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise ValueError("amount must be a number")
    if not math.isfinite(amount) or amount < 0:
        raise ValueError("amount must be a finite non-negative number")

    return payload


def make_order(sequence: int) -> dict[str, object]:
    return {
        "order_id": f"order-{sequence:06d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"customer-{sequence % 100:03d}",
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
```

**解釋：** Producer 和 consumer 共用同一個驗證規則。`event_time` 必須有時區；這樣資料庫不會收到含糊的本地時間。`order_id` 會由序號產生；重跑相同 producer command 會再次產生相同 ID，剛好可示範冪等寫入。

### 21. 建立 `tests` 資料夾及 `tests/test_event.py`

在 Explorer 按 **New Folder**，建立 `tests`。在 `tests` 內建立 `test_event.py`，貼上：

```python
from copy import deepcopy

import pytest

from event import make_order, validate_order


def valid_order() -> dict[str, object]:
    return {
        "order_id": "order-test",
        "event_time": "2026-10-13T02:00:00+00:00",
        "customer_id": "customer-012",
        "items": [{"sku": "item-004", "quantity": 2, "unit_price": 19.9}],
        "amount": 39.8,
        "currency": "HKD",
    }


def test_valid_order_is_returned() -> None:
    order = valid_order()
    assert validate_order(order) is order


@pytest.mark.parametrize(
    "field",
    ["order_id", "event_time", "customer_id", "items", "amount", "currency"],
)
def test_missing_required_field_is_rejected(field: str) -> None:
    order = valid_order()
    del order[field]
    with pytest.raises(ValueError, match=field):
        validate_order(order)


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
def test_invalid_field_is_rejected(
    field: str, value: object, message: str
) -> None:
    order = valid_order()
    order[field] = value
    with pytest.raises(ValueError, match=message):
        validate_order(order)


def test_non_object_event_is_rejected() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        validate_order(["not", "an", "object"])


def test_make_order_creates_utc_order() -> None:
    order = make_order(1)
    assert order["order_id"] == "order-000001"
    assert order["currency"] == "HKD"
    assert order["amount"] >= 0
    assert validate_order(deepcopy(order))["event_time"] == order["event_time"]
    assert str(order["event_time"]).endswith("+00:00")
```

儲存後執行純 unit test：

```powershell
python -m pytest tests/test_event.py -v
```

預期所有測試名稱前有 `PASSED`，最後顯示 `passed`。這些測試只檢查 Python 事件驗證，不需要 Kafka 或 PostgreSQL。

### 22. 建立 `db.py`：PostgreSQL 連線及冪等寫入

在專案根目錄建立 `db.py`，貼上：

```python
import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from event import validate_order


load_dotenv()


def get_connection() -> psycopg.Connection[object]:
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "orders"),
        user=os.environ.get("POSTGRES_USER", "de_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "local_dev_only"),
    )


def insert_order(order: dict[str, object]) -> bool:
    validate_order(order)
    event_time = datetime.fromisoformat(
        str(order["event_time"]).replace("Z", "+00:00")
    )

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO orders
                    (order_id, event_time, customer_id, items, amount, currency)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                RETURNING order_id
                """,
                (
                    order["order_id"],
                    event_time,
                    order["customer_id"],
                    Jsonb(order["items"]),
                    order["amount"],
                    order["currency"],
                ),
            )
            return cursor.fetchone() is not None
```

**解釋：**

- SQL 使用 `%s` 參數，不要把事件值直接拼入 SQL 字串。
- `ON CONFLICT (order_id) DO NOTHING` 令同一訂單重播時不會多插入一行。
- 函式回傳 `True` 表示新資料成功插入；回傳 `False` 表示此 `order_id` 已存在。資料庫連線/SQL 錯誤會明確拋出，不會假裝成功。
- `.env` 載入後，連線預設 host 是 `localhost`，因為程式在 Windows 執行。

### 23. 建立 `tests/test_db.py`：資料庫 integration test

在 `tests` 資料夾建立 `test_db.py`：

```python
from datetime import datetime, timezone
from uuid import uuid4

from db import get_connection, insert_order


def test_insert_order_is_idempotent() -> None:
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
        assert insert_order(order) is True
        assert insert_order(order) is False
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                result = cursor.fetchone()
                assert result is not None
                assert result[0] == 1
    finally:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM orders WHERE order_id = %s",
                    (order_id,),
                )
```

此測試必須在 PostgreSQL service healthy 後執行，因為它會實際連線資料庫。每次用唯一測試 ID，並於 `finally` 清除該筆資料。

執行：

```powershell
python -m pytest tests/test_db.py -v
```

預期 `test_insert_order_is_idempotent PASSED`。若失敗是 connection refused，先執行 `docker compose ps`，確認 db 是 `healthy`；若測試因為 table 不存在而失敗，檢查 PostgreSQL logs 及 schema 初始化情況。

### 24. 建立 `producer.py`：發送模擬訂單

在專案根目錄建立 `producer.py`：

```python
import argparse
import json
import os
import sys
import time

from confluent_kafka import KafkaError, Message, Producer
from dotenv import load_dotenv

from event import make_order


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be 0 or greater")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish sample orders to Kafka")
    parser.add_argument("--count", type=positive_int, default=10)
    parser.add_argument("--interval", type=non_negative_float, default=1.0)
    args = parser.parse_args()

    load_dotenv()
    topic = os.environ.get("KAFKA_TOPIC", "orders")
    producer = Producer(
        {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    )
    delivery_errors: list[str] = []

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            delivery_errors.append(str(error))
            print(f"Delivery failed: {error}", file=sys.stderr)
            return
        print(f"Delivered order key={message.key().decode('utf-8')} to {message.topic()}")

    for sequence in range(1, args.count + 1):
        order = make_order(sequence)
        encoded_order = json.dumps(order, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic,
            key=str(order["order_id"]).encode("utf-8"),
            value=encoded_order,
            callback=on_delivery,
        )
        producer.poll(0)
        if sequence < args.count:
            time.sleep(args.interval)

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
```

儲存後，在 terminal 檢查 CLI 說明：

```powershell
python producer.py --help
```

預期列出 `--count` 及 `--interval`。`--count` 必須大於 0；`--interval` 是每筆訂單間隔秒數，可以是 0。

### 25. 建立 `consumer.py`：消費、寫入、成功後提交 offset

在專案根目錄建立 `consumer.py`：

```python
import json
import logging
import os
import time

import psycopg
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from db import insert_order
from event import validate_order


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("orders-consumer")


def persist_with_retries(order: dict[str, object], attempts: int = 3) -> bool:
    for attempt in range(1, attempts + 1):
        try:
            return insert_order(order)
        except psycopg.Error:
            logger.exception("Database write failed (attempt %s/%s)", attempt, attempts)
            if attempt == attempts:
                raise
            time.sleep(attempt)
    raise RuntimeError("database retry loop exited unexpectedly")


def main() -> None:
    load_dotenv()
    topic = os.environ.get("KAFKA_TOPIC", "orders")
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            "group.id": os.environ.get("KAFKA_GROUP_ID", "orders-to-postgres"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    logger.info("Subscribed to topic '%s'; press Ctrl+C to stop", topic)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka message error: {message.error()}")

            location = (
                f"topic={message.topic()} "
                f"partition={message.partition()} offset={message.offset()}"
            )
            try:
                value = message.value()
                if value is None:
                    raise ValueError("message value must not be null")
                payload = json.loads(value.decode("utf-8"))
                order = validate_order(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                logger.exception("Invalid message; stopping without committing (%s)", location)
                raise

            inserted = persist_with_retries(order)
            committed = consumer.commit(message=message, asynchronous=False)
            if committed is not None:
                commit_errors = [partition.error for partition in committed if partition.error]
                if commit_errors:
                    raise RuntimeError(f"Kafka offset commit failed: {commit_errors}")

            result = "inserted" if inserted else "duplicate (already stored)"
            logger.info("Saved order_id=%s (%s); offset committed (%s)", order["order_id"], result, location)
    except KeyboardInterrupt:
        logger.info("Stopping consumer after Ctrl+C")
    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    main()
```

**重點解釋：**

1. `auto.offset.reset=earliest` 只對這個 group **尚未有已提交 offset** 時生效；不會令已有 offset 的 group 每次都由頭讀。
2. `enable.auto.commit=False` 關閉自動提交，避免資料尚未成功寫入 PostgreSQL，Kafka 卻已記錄「已處理」。
3. consumer 先驗證、再寫 PostgreSQL、最後才 commit offset。相同 `order_id` 雖然可能是 duplicate，只要 DB 明確確認已存在，就可以安全 commit。
4. DB 最多重試 3 次，每次失敗會記錄錯誤並等候。重試耗盡會停止程式，不會提交該筆 offset。
5. 無效 JSON/事件會記錄 topic、partition、offset 並停止，不會靜默略過。該 partition 可能會在下次啟動時再次遇到同一筆 poison message；修正/處理訊息後才可繼續。
6. `Ctrl+C` 會進入 `finally` 關閉 consumer。

---

## 第五部分：端對端運行與驗收

### 26. 確認檔案結構

在 VS Code Explorer 確認大致有以下檔案：

```text
kafka-learning-project/
├── .env
├── .env.example
├── .gitignore
├── consumer.py
├── db.py
├── docker-compose.yml
├── event.py
├── producer.py
├── requirements.txt
├── schema.sql
└── tests/
    ├── test_db.py
    └── test_event.py
```

確認 `.env` 存在但被 `.gitignore` 排除：

```powershell
git status --short
```

`git status` 不應列出 `.env` 或 `.venv`。如果有，先檢查 `.gitignore` 和檔案名稱，暫時不要 commit。

### 27. 再跑一次 unit tests 及 DB integration test

先確認 Compose services 狀態：

```powershell
docker compose ps
```

再跑測試：

```powershell
python -m pytest tests/test_event.py -v
python -m pytest tests/test_db.py -v
```

第一個測試不依賴 Docker；第二個測試需要 db healthy。若 db 測試失敗，先看：

```powershell
docker compose logs --tail 80 db
```

### 28. 在 topic 發送及檢視第一批訊息

先送 5 筆訂單：

```powershell
python producer.py --count 5 --interval 0
```

預期每筆有 `Delivered order key=order-...`，最後顯示 `Successfully delivered 5 order(s) to topic 'orders'`。

用 Kafka console consumer 由 topic 開頭讀 5 筆：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5
```

預期見到 5 行 JSON，每行有 `order_id`、`event_time`、`customer_id`、`items`、`amount`、`currency`。這個 console consumer 只作檢視，不寫入 PostgreSQL，也不使用 Python consumer 的 group。

> **新手常見混淆：** 這 5 筆訊息仍留在 Kafka。稍後第一次啟動 `orders-to-postgres` group 時，因為它沒有 committed offset 且設定 `earliest`，Python consumer 會從頭讀取這 5 筆。所以稍後的 DB 初始計數會先包含這 5 筆，而不是只計算之後發送的訊息。

### 29. 開兩個 terminal 啟動 Python consumer 和 producer

在 VS Code 選 **Terminal → New Terminal** 兩次，建立兩個 PowerShell terminal。每個新 terminal 都輸入以下指令確認位置並啟動 `.venv`（若提示執行政策阻止，就沿用第 10 步的完整 Python 路徑做法）：

```powershell
Get-Location
.\.venv\Scripts\Activate.ps1
```

確保兩個 terminal 都位於 project folder，且都使用 `.venv`。

**Terminal A：先啟動 consumer**

```powershell
python consumer.py
```

預期看到：

```text
Subscribed to topic 'orders'; press Ctrl+C to stop
```

之後它會讀入之前 5 筆訊息、寫入 PostgreSQL，並顯示 `Saved order_id=...` 和 `offset committed`。保持 Terminal A 開啟。

**Terminal B：發送另外 10 筆**

```powershell
python producer.py --count 10 --interval 0
```

預期 producer 報告 10 筆成功；Terminal A 陸續顯示 10 筆訊息已處理。第二批的 `order-000001` 至 `order-000005` 與第一批重複，會被當成 duplicate；只有 `order-000006` 至 `order-000010` 是新 row。因此兩批合計有 **10 個不同訂單**，不是 15 個。

**先開 consumer 的原因：** 一般教學以 consumer 先訂閱、producer 再開始發訊息較容易觀察即時流動。即使 consumer 未啟動時 Kafka 仍會保存訊息，但新 consumer group 的起始位置受 offset 設定影響。

### 30. 用 SQL 查詢 PostgreSQL

開第三個 terminal（或先停止 Terminal B 的命令後使用），在 project folder 輸入：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT COUNT(*) FROM orders;"
```

若剛才依次發送第一批 5 筆及第二批 10 筆，預期 count 為 `10`：第二批重用了前 5 個 ID，只有後 5 個是新訂單。

查看訂單摘要：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT order_id, event_time, customer_id, amount, currency FROM orders ORDER BY event_time;"
```

預期看到每筆訂單的 ID、含時區的時間、客戶、金額及 `HKD`。查看商品 JSON：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT order_id, items FROM orders ORDER BY event_time LIMIT 5;"
```

`psql -c` 會執行引號內的 SQL 並退出。`\dt` 是 psql meta-command，只能直接傳給 psql，不是一般 SQL。

### 31. 示範重複事件與冪等寫入

保持 Terminal A 的 consumer 開啟，再於另一個 terminal 重跑同一個 producer command：

```powershell
python producer.py --count 10 --interval 0
```

producer 再次產生 `order-000001` 至 `order-000010`，Kafka 會接收這些訊息；consumer 會看到 `duplicate (already stored)`，但仍會在資料庫確認成功後提交 offset。

再次查行數：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT COUNT(*) FROM orders;"
```

行數應保持不變。這是因為 PostgreSQL 的 primary key 和 `ON CONFLICT DO NOTHING` 阻止相同 `order_id` 重複插入。重播事件的 event_time 會更新，但資料庫不會用它覆蓋已保存的 row。

### 32. 示範 consumer group offset 會延續

1. 在 Terminal A 按 `Ctrl+C`。
2. 等候看到 `Consumer closed`。
3. 再次執行：

   ```powershell
   python consumer.py
   ```

4. 因 `orders-to-postgres` group 已有 committed offset，consumer 應由上次成功位置之後開始，而不是每次都從 topic 開頭重播全部資料。
5. 再用 SQL 查 `COUNT(*)`；已經保存的資料不會因為重啟 consumer 而增加。

每個 consumer group 有自己的 offset。刪除 volume、改 group ID 或刪 topic 都會改變這個練習的狀態，不要用它們來「隨便重置」而不先理解後果。

### 33. 查 Kafka group 及服務 logs

檢查 group 狀態：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-to-postgres
```

預期表格會列出 topic、partition、CURRENT-OFFSET、LOG-END-OFFSET 及 LAG。consumer 正在執行時可能見到 partition assignment；停止 consumer 時可能暫時顯示 group 沒有 active member。

查看最近的 container logs：

```powershell
docker compose logs --tail 100 kafka
docker compose logs --tail 100 db
```

Python consumer 的 logs 在 Terminal A；producer 的 delivery logs 在執行 producer 的 terminal。

### 34. 錯誤訊息如何處理

- **Producer 無法連 Kafka：** 看 `docker compose ps`，確認 Kafka 是 `Up`；看 `docker compose logs --tail 100 kafka`；確認 `.env` 的 `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`。
- **Consumer 報資料庫連線失敗：** 確認 db 是 `healthy`、`.env` 的 host/port/user/password 與 Compose 一致，然後看 `docker compose logs db`。consumer 會有限次重試，仍失敗便退出，該訊息 offset 不會提交。
- **JSON/欄位驗證錯誤：** consumer log 會有 topic、partition、offset。它會停止且不提交 offset，避免錯誤訊息被當作成功處理。修正/移除 poison message 需要明確處理；不要直接吞掉 exception。
- **PostgreSQL table 不存在：** 確認 `schema.sql` 位置、Compose bind mount，以及資料庫初始化 logs。PostgreSQL init scripts 只會在 data directory 首次初始化時執行。
- **Port already allocated：** 可能有本機程式已使用 `5432` 或 `9092`。先找出並正常停止佔用程式；不要未理解就更改 Compose port，因為 Python `.env` 也要一起對應。
- **`docker compose config` 顯示 `${...}`/設定錯誤：** 確認已在 project folder、`.env` 存在且檔名沒有變成 `.env.txt`。
- **PowerShell 找不到 `python` 或套件：** 重新選 `.venv` interpreter；確認 terminal 在 project folder，前面有 `(.venv)`。
- **Topic 已存在：** `--create-if-not-exists` 是安全的；再執行 `--describe` 確認 partition 數。

在這個簡化流程中，無效訊息會阻塞該 partition；後續延伸可新增 DLQ topic，但要先設計好「成功寫入 DLQ 後才提交原 topic offset」。

---

## 第六部分：停止服務、保存狀態及清理

### 35. 正常停止 Python 程式

1. 在 consumer terminal 按 `Ctrl+C`。
2. 確認看到 `Consumer closed`。
3. Producer 是短命命令，完成後會自行退出；若仍在執行，等它完成或按 `Ctrl+C`。

### 36. 停止 Docker services（保留資料）

在 project folder 的 PowerShell terminal 執行：

```powershell
docker compose down
```

預期 containers 被停止/移除。Kafka 和 PostgreSQL 的 named volumes 保留，所以之後 `docker compose up -d` 會沿用原資料及 offset。

下次繼續練習時：

```powershell
docker compose up -d
docker compose ps
```

等 db healthy 後即可繼續。

### 37. 完全刪除練習資料（可選，具破壞性）

**只有在你真的要清除 Kafka topic 資料、consumer offsets 及 PostgreSQL 所有訂單，並由全新狀態重做時才執行。**

先確認自己位於正確的練習 project folder，然後輸入：

```powershell
docker compose down -v
```

此命令會刪除本練習的 `kafka_data` 及 `postgres_data` volumes。刪除後不能從這些本機 volumes 還原資料。下次啟動 PostgreSQL 時會重新執行 `schema.sql`。

若你只想暫停電腦或稍後繼續，**不要**使用 `-v`。

### 38. 可選：做本機 Git checkpoint

如想保存目前程式版本，在 project folder 執行：

```powershell
git status --short
git add .gitignore .env.example requirements.txt docker-compose.yml schema.sql event.py db.py producer.py consumer.py tests
git status --short
```

仔細檢查 staged files。確認沒有 `.env`、`.venv`、密碼、資料庫資料後，才可 commit：

```powershell
git commit -m "feat: build local kafka orders pipeline"
```

若 `git commit` 提示未設定 user name/email，按提示只設定你自己的 Git identity；不要把練習 repository 推送到遠端，除非你確定要公開分享。

---

## 完成檢查表

- [ ] `py -3.12 --version` 顯示 Python 3.12。
- [ ] `git --version`、`docker --version`、`docker compose version` 成功。
- [ ] `docker info` 能顯示 Server。
- [ ] VS Code 使用專案 `.venv` interpreter。
- [ ] `.env` 存在，`.gitignore` 排除 `.env` 和 `.venv`。
- [ ] `docker compose ps` 顯示 Kafka `Up`、PostgreSQL `healthy`。
- [ ] Kafka topic `orders` 存在，1 partition。
- [ ] `tests/test_event.py` unit tests 通過。
- [ ] `tests/test_db.py` integration test 通過。
- [ ] Producer 能把有效 JSON 訂單送入 Kafka。
- [ ] Consumer 能讀取訊息、驗證並寫入 PostgreSQL。
- [ ] PostgreSQL SQL 查詢結果與訂單數量吻合。
- [ ] 重播相同 `order_id` 不會增加資料列。
- [ ] 重啟相同 consumer group 後會延續已提交的 offset。
- [ ] 明白 `docker compose down` 保留 volumes，而 `docker compose down -v` 會刪除資料。

## 建議下一步延伸

完成並理解以上流程後，才考慮：

1. 增加 topic partitions，並用兩個同 group consumer 觀察 partition assignment。
2. 加入 dead-letter topic，並規定訊息寫入 DLQ 成功後才提交原 topic offset。
3. 將 Python producer/consumer 放進 Docker Compose，練習 Compose service DNS/networking。
4. 加事件 schema version、更完整的 items 欄位驗證及測試。
5. 加 CI、SQL 聚合查詢或 dashboard；雲端 Kafka/Spark/Flink 留到本機流程穩定之後。
