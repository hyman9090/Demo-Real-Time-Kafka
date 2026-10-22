# Windows 10 手動搭建 Kafka 即時訂單管道：由下載到端對端驗收

> 本教學以「你親手做一次」為目標。請逐步閱讀、自己下載、自己按安裝畫面、自己建立檔案及輸入指令。不要一次過貼上所有 commands；每個步驟都有預期結果，確認後才繼續。
>
> 本文件根據 [Kafka 學習 Project 設計](../superpowers/specs/2026-10-13-kafka-learning-project-design.md) 及 [Implementation Plan](../superpowers/plans/2026-10-13-kafka-learning-project.md) 編寫。為令新手可由零完成，以下亦提供各個練習檔案的完整範例內容。這些程式碼是你要親手建立的練習材料；本教學不會替你安裝軟件、執行 setup、建立或啟動任何服務。

## 你完成後會有甚麼

本機會運行一條簡化的即時資料管道：

```text
src/demo_real_time_kafka/producer.py（Windows 上的 Python）
      │  JSON 訂單（各發一份到兩個 topic）
      ▼
Kafka topics：orders、orders-audit（各 2 partitions）
      ├── orders → orders-to-postgres group（1 個 consumer）
      └── orders-audit → orders-audit-1、orders-audit-2 groups
                         （兩個 group 各自收到完整訊息）
      │  驗證、寫入成功後提交各 group 的 offset
      ▼
PostgreSQL 資料庫：orders（Docker container）
```

你會學到 producer、兩個 topic、每個 topic 的兩個 partition、三個 consumer group、offset、冪等寫入、Docker Compose、基本錯誤處理，以及用 SQL 驗證資料。

`orders-audit` 在這個練習中是展示 topic fan-out 的副本，內容與 `orders` 相同；它不是另一種 audit event schema。

**本教學第一版不會做** dashboard、雲端部署、Spark/Flink、Kubernetes、Schema Registry 或 dead-letter topic。Kafka 和 PostgreSQL 在 Docker 中執行；Python 程式在 Windows host 執行，避免一開始同時處理容器內 Python networking。

## 重要約定及安全提醒

- 所有指令以 **VS Code 的 PowerShell terminal** 為準。不是 Command Prompt，也不是 Git Bash。
- 專案資料夾建議放在本機短路徑，例如 `C:\Users\你的Windows帳戶名稱\kafka-learning-project`；避免 OneDrive 同步目錄、空格及太深的路徑。
- 本教學使用 Python 3.14 或以上，由 uv 管理 Python 版本及專案環境；Kafka image 固定為 `apache/kafka:4.0.0`、PostgreSQL image 固定為 `postgres:17`。不要自行改成 `latest`。
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

### 3. 安裝 uv

本教學以 uv 統一管理所需 Python 版本、專案 `.venv`、依賴及 lockfile。

1. 在 Windows 開啟 **Start**，搜尋並開啟 **Terminal** 或 **PowerShell**。
2. 輸入以下指令安裝 uv：

   ```powershell
   winget install --id astral-sh.uv -e
   ```

3. 安裝完成後，關閉並重新開啟 VS Code，選 **Terminal → New Terminal**，確保使用 PowerShell。
4. 輸入：

   ```powershell
   uv --version
   ```

   預期會顯示 uv 版本。若 `uv` 未能識別，重新開啟 terminal；仍有問題就確認 WinGet 安裝已完成，並按 [uv 官方安裝說明](https://docs.astral.sh/uv/getting-started/installation/) 排查。

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
2. 按 **Download Docker Desktop**，按你的電腦架構選 Windows 安裝程式（如你的 System type 是 ARM，先查 Docker 官方支援說明）。
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

### 10. 初始化 uv Python 專案

在專案資料夾初始化 uv package project，並指定 Python 3.14。這樣會建立 `src/demo_real_time_kafka/`，Python 模組放入 package，而不是放在專案根目錄：

```powershell
uv init --package --name demo-real-time-kafka --python 3.14
uv sync
uv run python --version
```

`uv init --package` 會建立 `pyproject.toml` 及 `src/demo_real_time_kafka/` package；`uv sync` 會按設定取得 Python、建立 `.venv`，並產生 `uv.lock`。預期版本為 `Python 3.14.x` 或以上。日後新增/同步依賴時，無須啟動 `.venv`；用 `uv add` 和 `uv run` 即可。

uv 亦會在 `src/demo_real_time_kafka/` 建立 `__init__.py`，用來標示 Python package。這個教學不需要修改它；其餘程式模組會放在同一個資料夾，並透過 `demo_real_time_kafka` package 名稱匯入或執行。

### 11. 在 VS Code 選擇 `.venv` interpreter

1. 按 `Ctrl+Shift+P` 開啟 Command Palette。
2. 搜尋並選 **Python: Select Interpreter**。
3. 在清單選路徑含有本專案 `.venv\Scripts\python.exe` 的項目。
4. 如清單沒有 `.venv`，先在 project folder 執行 `uv sync`，再重新開啟 interpreter 清單。
5. VS Code 右下角/狀態列應顯示 Python 3.14 或以上及 `.venv`。這一步只供 VS Code 編輯器使用；在 terminal 執行程式時仍以 `uv run` 為準。

---

## 第三部分：親手建立設定檔與服務

### 12. 在 VS Code 建立檔案的方法

接下來每個檔案都由你在 VS Code 建立。Python 程式檔放在 `src/demo_real_time_kafka/`；Compose、SQL、環境設定及測試檔則放在專案根目錄或 `tests/`：

1. 在 Explorer 左側，按 **New File** 圖示；或按 **File → New Text File**。
2. 輸入指定檔名（包括副檔名），例如 `.gitignore`，按 Enter。
3. 將相應 code block 內容複製到編輯器。
4. 按 `Ctrl+S` 儲存。
5. 如 VS Code 問要不要新增副檔名/切換語言，保持檔名正確即可。

如果要建立 `tests` 子資料夾，在 Explorer 按 **New Folder**，輸入 `tests`，再於該資料夾中建立測試檔案。

### 13. 建立 `.gitignore`

如 `.gitignore` 已由 uv 建立，打開它並合併以下規則；否則新建 `.gitignore`。保留檔案內其他需要的規則：

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
KAFKA_TOPICS=orders,orders-audit
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

### 15. 用 uv 加入及同步 Python 套件

在 project folder 輸入：

```powershell
uv add confluent-kafka "psycopg[binary]" python-dotenv
uv add --dev pytest
uv sync --locked
uv run python -c "import confluent_kafka, psycopg, dotenv, pytest; print('dependencies OK')"
```

預期最後一行顯示 `dependencies OK`。`uv add` 會將直接依賴記錄在 `pyproject.toml`、更新 `uv.lock`，並同步 `.venv`；`pytest` 會列為開發依賴。之後用 `uv sync` 按 lockfile 還原環境，用 `uv run ...` 在該環境執行指令。毋須手動固定每個套件版本，`uv.lock` 會鎖定實際解析出的版本。

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
      KAFKA_NUM_PARTITIONS: 2
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
- Kafka 用 KRaft mode，單一 node 同時做 broker/controller；練習版有 1 broker，每個 topic 有 2 partitions。
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

分別建立 `orders` 和 `orders-audit` topic，每個 topic 有 2 partitions（重複執行亦安全）：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 2 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders-audit --partitions 2 --replication-factor 1
```

先查看兩個 topic 的 partition 數：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders-audit
```

`--create-if-not-exists` 不會更改已存在的 topic。**只對 `PartitionCount` 少於 2 的 topic** 執行相應的 `--alter` 指令；已經是 2 partitions 的 topic 不要再 alter。partition 數只可增加，不可減少。

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic orders --partitions 2
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic orders-audit --partitions 2
```

若某個既有 topic 已經多於 2 partitions，Kafka 不支援縮減；保留現有設定或以全新 topic 開始練習。完成後再次執行以上兩條 `--describe` 指令確認兩個結果都有 `PartitionCount: 2`。topic 是 Kafka 內訊息的分類/串流名稱；partition 讓 topic 的訊息分開儲存及分配。此設定只有一個 broker，所以 replication factor 保持 1。

---

## 第四部分：手動建立 Python 程式及測試

### 20. 建立 `src/demo_real_time_kafka/event.py`：事件格式、驗證及範例資料產生器

在 Explorer 展開 `src/demo_real_time_kafka/`，於該 package 建立 `event.py`，貼上：

```python
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
```

**程式解釋：**

- `validate_order(payload)` 接收一個待檢查的事件，逐項確認它是 JSON object、必要欄位齊全，而且各欄位格式符合預期；不符合時會拋出 `ValueError`，讓呼叫端明確處理，而不是把壞資料當成有效訂單。
- `event_time` 會先轉成 Python datetime 解析，並要求帶有時區；這樣資料庫不會收到含糊的本地時間。金額亦會檢查為有限、非負數，並特別拒絕 Python 中屬於 `bool` 子類別的 `True`/`False`。
- `make_order(sequence)` 依序號產生一筆可重複的範例訂單；時間是目前 UTC 時間，`order_id` 則由序號決定。因此重跑相同 producer command 會再產生相同 ID，可用來示範冪等寫入。
- Producer 和 consumer 都使用這個模組：producer 用 `make_order()` 造資料，consumer 和資料庫寫入前則用 `validate_order()` 驗證。

### 21. 建立 `tests` 資料夾及 `tests/test_event.py`

在 Explorer 按 **New Folder**，建立 `tests`。在 `tests` 內建立 `test_event.py`，貼上：

```python
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
def test_invalid_field_is_rejected(
    field: str, value: object, message: str
) -> None:
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
```

**程式解釋：**

- `valid_order()` 提供一筆固定的合法測試資料；每個測試都可修改它，不用重複手寫整個事件。
- `test_valid_order_is_returned()` 確認有效事件會通過驗證，而且函式回傳原本的事件物件。
- 兩個 `pytest.mark.parametrize` 測試會把同一組測試函式套用到多個欄位案例：第一組逐一刪除必要欄位，第二組用空字串、無時區時間、錯誤型別及負數等值確認會被拒絕。
- `test_non_object_event_is_rejected()` 檢查傳入 list 而不是 object 時會報錯；`test_make_order_creates_utc_order()` 則檢查範例訂單的 ID、幣別、金額及 UTC 時間格式。
- 這些是 unit tests，只測試事件模組，不會連接 Kafka 或 PostgreSQL。

儲存後執行純 unit test：

```powershell
uv run pytest tests/test_event.py -v
```

預期所有測試名稱前有 `PASSED`，最後顯示 `passed`。這些測試只檢查 Python 事件驗證，不需要 Kafka 或 PostgreSQL。

### 22. 建立 `db.py`：PostgreSQL 連線及冪等寫入

在 `src/demo_real_time_kafka/` 建立 `db.py`，貼上：

```python
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
    event_time = datetime.fromisoformat(
        str(order["event_time"]).replace("Z", "+00:00")
    )

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
```

**解釋：**

- `get_connection()` 讀取 `.env` 中的 PostgreSQL 連線設定，並建立 psycopg connection；預設 host 是 `localhost`，因為 Python 程式在 Windows host 執行。
- `insert_order(order)` 先重用 `validate_order()` 檢查資料，再把時間轉成 datetime，並用 `Jsonb` 將 `items` 傳給 PostgreSQL 的 JSONB 欄位。
- SQL 使用 `%s` 參數，不要把事件值直接拼入 SQL 字串。
- `ON CONFLICT (order_id) DO NOTHING` 令同一訂單重播時不會多插入一行。
- 函式回傳 `True` 表示新資料成功插入；回傳 `False` 表示此 `order_id` 已存在。資料庫連線/SQL 錯誤會明確拋出，不會假裝成功。
- `with` 區塊結束時 psycopg 會完成交易並關閉該次 connection；資料庫錯誤會向上拋出，由 consumer 的重試邏輯處理。

### 23. 建立 `tests/test_db.py`：資料庫 integration test

在 `tests` 資料夾建立 `test_db.py`：

```python
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
```

此測試必須在 PostgreSQL service healthy 後執行，因為它會實際連線資料庫。每次用唯一測試 ID，並於 `finally` 清除該筆資料。

**程式解釋：**

- `test_insert_order_is_idempotent()` 用 UUID 建立本次專屬的 `order_id`，避免與之前測試或正式示範資料撞 ID。
- 第一次呼叫 `insert_order()` 預期回傳 `True`，第二次相同資料預期回傳 `False`；這同時驗證重複事件不會新增第二筆。
- 測試再用 `SELECT COUNT(*)` 確認資料庫只保存一列。`finally` 會在測試結束時刪除這個專屬測試 ID，避免測試資料留在 orders table。
- 因為測試實際執行 SQL，所以需要 PostgreSQL 已啟動且 schema 已建立；它不需要 Kafka。

執行：

```powershell
uv run pytest tests/test_db.py -v
```

預期 `test_insert_order_is_idempotent PASSED`。若失敗是 connection refused，先執行 `docker compose ps`，確認 db 是 `healthy`；若測試因為 table 不存在而失敗，檢查 PostgreSQL logs 及 schema 初始化情況。

### 24. 建立 `producer.py`：發送模擬訂單

在 `src/demo_real_time_kafka/` 建立 `producer.py`：

```python
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


def parse_topics(value: str) -> tuple[str, ...]:
    # 拒絕空 topic、空白項目及重複名稱，避免設定錯誤時靜默少發訊息。
    topics = tuple(topic.strip() for topic in value.split(","))
    if not topics or any(not topic for topic in topics):
        raise ValueError("KAFKA_TOPICS must contain non-empty comma-separated names")
    if len(set(topics)) != len(topics):
        raise ValueError("KAFKA_TOPICS must not contain duplicate names")
    return topics


def main() -> int:
    # 設定可由命令列調整的訂單數量和每筆發送間隔。
    parser = argparse.ArgumentParser(description="Publish sample orders to Kafka")
    parser.add_argument("--count", type=positive_int, default=10)
    parser.add_argument("--interval", type=non_negative_float, default=1.0)
    args = parser.parse_args()

    # 載入 .env，並使用預設 topics 和 broker 作為本機設定。
    load_dotenv()
    topics = parse_topics(os.environ.get("KAFKA_TOPICS", "orders,orders-audit"))
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
        for topic in topics:
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

    topic_list = "', '".join(topics)
    print(f"Successfully delivered {args.count} order(s) to topic(s) '{topic_list}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

**程式解釋：**

- `positive_int()` 和 `non_negative_float()` 是命令列參數檢查器，分別限制訂單數目必須大於 0、發送間隔不可小於 0；輸入不合法時 argparse 會顯示錯誤。
- `main()` 讀取 `--count` 和 `--interval`，載入 `.env` 的 `KAFKA_TOPICS`，再用 Kafka `Producer` 連接 broker。每次迴圈由 `make_order()` 產生一筆訂單，轉成 UTF-8 JSON，並以 `order_id` 作為 Kafka message key，將同一筆事件發送到兩個 topic。
- Kafka producer 的發送是非同步的；`on_delivery()` 是 delivery callback，等 broker 回報後才輸出成功或錯誤。`poll(0)` 讓 client 有機會處理已完成的 callback，`flush(10)` 則在程式結束前等候最多 10 秒清空尚未完成的訊息。
- 只要有訊息仍未完成或 callback 回報 delivery error，程式會以非零 exit code 結束；全部成功才輸出成功筆數並回傳 0。
- 最後的 `if __name__ == "__main__"` 令這個模組可用 `python -m demo_real_time_kafka.producer` 執行；`raise SystemExit(...)` 會把 `main()` 的回傳值交給作業系統作為程式結束狀態。

儲存後，在 terminal 檢查 CLI 說明：

```powershell
uv run python -m demo_real_time_kafka.producer --help
```

預期列出 `--count` 及 `--interval`。`--count` 必須大於 0；`--interval` 是每筆訂單間隔秒數，可以是 0。`KAFKA_TOPICS` 以逗號分隔 topic 名稱；預設每筆訂單都會發送到 `orders` 和 `orders-audit`。

### 25. 建立 `consumer.py`：消費、寫入、成功後提交 offset

在 `src/demo_real_time_kafka/` 建立 `consumer.py`：

```python
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
                logger.exception("Invalid message; stopping without committing (%s)", location)
                raise

            # 先確保資料庫已新增或確認 duplicate，再提交 Kafka offset。
            inserted = persist_with_retries(order)
            committed = consumer.commit(message=message, asynchronous=False)
            if committed is not None:
                # 同步 commit 可能回傳各分區結果，檢查並明確報告錯誤。
                commit_errors = [partition.error for partition in committed if partition.error]
                if commit_errors:
                    raise RuntimeError(f"Kafka offset commit failed: {commit_errors}")

            result = "inserted" if inserted else "duplicate (already stored)"
            logger.info("Saved order_id=%s (%s); offset committed (%s)", order["order_id"], result, location)
    except KeyboardInterrupt:
        logger.info("Stopping consumer after Ctrl+C")
    finally:
        # 不論正常停止或發生錯誤，都關閉 consumer 並離開 group。
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

**程式執行順序：**

- `main()` 載入環境設定，並以 `--topic` 和 `--group-id` 指定此 consumer group 的 topic 及 group ID；建立 Kafka consumer 後，持續 `poll()` 等待訊息。
- 每收到一筆訊息，程式先檢查 Kafka message error，再把 bytes 解碼為 UTF-8 JSON，並用 `validate_order()` 確認事件格式。無效訊息會記錄位置後拋錯，offset 不會提交。
- `persist_with_retries()` 只對 psycopg 資料庫錯誤重試，最多三次；每次失敗會記錄 traceback 並逐次延長等待時間。重試用盡後錯誤會向外拋出，該筆訊息不會 commit。
- 資料庫成功新增訂單或確認該 ID 已存在後，consumer 才同步提交該筆 Kafka offset；若 commit 回報錯誤，程式會明確報錯，而不會記錄成成功處理。
- 收到 `Ctrl+C` 時會進入 `finally`，呼叫 `consumer.close()` 釋放 consumer 資源；其他未預期錯誤則會向外傳出，方便看到失敗原因。

consumer 預設使用 `.env` 的 `KAFKA_TOPIC` 和 `KAFKA_GROUP_ID`；命令列參數可覆寫它們，讓三個 consumer group 同時以不同設定執行。檢查參數：

```powershell
uv run python -m demo_real_time_kafka.consumer --help
```

---

## 第五部分：端對端運行與驗收

### 26. 確認檔案結構

在 VS Code Explorer 確認大致有以下檔案：

```text
kafka-learning-project/
├── .env
├── .env.example
├── .gitignore
├── .python-version
├── docker-compose.yml
├── pyproject.toml
├── README.md
├── schema.sql
├── src/
│   └── demo_real_time_kafka/
│       ├── __init__.py
│       ├── consumer.py
│       ├── db.py
│       ├── event.py
│       └── producer.py
├── uv.lock
└── tests/
    ├── test_db.py
    ├── test_event.py
    └── test_producer.py
```

確認 `.env` 存在但被 `.gitignore` 排除：

```powershell
git status --short
```

`git status` 不應列出 `.env` 或 `.venv`。如果有，先檢查 `.gitignore` 和檔案名稱，暫時不要 commit。

### 27. 再跑一次 unit tests 及 DB integration test

在 `tests` 資料夾建立 `test_producer.py`，貼上：

```python
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
```

先執行不需要 Docker 的 unit tests：

```powershell
uv run pytest tests/test_event.py tests/test_producer.py -v
```

兩個測試檔都應通過；`test_producer.py` 會確認逗號分隔 topic 設定、空名稱及重複名稱的處理。

再確認 Compose services 狀態：

```powershell
docker compose ps
```

再跑測試：

```powershell
uv run pytest tests/test_db.py -v
```

DB integration test 需要 db healthy。若 db 測試失敗，先看：

```powershell
docker compose logs --tail 80 db
```

### 28. 在兩個 topic 發送及檢視第一批訊息

先送 5 筆訂單：

```powershell
uv run python -m demo_real_time_kafka.producer --count 5 --interval 0
```

預期每筆訂單會分別顯示已送到 `orders` 和 `orders-audit`，最後顯示 `Successfully delivered 5 order(s) to topic(s) 'orders', 'orders-audit'`。

分別用 Kafka console consumer 由兩個 topic 開頭讀 5 筆：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders-audit --from-beginning --max-messages 5
```

每個指令預期見到 5 行 JSON，每行有 `order_id`、`event_time`、`customer_id`、`items`、`amount`、`currency`。這個 console consumer 只作檢視，不寫入 PostgreSQL，也不使用 Python consumer 的 group。

> **新手常見混淆：** 每個 topic 的 5 筆訊息仍留在 Kafka。稍後三個新 consumer group 都會由各自 topic 的開頭讀取；`orders-audit-1` 和 `orders-audit-2` 是不同 group，因此兩者都會各自收到 `orders-audit` 的全部訊息。

### 29. 開四個 terminal 啟動三個 consumer group 和 producer

在 VS Code 選 **Terminal → New Terminal** 四次，建立四個 PowerShell terminal。每個新 terminal 都輸入以下指令確認位置，並同步 uv 環境：

```powershell
Get-Location
uv sync
```

確保四個 terminal 都位於 project folder。`uv run` 會自動使用 `.venv`，毋須手動啟動虛擬環境。

**Terminal A：啟動消費 `orders` 的 group**

```powershell
uv run python -m demo_real_time_kafka.consumer --topic orders --group-id orders-to-postgres
```

預期看到：

```text
Group 'orders-to-postgres' subscribed to topic 'orders'; press Ctrl+C to stop
```

**Terminal B、C：分別啟動兩個消費 `orders-audit` 的 group**

Terminal B：

```powershell
uv run python -m demo_real_time_kafka.consumer --topic orders-audit --group-id orders-audit-1
```

Terminal C：

```powershell
uv run python -m demo_real_time_kafka.consumer --topic orders-audit --group-id orders-audit-2
```

三個 terminal 都應顯示各自的 group ID 和 topic，並讀入前面已發送的 5 筆訊息。保持三個 consumer 開啟。

**Terminal D：發送另外 10 筆**

```powershell
uv run python -m demo_real_time_kafka.producer --count 10 --interval 0
```

producer 會把每筆訂單分別發到兩個 topic。`orders-to-postgres` 只消費 `orders`；另外兩個 group 都獨立消費 `orders-audit`。第二批的 `order-000001` 至 `order-000005` 與第一批重複，會被當成 duplicate；只有 `order-000006` 至 `order-000010` 是新 row。因此三個 group 處理後，資料庫合計有 **10 個不同訂單**，不是 15 個或 30 個。

**先開 consumer 的原因：** 一般教學以 consumer 先訂閱、producer 再開始發訊息較容易觀察即時流動。即使 consumer 未啟動時 Kafka 仍會保存訊息，但每個新 consumer group 的起始位置受 offset 設定影響。

### 30. 用 SQL 查詢 PostgreSQL

開另一個 terminal（或等 Terminal D 的 producer 完成後使用），在 project folder 輸入：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT COUNT(*) FROM orders;"
```

若剛才依次發送第一批 5 筆及第二批 10 筆，預期 count 為 `10`：第二批重用了前 5 個 ID，只有後 5 個是新訂單。三個 group 即使各自處理同一批訂單，資料庫 primary key 仍會阻止重複 row。

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

保持三個 consumer terminal 開啟，再於另一個 terminal 重跑同一個 producer command：

```powershell
uv run python -m demo_real_time_kafka.producer --count 10 --interval 0
```

producer 再次產生 `order-000001` 至 `order-000010`，Kafka 會接收這些訊息；consumer 會看到 `duplicate (already stored)`，但仍會在資料庫確認成功後提交 offset。

再次查行數：

```powershell
docker compose exec db psql -U de_user -d orders -c "SELECT COUNT(*) FROM orders;"
```

行數應保持不變。這是因為 PostgreSQL 的 primary key 和 `ON CONFLICT DO NOTHING` 阻止相同 `order_id` 重複插入。重播事件的 event_time 會更新，但資料庫不會用它覆蓋已保存的 row。

### 32. 示範三個 consumer group 各自延續 offset

1. 在 Terminal A 按 `Ctrl+C`。
2. 等候看到 `Consumer closed`。
3. 在 Terminal A 再次執行原本的命令：

   ```powershell
   uv run python -m demo_real_time_kafka.consumer --topic orders --group-id orders-to-postgres
   ```

4. 因 `orders-to-postgres` group 已有 committed offset，consumer 應由上次成功位置之後開始，而不是每次都從 `orders` 開頭重播全部資料。Terminal B、C 的 group 各有自己的 `orders-audit` offset。
5. 再用 SQL 查 `COUNT(*)`；已經保存的資料不會因為重啟 consumer 而增加。

每個 consumer group 有自己的 offset。刪除 volume、改 group ID 或刪 topic 都會改變這個練習的狀態，不要用它們來「隨便重置」而不先理解後果。

### 33. 查 Kafka group 及服務 logs

分別檢查三個 group 狀態：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-to-postgres
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-audit-1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-audit-2
```

預期每個 group 表格會列出其 topic 的兩個 partition、CURRENT-OFFSET、LOG-END-OFFSET 及 LAG。consumer 正在執行時可能見到 partition assignment；停止 consumer 時可能暫時顯示 group 沒有 active member。兩個 `orders-audit` group 各自接收 topic 全部資料；不同 group 之間不會分攤訊息。

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
- **`uv run` 找不到套件或 Python：** 確認 terminal 在 project folder，`pyproject.toml` 和 `uv.lock` 存在，再執行 `uv sync`。用 `uv run python --version` 確認版本；不要改用系統 Python 或手動啟動 `.venv`。
- **Topic 已存在但只有 1 個 partition：** `--create-if-not-exists` 不會修改現有 topic；使用 `kafka-topics.sh --alter --topic <topic> --partitions 2`，再用 `--describe` 確認。partition 數只可增加，不可減少。

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
git add .gitignore .env.example .python-version pyproject.toml uv.lock docker-compose.yml schema.sql src tests
git status --short
```

仔細檢查 staged files。確認沒有 `.env`、`.venv`、密碼、資料庫資料後，才可 commit：

```powershell
git commit -m "feat: build local kafka orders pipeline"
```

若 `git commit` 提示未設定 user name/email，按提示只設定你自己的 Git identity；不要把練習 repository 推送到遠端，除非你確定要公開分享。

---

## 完成檢查表

- [x] `uv --version` 顯示 uv 已安裝，`uv run python --version` 顯示 Python 3.14 或以上。
- [x] `git --version`、`docker --version`、`docker compose version` 成功。
- [x] `docker info` 能顯示 Server。
- [x] VS Code 使用專案 `.venv` interpreter。
- [x] `.env` 存在，`.gitignore` 排除 `.env` 和 `.venv`。
- [x] `docker compose ps` 顯示 Kafka `Up`、PostgreSQL `healthy`。
- [x] Kafka topics `orders` 和 `orders-audit` 都存在，各有 2 partitions。
- [x] 三個 consumer groups 按設定分別消費 `orders` 或 `orders-audit`。
- [x] `tests/test_event.py` 和 `tests/test_producer.py` unit tests 通過。
- [x] `tests/test_db.py` integration test 通過。
- [x] Producer 能把有效 JSON 訂單送入 Kafka。
- [x] Consumer 能讀取訊息、驗證並寫入 PostgreSQL。
- [x] PostgreSQL SQL 查詢結果與訂單數量吻合。
- [x] 重播相同 `order_id` 不會增加資料列。
- [x] 重啟相同 consumer group 後會延續已提交的 offset。
- [x] 明白 `docker compose down` 保留 volumes，而 `docker compose down -v` 會刪除資料。

## 建議下一步延伸

完成並理解以上流程後，才考慮：

1. 在同一個 consumer group 啟動多個 consumer，觀察兩個 partitions 如何分配；當 group member 多於 partitions 時，會有 consumer 暫時沒有 partition。
2. 加入 dead-letter topic，並規定訊息寫入 DLQ 成功後才提交原 topic offset。
3. 將 Python producer/consumer 放進 Docker Compose，練習 Compose service DNS/networking。
4. 加事件 schema version、更完整的 items 欄位驗證及測試。
5. 加 CI、SQL 聚合查詢或 dashboard；雲端 Kafka/Spark/Flink 留到本機流程穩定之後。
