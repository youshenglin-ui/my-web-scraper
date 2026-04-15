# 指定使用穩定版的 Debian 12 (bookworm)，避免最新系統造成 Playwright 相依性衝突
FROM python:3.12-slim-bookworm

# 🚀 新增：安裝時區資料並強制設定為台灣時間 (UTC+8)
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Taipei

# 設定工作目錄
WORKDIR /app

# 複製環境套件清單並安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安裝 Playwright 無頭瀏覽器及其系統依賴
RUN playwright install --with-deps chromium

# 複製所有專案檔案到容器中
COPY . .

# 建立下載資料夾與資料庫存放位置
RUN mkdir -p downloads

# 啟動伺服器 (改用 shell 形式，自動讀取 Render 雲端分配的 PORT 環境變數)
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}