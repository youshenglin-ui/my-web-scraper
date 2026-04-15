# 使用輕量級的 Python 3.12 映像檔
FROM python:3.12-slim

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

# 暴露 FastAPI 預設的 8000 Port
EXPOSE 8000

# 啟動伺服器
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]