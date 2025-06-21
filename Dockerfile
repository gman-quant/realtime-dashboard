# 使用官方 Python 3.11 的輕量級映像檔作為基礎
FROM python:3.11-slim

# 更新套件庫並安裝 git
RUN apt-get update && apt-get install -y git --no-install-recommends

# 在容器內建立一個 /app 的工作目錄
WORKDIR /app

# 先複製依賴清單進去
COPY requirements.txt .

# 安裝依賴套件
RUN pip install --no-cache-dir -r requirements.txt

# 將目前資料夾的所有檔案複製到容器的 /app 目錄下
COPY . .

# 設定容器啟動時要執行的指令
# uvicorn main:app --host 0.0.0.0 --port 8000 --reload
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]