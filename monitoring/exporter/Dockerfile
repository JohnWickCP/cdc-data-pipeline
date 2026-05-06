FROM python:3.9-slim

WORKDIR /app

# Copy thư viện cần thiết trước để tận dụng cache của Docker
COPY requirements.txt .

# Cài đặt thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn exporter
COPY metrics_exporter.py .



# Expose port Prometheus (mặc định trong code là 8000)
EXPOSE 8000

# Chạy exporter
CMD ["python", "metrics_exporter.py"]
