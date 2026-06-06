# Docker Cleanup — Giải phóng dung lượng trên Windows

Docker Desktop + WSL2 backend có thể chiếm 30GB+ theo thời gian. Hướng dẫn này giúp lấy lại dung lượng theo từng bước, từ an toàn đến triệt để.

---

## Bước 1 — Kiểm tra Docker đang chiếm bao nhiêu

```bash
docker system df
```

Output mẫu:
```
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          18        5         12.3GB    8.1GB (65%)
Containers      3         3         245MB     0B (0%)
Local Volumes   8         3         2.1GB     1.4GB (66%)
Build Cache     47        0         3.2GB     3.2GB
```

---

## Bước 2 — Xóa những thứ không dùng (an toàn)

### 2a. Xóa build cache (thường lớn nhất, an toàn nhất)
```bash
docker builder prune -f
```

### 2b. Xóa images không dùng (dangling = không có tag)
```bash
docker image prune -f
```

### 2c. Xóa containers đã dừng
```bash
docker container prune -f
```

### 2d. Xóa volumes không dùng (cẩn thận — mất data)
```bash
docker volume prune -f
```

### 2e. Xóa tất cả cùng lúc (containers + images + networks + build cache)
```bash
docker system prune -f
```

Thêm `--volumes` nếu muốn xóa cả volumes:
```bash
docker system prune -f --volumes
```

---

## Bước 3 — Xóa images cụ thể của project này

Sau khi chạy `stop.sh -v`, các images vẫn còn. Xóa thủ công nếu muốn:

```bash
# Xem danh sách images
docker images

# Xóa images CDC pipeline (giải phóng ~5-8GB)
docker rmi debezium/connect:2.5 \
           confluentinc/cp-kafka:7.5.0 \
           confluentinc/cp-zookeeper:7.5.0 \
           bitnami/spark:3.5.0 \
           mongo:7.0 \
           redis:7 \
           mysql:8.0 \
           prom/prometheus:latest \
           grafana/grafana:latest

# Xóa image metrics-exporter đã build (nếu có)
docker rmi cdc-data-pipeline-metrics-exporter 2>/dev/null || true
```

---

## Bước 4 — Thu hồi dung lượng WSL2 vhdx (quan trọng nhất trên Windows)

Docker Desktop dùng WSL2 backend. Dù đã xóa containers/images bên trong Docker, file `.vhdx` (virtual disk) của WSL2 **không tự thu nhỏ** — phải compact thủ công.

### Cách thực hiện (chạy trong PowerShell với quyền Admin):

**Bước 4.1 — Tắt Docker Desktop và WSL**
```powershell
# Tắt Docker Desktop hoàn toàn (tray icon → Quit)
# Sau đó tắt WSL:
wsl --shutdown
```

**Bước 4.2 — Tìm file vhdx**

File thường nằm tại một trong các đường dẫn sau:
```
C:\Users\<tên_user>\AppData\Local\Docker\wsl\data\ext4.vhdx
C:\Users\<tên_user>\AppData\Local\Packages\CanonicalGroupLimited...\LocalState\ext4.vhdx
```

Tìm nhanh:
```powershell
Get-ChildItem -Path "$env:LOCALAPPDATA\Docker" -Filter "*.vhdx" -Recurse -ErrorAction SilentlyContinue
```

**Bước 4.3 — Compact vhdx bằng diskpart**
```powershell
# Mở PowerShell với quyền Admin, chạy diskpart
diskpart
```

Trong diskpart, nhập từng dòng:
```
select vdisk file="C:\Users\<tên_user>\AppData\Local\Docker\wsl\data\ext4.vhdx"
attach vdisk readonly
compact vdisk
detach vdisk
exit
```

Thay `<tên_user>` bằng tên user thật. Compact có thể mất 5-15 phút.

**Bước 4.4 — Khởi động lại Docker Desktop**

Sau khi compact xong, mở Docker Desktop bình thường.

---

## Bước 5 — Di chuyển Docker data sang ổ khác (nếu ổ C gần đầy)

Nếu muốn Docker lưu data sang ổ D:

1. Tắt Docker Desktop
2. Vào **Docker Desktop → Settings → Resources → Advanced**
3. Đổi "Disk image location" sang `D:\DockerData` (hoặc bất kỳ thư mục nào)
4. Apply & Restart

---

## Tóm tắt theo mức độ cần thiết

| Tình huống | Lệnh |
|------------|------|
| Sau mỗi session benchmark | `docker system prune -f` |
| Dọn dẹp định kỳ (hàng tuần) | `docker system prune -f --volumes` |
| Xóa toàn bộ project CDC | `bash stop.sh -v` rồi xóa images thủ công |
| Lấy lại dung lượng ổ C thật sự | Compact vhdx (Bước 4) |
| Docker chiếm > 20GB | Compact vhdx + di chuyển sang ổ D (Bước 4+5) |

---

## Kiểm tra sau khi dọn

```bash
docker system df          # Xem Docker còn chiếm bao nhiêu
docker images             # Còn images gì
docker ps -a              # Còn containers gì
docker volume ls          # Còn volumes gì
```
