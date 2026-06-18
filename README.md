# Hệ Thống TinyDB Phân Tán và Mở Rộng

> Nghiên cứu dự án TinyDB và phát triển các tính năng xử lý phân tán nâng cao bằng Python và FastAPI.

## Giới thiệu

TinyDB là một cơ sở dữ liệu NoSQL mã nguồn mở, nhẹ, được xây dựng hoàn toàn bằng Python và sử dụng tệp JSON để lưu trữ dữ liệu.

Tuy nhiên, TinyDB nguyên bản chỉ hoạt động trên một máy đơn lẻ và không hỗ trợ các cơ chế phân tán như mở rộng tải, chịu lỗi hay đồng bộ dữ liệu giữa nhiều nút lưu trữ.

Dự án này mở rộng TinyDB thành một hệ quản trị cơ sở dữ liệu phân tán (Distributed Database System) theo mô hình **Proxy-Based Architecture**, áp dụng các kỹ thuật của môn Hệ thống phân tán nhằm giải quyết các bài toán:

* Phân chia tải lưu trữ dữ liệu
* Tăng khả năng mở rộng hệ thống
* Tăng tính sẵn sàng (High Availability)
* Chịu lỗi khi Node gặp sự cố
* Giám sát trạng thái cụm máy chủ theo thời gian thực

---

# Mục tiêu dự án

* Chuyển đổi TinyDB từ mô hình cục bộ sang mô hình phân tán.
* Xây dựng cơ chế Sharding để phân phối dữ liệu.
* Thiết kế Proxy Router làm trung tâm điều phối.
* Triển khai cơ chế Heartbeat giám sát các Node.
* Xây dựng hệ thống tự phục hồi dữ liệu (Self-Healing Recovery).
* Minh bạch hóa trạng thái vận hành của cụm máy chủ thông qua Dashboard Telemetry Metrics.

---

# Kiến trúc hệ thống

```text
                    Client
                       |
                       |
                       v
              +----------------+
              |  Proxy Router  |
              |   Port 8000    |
              +----------------+
                 /          \
                /            \
               v              v

      +----------------+   +----------------+
      |    Shard A     |   |    Shard B     |
      |   Port 8001    |   |   Port 8002    |
      +----------------+   +----------------+

```

## Thành phần hệ thống

### Proxy Router

Chịu trách nhiệm:

* Nhận request từ Client
* Định tuyến dữ liệu tới Shard phù hợp
* Thực hiện Scatter-Gather Query
* Giám sát trạng thái Node
* Quản lý hàng đợi phục hồi dữ liệu
* Thu thập Telemetry Metrics

### Storage Node

Mỗi Node:

* Sử dụng TinyDB để lưu trữ dữ liệu
* Quản lý một phân mảnh dữ liệu riêng
* Thực hiện các thao tác CRUD
* Phản hồi trạng thái sức khỏe (Heartbeat)

---

# Công nghệ sử dụng

| Công nghệ        | Mục đích                  |
| ---------------- | ------------------------- |
| Python 3.10+     | Ngôn ngữ phát triển       |
| FastAPI          | Xây dựng REST API         |
| TinyDB           | Lưu trữ dữ liệu           |
| Uvicorn          | ASGI Server               |
| Requests / HTTPX | Giao tiếp giữa các Node   |
| JSON             | Định dạng lưu trữ dữ liệu |

---

# Chức năng cốt lõi

## 1. Horizontal Sharding

Hệ thống phân chia dữ liệu dựa trên ID chẵn/lẻ.

| Điều kiện | Nơi lưu trữ    |
| --------- | -------------- |
| ID chẵn   | Shard A (8001) |
| ID lẻ     | Shard B (8002) |

Ví dụ:

```json
{
    "id": 20,
    "name": "Nguyen Van A"
}
```

→ Được lưu tại Shard A.

```json
{
    "id": 21,
    "name": "Tran Van B"
}
```

→ Được lưu tại Shard B.

---

## 2. Scatter-Gather Query

Khi người dùng gọi:

```http
GET /sharding/all
```

Proxy sẽ:

1. Gửi yêu cầu tới tất cả Shard.
2. Thu thập kết quả trả về.
3. Hợp nhất dữ liệu.
4. Trả về danh sách hoàn chỉnh cho Client.

Điều này giúp người dùng truy cập dữ liệu toàn hệ thống mà không cần biết dữ liệu đang nằm ở Shard nào.

---

# Tính năng nâng cao

## 1. Heartbeat & Catch-up Queue Recovery

### Bài toán

Trong hệ thống phân tán, Node có thể:

* Mất điện
* Mất kết nối mạng
* Lỗi phần cứng

Nếu tiếp tục gửi dữ liệu tới Node lỗi, dữ liệu sẽ bị mất.

### Giải pháp

Proxy triển khai:

* Heartbeat Checker
* Recovery Queue

Cứ mỗi 4 giây:

* Proxy kiểm tra trạng thái các Node.
* Nếu Node OFFLINE:

  * Request ghi dữ liệu không bị từ chối.
  * Dữ liệu được lưu tạm trong Recovery Queue.

Khi Node ONLINE trở lại:

* Hệ thống tự động đồng bộ dữ liệu còn thiếu.
* Các bản ghi được gửi lại theo thứ tự FIFO.
* Người dùng không cần thao tác thủ công.

### Lợi ích

* Không mất dữ liệu khi Node gặp sự cố.
* Tăng khả năng chịu lỗi.
* Hỗ trợ cơ chế Self-Healing.

---

## 2. Telemetry Metrics Dashboard

### Bài toán

Quản trị viên cần biết:

* Hệ thống đã xử lý bao nhiêu request.
* Dữ liệu được phân phối ra sao.
* Có Node nào đang quá tải hay không.
* Bao nhiêu dữ liệu đã được phục hồi.

### Giải pháp

Endpoint:

```http
GET /cluster/metrics
```

Cung cấp các chỉ số:

* total_write_requests
* routed_to_shard_a
* routed_to_shard_b
* recovery_queue_size
* total_resynced_items

### Lợi ích

* Theo dõi hệ thống theo thời gian thực.
* Hỗ trợ đánh giá hiệu năng.
* Tăng khả năng giám sát cụm máy chủ.

---

# Cài đặt

## Clone Repository

```bash
git clone https://github.com/ducmanh14/distributed-tinydb.git
cd distributed-tinydb
```

## Cài đặt thư viện

```bash
pip install -r requirements.txt
```

---

# Chạy hệ thống

## Khởi động Shard A

```powershell
$env:DB_FILE="data/shard_a.json"
uvicorn node:app --port 8001
```

## Khởi động Shard B

```powershell
$env:DB_FILE="data/shard_b.json"
uvicorn node:app --port 8002
```

## Khởi động Proxy Router

```powershell
uvicorn proxy:app --port 8000
```

---

# Kiểm thử

Sau khi khởi động thành công:

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

Thử nghiệm:

### Ghi dữ liệu ID chẵn

```json
{
    "id": 20,
    "name": "Test Even"
}
```

Kết quả:

* Lưu tại shard_a.json

### Ghi dữ liệu ID lẻ

```json
{
    "id": 21,
    "name": "Test Odd"
}
```

Kết quả:

* Lưu tại shard_b.json

### Đọc toàn bộ dữ liệu

```http
GET /sharding/all
```

Kết quả:

* Proxy tổng hợp dữ liệu từ tất cả Shard.

---

# Cấu trúc thư mục

```text
distributed-tinydb/
│
├── node.py
├── proxy.py
├── requirements.txt
│
├── data/
│   ├── shard_a.json
│   └── shard_b.json
│
└── README.md
```

---

# Hạn chế hiện tại

* Recovery Queue đang lưu trên RAM.
* Nếu Proxy bị sập, dữ liệu trong Queue có thể bị mất.
* Thuật toán Sharding ID chẵn/lẻ khó mở rộng khi số lượng Shard tăng lên.

---

# Hướng phát triển

## Persistent Queue

Thay thế Recovery Queue bằng:

* RabbitMQ
* Apache Kafka

để đảm bảo dữ liệu không bị mất.

## Consistent Hashing

Thay thế thuật toán ID chẵn/lẻ bằng Consistent Hashing nhằm:

* Dễ dàng mở rộng số lượng Shard.
* Giảm lượng dữ liệu phải di chuyển khi thêm Node mới.

## Triển khai Production

* Docker
* Kubernetes
* Monitoring bằng Prometheus
* Grafana Dashboard

---

# Tác giả

**Mai Đức Mạnh**
MSSV: 23010814
Khoa Công nghệ Thông tin – Đại học Phenikaa

---

# Giảng viên

**Nguyễn Lệ Thu**

---

# Môn học

Bài tập lớn môn **Hệ Thống Phân Tán**
Đề tài: **Nghiên cứu dự án TinyDB và phát triển mở rộng các tính năng xử lý phân tán cao cấp**

# Link video demo dự án 
https://drive.google.com/file/d/1K0_xHHD1gU-5Ot5Xg6_N4Sj9YQzN8Zfb/view?usp=sharing
