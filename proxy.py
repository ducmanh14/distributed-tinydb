# proxy.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests

app = FastAPI()

# Cấu hình địa chỉ URL của 2 trạm dữ liệu (Chúng ta sẽ chạy ở các cổng khác nhau)
MASTER_NODE = "http://127.0.0.1:8001"
SLAVE_NODE = "http://127.0.0.1:8002"

class DataPayload(BaseModel):
    data: dict

@app.get("/")
def proxy_status():
    return {"status": "Proxy điều phối đang hoạt động", "mode": "Master-Slave Replication"}

# 1. TÍNH NĂNG GHI: Ghi vào Master, tự động nhân bản sang Slave
@app.post("/replication/insert")
def replication_insert(payload: DataPayload):
    # Bước 1: Ghi dữ liệu vào Master Node
    try:
        response_master = requests.post(f"{MASTER_NODE}/insert", json={"data": payload.data})
        master_result = response_master.json()
    except Exception:
        raise HTTPException(status_code=500, detail="Trạm Master bị sập! Hệ thống không thể ghi dữ liệu.")

    # Bước 2: Tự động nhân bản (Replicate) ngầm sang Slave Node
    try:
        requests.post(f"{SLAVE_NODE}/insert", json={"data": payload.data})
        slave_status = "Đồng bộ thành công"
    except Exception:
        slave_status = "Thất bại (Trạm Slave đang sập)"

    return {
        "message": "Tiến trình ghi hoàn tất",
        "master_response": master_result,
        "slave_synchronization": slave_status
    }

# 2. TÍNH NĂNG ĐỌC: Ưu tiên đọc từ Slave, tự động cứu hộ nếu Slave sập
@app.get("/replication/read")
def replication_read():
    # Thử nghiệm đọc từ Slave trước để giảm tải cho Master
    try:
        res = requests.get(f"{SLAVE_NODE}/all")
        return {
            "source": "Slave Node (Port 8002)",
            "data": res.json()
        }
    except Exception:
        # Nếu Slave sập, tự động chuyển hướng (Failover) sang đọc từ Master
        print("[CẢNH BÁO] Slave Node bị sập! Tự động kích hoạt cơ chế cứu hộ Failover sang Master.")
        try:
            res = requests.get(f"{MASTER_NODE}/all")
            return {
                "source": "Master Node (Port 8001) - Chế độ cứu hộ (Failover)",
                "data": res.json()
            }
        except Exception:
            raise HTTPException(status_code=500, detail="Toàn bộ hệ thống trạm dữ liệu đã sập!")