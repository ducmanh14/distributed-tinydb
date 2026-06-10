# proxy.py (Cập nhật Ngày 4)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import asyncio

app = FastAPI()

SHARD_A = "http://127.0.0.1:8001"
SHARD_B = "http://127.0.0.1:8002"

# BẢNG TRẠNG THÁI SỨC KHỎE: Nơi lưu trữ tình trạng thực tế của hệ thống
cluster_health = {
    "Shard A (Port 8001)": "Unknown",
    "Shard B (Port 8002)": "Unknown"
}

class ShardDataPayload(BaseModel):
    id: int
    content: dict

# 🔄 VÒNG LẶP HEARTBEAT: Cứ 5 giây tự động phát tín hiệu kiểm tra (Ping) các Node
async def heartbeat_checker():
    while True:
        # Kiểm tra Shard A
        try:
            res = requests.get(f"{SHARD_A}/all", timeout=2)
            if res.status_code == 200:
                cluster_health["Shard A (Port 8001)"] = "🟢 ONLINE (Khỏe mạnh)"
        except Exception:
            cluster_health["Shard A (Port 8001)"] = "🔴 OFFLINE (Đã sập)"

        # Kiểm tra Shard B
        try:
            res = requests.get(f"{SHARD_B}/all", timeout=2)
            if res.status_code == 200:
                cluster_health["Shard B (Port 8002)"] = "🟢 ONLINE (Khỏe mạnh)"
        except Exception:
            cluster_health["Shard B (Port 8002)"] = "🔴 OFFLINE (Đã sập)"

        # Nghỉ 5 giây rồi lặp lại
        await asyncio.sleep(5)

# Kích hoạt vòng lặp Heartbeat ngay khi Proxy vừa được bật lên
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(heartbeat_checker())

@app.get("/")
def proxy_status():
    return {
        "status": "Proxy Router đang hoạt động",
        "mode": "Sharding + Heartbeat Monitoring (Nhịp tim tự động)",
        "cluster_health_snapshot": cluster_health
    }

# Endpoint chuyên dụng để thầy cô vào kiểm tra sức khỏe hệ thống
@app.get("/cluster/health")
def get_health_status():
    return {"cluster_status": cluster_health}

# --- GIỮ NGUYÊN CÁC TÍNH NĂNG NGÀY 3 ĐỂ HỆ THỐNG KHÔNG BỊ GÃY ---
@app.post("/sharding/insert")
def sharding_insert(payload: ShardDataPayload):
    data_id = payload.id
    if data_id % 2 == 0:
        target_shard = SHARD_A
        shard_name = "Shard A (Port 8001 - Chẵn)"
    else:
        target_shard = SHARD_B
        shard_name = "Shard B (Port 8002 - Lẻ)"
    
    # Kiểm tra nhanh bảng nhịp tim trước khi ghi để chặn lỗi sớm (Fail-Fast)
    if "OFFLINE" in cluster_health[f"{'Shard A' if data_id % 2 == 0 else 'Shard B'} (Port {'8001' if data_id % 2 == 0 else '8002'})"]:
        raise HTTPException(status_code=503, detail=f"Hệ thống từ chối lệnh! {shard_name} hiện đang sập.")

    try:
        response = requests.post(f"{target_shard}/insert", json={"data": {"id": data_id, "info": payload.content}})
        return {"message": "Phân mảnh dữ liệu thành công", "assigned_shard": shard_name, "node_response": response.json()}
    except Exception:
        raise HTTPException(status_code=500, detail=f"Lỗi kết nối đột xuất tới {shard_name}.")

@app.get("/sharding/all")
def sharding_get_all():
    combined_data = []
    try:
        res_a = requests.get(f"{SHARD_A}/all").json()
        combined_data.extend(res_a)
    except Exception:
        pass
    try:
        res_b = requests.get(f"{SHARD_B}/all").json()
        combined_data.extend(res_b)
    except Exception:
        pass
    return {"description": "Dữ liệu gộp từ các mảnh còn sống", "total_records": len(combined_data), "data": combined_data}