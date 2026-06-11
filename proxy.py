# proxy.py (Cập nhật Ngày 6)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import asyncio

app = FastAPI()

SHARD_A = "http://127.0.0.1:8001"
SHARD_B = "http://127.0.0.1:8002"

# Bảng trạng thái sức khỏe
cluster_health = {"Shard A (Port 8001)": "Unknown", "Shard B (Port 8002)": "Unknown"}

# ==================== ĐOẠN CODE MỚI NGÀY 6 (RECOVERY QUEUE) ====================
# Hộp thư chờ: Nơi giữ lại dữ liệu nếu chẳng may trạm đích đang bị sập
recovery_queues = {
    "Shard A (Port 8001)": [],
    "Shard B (Port 8002)": []
}

class ShardDataPayload(BaseModel):
    id: int
    content: dict

# Hàm ngầm thực hiện đồng bộ bù dữ liệu khi Node sống lại
def trigger_resync(shard_name, url):
    queue = recovery_queues[shard_name]
    if not queue:
        return
    
    print(f"🔄 [RESYNC] Phát hiện {shard_name} vừa sống lại! Đang đồng bộ bù {len(queue)} dữ liệu lỡ...")
    
    successful_syncs = []
    for item in list(queue): # Duyệt qua danh sách dữ liệu đang chờ
        try:
            res = requests.post(f"{url}/insert", json={"data": item})
            if res.status_code == 200:
                successful_syncs.append(item)
        except Exception:
            print(f"❌ [RESYNC] Lỗi kết nối lại trong quá trình đồng bộ {shard_name}. Tạm dừng.")
            break
            
    # Xóa những dữ liệu đã đồng bộ bù thành công khỏi Hộp thư chờ
    for item in successful_syncs:
        queue.remove(item)
        
    print(f"✨ [RESYNC] Hoàn thành đồng bộ bù cho {shard_name}. Hộp thư chờ còn lại: {len(queue)} bản ghi.")

# 🔄 VÒNG LẶP HEARTBEAT CẢI TIẾN: Vừa kiểm tra sức khỏe, vừa kích hoạt hồi phục dữ liệu
async def heartbeat_checker():
    while True:
        for shard_name, url in [("Shard A (Port 8001)", SHARD_A), ("Shard B (Port 8002)", SHARD_B)]:
            old_status = cluster_health[shard_name]
            current_status = "🔴 OFFLINE"
            
            try:
                res = requests.get(f"{url}/all", timeout=1)
                if res.status_code == 200:
                    current_status = "🟢 ONLINE"
            except Exception:
                current_status = "🔴 OFFLINE"
            
            cluster_health[shard_name] = current_status
            
            # 🌟 ĐIỂM NHẤN NGÀY 6: Nếu trạng thái chuyển từ OFFLINE -> ONLINE (Sống lại!)
            if old_status == "🔴 OFFLINE" and current_status == "🟢 ONLINE":
                trigger_resync(shard_name, url)
                
        await asyncio.sleep(4) # Kiểm tra mỗi 4 giây cho nhanh

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(heartbeat_checker())

@app.get("/")
def proxy_status():
    return {
        "status": "Proxy Router với tính năng Hồi Phục Dữ Liệu đang chạy",
        "cluster_health": cluster_health,
        "pending_recovery_queues_count": {k: len(v) for k, v in recovery_queues.items()}
    }

# Endpoint xem chi tiết trong hộp thư chờ còn những gì
@app.get("/recovery/queues")
def get_recovery_queues():
    return {"recovery_queues": recovery_queues}

# 📥 CẢI TIẾN ĐƯỜNG GHI DỮ LIỆU: Nếu trạm sập, lưu vào hàng đợi thay vì báo lỗi!
@app.post("/sharding/insert")
def sharding_insert(payload: ShardDataPayload):
    data_id = payload.id
    if data_id % 2 == 0:
        target_shard = SHARD_A
        shard_name = "Shard A (Port 8001)"
    else:
        target_shard = SHARD_B
        shard_name = "Shard B (Port 8002)"
        
    prepared_data = {"id": data_id, "info": payload.content}
    
    # Nếu trạm đang ONLINE -> Ghi trực tiếp như bình thường
    if cluster_health[shard_name] == "🟢 ONLINE":
        try:
            requests.post(f"{target_shard}/insert", json={"data": prepared_data})
            return {"status": "Ghi trực tiếp thành công", "target": shard_name}
        except Exception:
            pass # Nếu lỗi đột xuất, rơi xuống đoạn code bỏ vào hộp thư chờ phía dưới
            
    # ĐIỂM ĐẮT GIÁ: Nếu trạm đang OFFLINE -> Gom dữ liệu cất vào hộp thư chờ!
    recovery_queues[shard_name].append(prepared_data)
    return {
        "status": "Chấp nhận yêu cầu (Hệ thống đang sập nhưng dữ liệu đã được đưa vào hàng đợi bảo vệ an toàn!)",
        "target_queued": shard_name,
        "current_queue_size": len(recovery_queues[shard_name])
    }

# (Giữ nguyên hàm đọc tổng hợp Scatter-Gather của Ngày 3)
@app.get("/sharding/all")
def sharding_get_all():
    combined_data = []
    for url in [SHARD_A, SHARD_B]:
        try:
            res = requests.get(f"{url}/all", timeout=1).json()
            combined_data.extend(res)
        except Exception: pass
    return {"data": combined_data}