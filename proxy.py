# proxy.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests

app = FastAPI()

# Quy định kiến trúc Ngày 3:
# Shard A (Port 8001) chịu trách nhiệm lưu trữ các dữ liệu có ID CHẴN
# Shard B (Port 8002) chịu trách nhiệm lưu trữ các dữ liệu có ID LẺ
SHARD_A = "http://127.0.0.1:8001"
SHARD_B = "http://127.0.0.1:8002"

class ShardDataPayload(BaseModel):
    id: int  # Bắt buộc phải có ID để thuật toán Sharding phân loại
    content: dict

@app.get("/")
def proxy_status():
    return {"status": "Proxy Router đang hoạt động", "mode": "Sharding (Even/Odd ID Split)"}

# 1. TÍNH NĂNG GHI ĐA MẢNH (SHARDING INSERT)
@app.post("/sharding/insert")
def sharding_insert(payload: ShardDataPayload):
    data_id = payload.id
    
    # Thuật toán định tuyến dữ liệu (Routing Algorithm)
    if data_id % 2 == 0:
        target_shard = SHARD_A
        shard_name = "Shard A (Port 8001 - Chẵn)"
    else:
        target_shard = SHARD_B
        shard_name = "Shard B (Port 8002 - Lẻ)"
    
    # Tiến hành đẩy dữ liệu vào Shard được chỉ định
    try:
        response = requests.post(f"{target_shard}/insert", json={"data": {"id": data_id, "info": payload.content}})
        return {
            "message": "Phân mảnh dữ liệu thành công",
            "assigned_shard": shard_name,
            "node_response": response.json()
        }
    except Exception:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối tới {shard_name}. Trạm này có thể đã sập!")

# 2. TÍNH NĂNG ĐỌC TỔNG HỢP TỪ TẤT CẢ CÁC SHARD (SCATTER-GATHER)
@app.get("/sharding/all")
def sharding_get_all():
    combined_data = []
    
    # Thu thập từ Shard A
    try:
        res_a = requests.get(f"{SHARD_A}/all").json()
        combined_data.extend(res_a)
    except Exception:
        print("[CẢNH BÁO] Không thể lấy dữ liệu từ Shard A")
        
    # Thu thập từ Shard B
    try:
        res_b = requests.get(f"{SHARD_B}/all").json()
        combined_data.extend(res_b)
    except Exception:
        print("[CẢNH BÁO] Không thể lấy dữ liệu từ Shard B")
        
    return {
        "description": "Dữ liệu được gộp lại từ tất cả các mảnh (Shard A + Shard B)",
        "total_records": len(combined_data),
        "data": combined_data
    }