# proxy.py (Cập nhật Ngày 5)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import asyncio
import uuid  # Thư viện tự tạo mã ID ngẫu nhiên không trùng lặp

app = FastAPI()

SHARD_A = "http://127.0.0.1:8001"
SHARD_B = "http://127.0.0.1:8002"

cluster_health = {"Shard A (Port 8001)": "Unknown", "Shard B (Port 8002)": "Unknown"}

class GlobalDataPayload(BaseModel):
    content: dict

# (Giữ nguyên vòng lặp Heartbeat của Ngày 4)
async def heartbeat_checker():
    while True:
        for shard_name, url in [("Shard A (Port 8001)", SHARD_A), ("Shard B (Port 8002)", SHARD_B)]:
            try:
                res = requests.get(f"{url}/all", timeout=2)
                if res.status_code == 200: cluster_health[shard_name] = "🟢 ONLINE (Khỏe mạnh)"
            except Exception: cluster_health[shard_name] = "🔴 OFFLINE (Đã sập)"
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event(): asyncio.create_task(heartbeat_checker())

@app.get("/")
def proxy_status(): return {"status": "Proxy Coordinator đang hoạt động", "cluster_health": cluster_health}

# ==================== ĐOẠN CODE MỚI NGÀY 5 (2PC) ====================

@app.post("/transaction/global-insert")
def global_insert(payload: GlobalDataPayload):
    # Tự động tạo một mã giao dịch duy nhất toàn cầu (Transaction ID)
    tx_id = str(uuid.uuid4())
    
    nodes = {"Shard A": SHARD_A, "Shard B": SHARD_B}
    votes = {} # Nơi gom phiếu bầu của các trạm
    
    # 🌟 GIAI ĐOẠN 1: PREPARE (Hỏi ý kiến)
    print(f"[PROXY] Bắt đầu Giai đoạn 1 cho TxID: {tx_id}. Gửi lệnh Prepare...")
    for name, url in nodes.items():
        try:
            res = requests.post(
                f"{url}/transaction/prepare", 
                json={"tx_id": tx_id, "data": payload.content},
                timeout=3
            )
            if res.status_code == 200 and res.json().get("status") == "prepared":
                votes[name] = "COMMIT"  # Node đồng ý chuẩn bị xong
            else:
                votes[name] = "ABORT"   # Node từ chối
        except Exception:
            votes[name] = "ABORT"       # Node bị sập hoàn toàn
            
    # 🌟 GIAI ĐOẠN 2: EXECUTE (Ra quyết định dựa trên phiếu bầu)
    print(f"[PROXY] Kết quả phiếu bầu: {votes}")
    
    # Kịch bản 1: CẢ HAI bên cùng đồng ý COMMIT
    if all(vote == "COMMIT" for vote in votes.values()):
        print(f"[PROXY] Tất cả đồng ý! Phát lệnh COMMIT chính thức...")
        for name, url in nodes.items():
            requests.post(f"{url}/transaction/commit?tx_id={tx_id}")
        return {
            "transaction_id": tx_id,
            "votes_collected": votes,
            "final_decision": "GLOBAL_COMMIT (Dữ liệu đã ghi đồng thời vào cả 2 trạm thành công!)"
        }
    
    # Kịch bản 2: Chỉ cần MỘT bên từ chối (hoặc sập), ép buộc hủy bỏ toàn bộ hệ thống (Rollback)
    else:
        print(f"[PROXY] Phát hiện có lỗi hoặc trạm sập! Phát lệnh ABORT (Hủy bỏ) khẩn cấp...")
        for name, url in nodes.items():
            try:
                requests.post(f"{url}/transaction/abort?tx_id={tx_id}")
            except Exception:
                pass # Trạm nào sập sẵn rồi thì bỏ qua không cần thông báo tiếp
        
        raise HTTPException(
            status_code=400,
            detail={
                "transaction_id": tx_id,
                "votes_collected": votes,
                "final_decision": "GLOBAL_ABORT (Hệ thống đã tự động Rollback an toàn, không có file nào bị lưu rác)"
            }
        )