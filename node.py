# node.py (Cập nhật Ngày 5)
from fastapi import FastAPI, HTTPException
from tinydb import TinyDB
from pydantic import BaseModel
import os

app = FastAPI()

# Lấy cấu hình file lưu trữ từ biến môi trường
db_file = os.getenv("DB_FILE", "data/default.json")
db = TinyDB(db_file)

# 📦 BỘ NHỚ TẠM (Write-Ahead Log đơn giản): Giữ dữ liệu ở trạng thái chờ duyệt
pending_transactions = {}

class InsertPayload(BaseModel):
    data: dict

class PreparePayload(BaseModel):
    tx_id: str
    data: dict

@app.post("/insert")
def insert_data(payload: InsertPayload):
    db.insert(payload.data)
    return {"status": "success", "data": payload.data}

@app.get("/all")
def get_all():
    return db.all()

# ==================== ĐOẠN CODE MỚI NGÀY 5 (2PC) ====================

# GIAI ĐOẠN 1: PREPARE - Kiểm tra xem trạm có sẵn sàng ghi hay không
@app.post("/transaction/prepare")
def tx_prepare(payload: PreparePayload):
    if not payload.data:
        raise HTTPException(status_code=400, detail="Dữ liệu trống rỗng!")
    
    # Cất dữ liệu vào bộ nhớ tạm, chưa ghi vào ổ đĩa file .json
    pending_transactions[payload.tx_id] = payload.data
    print(f"[{db_file}] Giai đoạn 1: Đã nhận dữ liệu tạm cho TxID: {payload.tx_id}")
    return {"status": "prepared", "tx_id": payload.tx_id}

# GIAI ĐOẠN 2 - KỊCH BẢN A: COMMIT - Ghi chính thức vào file
@app.post("/transaction/commit")
def tx_commit(tx_id: str):
    if tx_id not in pending_transactions:
        raise HTTPException(status_code=404, detail="Không tìm thấy mã giao dịch chờ xử lý")
    
    # Lấy từ bộ nhớ tạm ra và ghi vĩnh viễn vào TinyDB
    data_to_write = pending_transactions[tx_id]
    db.insert(data_to_write)
    
    # Xóa khỏi bộ nhớ tạm giải phóng bộ nhớ
    del pending_transactions[tx_id]
    print(f"[{db_file}] Giai đoạn 2: COMMIT thành công cho TxID: {tx_id}")
    return {"status": "committed"}

# GIAI ĐOẠN 2 - KỊCH BẢN B: ABORT - Hủy bỏ dữ liệu tạm (Rollback)
@app.post("/transaction/abort")
def tx_abort(tx_id: str):
    if tx_id in pending_transactions:
        del pending_transactions[tx_id]
        print(f"[{db_file}] Giai đoạn 2: ABORT (Rollback) thành công cho TxID: {tx_id}")
    return {"status": "aborted"}