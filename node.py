# node.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tinydb import TinyDB
import os

app = FastAPI()

# Tự động lấy tên file dữ liệu được chỉ định, nếu không có sẽ tự tạo file 'node_data.json'
DB_FILE = os.getenv("DB_FILE", "data/node_data.json")
db = TinyDB(DB_FILE)

class Document(BaseModel):
    data: dict

@app.get("/")
def check_status():
    return {"status": "online", "database_storage": DB_FILE}

@app.post("/insert")
def insert_document(doc: Document):
    try:
        doc_id = db.insert(doc.data)
        return {"status": "success", "assigned_id": doc_id, "stored_data": doc.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi ghi dữ liệu: {str(e)}")

@app.get("/all")
def get_all_documents():
    return db.all()