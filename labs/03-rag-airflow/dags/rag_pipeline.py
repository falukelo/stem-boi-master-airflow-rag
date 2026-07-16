# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
import shutil
from airflow import DAG
from airflow.decorators import task
# ใน Airflow 3 ระบบย้าย FileSensor ไปอยู่ใน Standard Provider
from airflow.providers.standard.sensors.filesystem import FileSensor
from langchain_text_splitters import RecursiveCharacterTextSplitter
import google.generativeai as genai
import chromadb

# กำหนดโฟลเดอร์สำหรับตรวจจับไฟล์
DATA_DIR = "/opt/airflow/data"
PROCESSED_DIR = "/opt/airflow/data/processed"
CHROMA_DB_PATH = "/opt/airflow/data/chromadb"

default_args = {
    'owner': 'Astra',
    'start_date': datetime(2026, 6, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='automated_rag_pipeline',
    default_args=default_args,
    schedule_interval=None,  # รันเฉพาะกิจ หรือทริกเกอร์มือ
    catchup=False,
    tags=['kx', 'rag', 'airflow3'],
    description='ท่อส่งข้อมูล RAG อัตโนมัติเมื่อมีไฟล์เอกสารใหม่เข้ามาวาง'
) as dag:

    # 1. รอตรวจจับไฟล์ใหม่ในไดเรกทอรี
    wait_for_file = FileSensor(
        task_id='wait_for_new_document',
        filepath='new_doc.txt',  # รอไฟล์ที่ชื่อ new_doc.txt ในไดเรกทอรีเชื่อมต่อ
        fs_conn_id='fs_default',  # ระบุ File Connection ID
        poke_interval=15,
        timeout=600,
        mode='poke'
    )

    # 2. ทำการสกัด แบ่งข้อความ และเก็บเข้า ChromaDB
    @task
    def process_and_ingest_document():
        # เชื่อมโยงโมเดล Gemini โดยใช้ API Key จาก Env
        gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set!")
        genai.configure(api_key=gemini_api_key)

        file_path = os.path.join(DATA_DIR, "new_doc.txt")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ไม่พบไฟล์สำหรับประมวลผลที่ {file_path}")

        # อ่านข้อมูลดิบ
        with open(file_path, "r", encoding="utf-8") as f:
            document_content = f.read()

        # ทำ Chunking
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=250,
            chunk_overlap=50,
            separators=["\n\n", "\n", " ", ""]
        )
        chunks = splitter.split_text(document_content)

        # ตั้งค่า ChromaDB Client
        # ใช้ PersistentClient เพื่อให้ข้อมูลไม่หายไปเมื่อปิดคอนเทนเนอร์
        chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        collection_name = "kx_airflow_documents"
        
        # รีเซ็ตหรือOverwriteเพื่อความทำงานซ้ำได้ผลเดิม (Idempotency)
        try:
            chroma_client.delete_collection(name=collection_name)
        except Exception:
            pass
        collection = chroma_client.create_collection(name=collection_name)

        # บันทึกข้อมูลและเวกเตอร์
        for i, chunk in enumerate(chunks):
            # เรียกแปลงเวกเตอร์ผ่าน Gemini Embeddings
            result = genai.embed_content(
                model="models/text-embedding-004",
                contents=chunk,
                task_type="retrieval_document"
            )
            vector = result['embedding']
            
            collection.add(
                ids=[f"doc_{i}"],
                embeddings=[vector],
                documents=[chunk],
                metadatas=[{"source": "new_doc.txt", "chunk_index": i}]
            )

        # เคลื่อนย้ายไฟล์ที่ประมวลผลแล้วไปโฟลเดอร์อื่นเพื่อไม่ให้รันซ้ำซ้อน
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        dest_path = os.path.join(PROCESSED_DIR, f"processed_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt")
        shutil.move(file_path, dest_path)

        # ส่งต่อเฉพาะหัวข้อหรือ ID ไป Task ถัดไป (Claim Check Pattern)
        return "สำเร็จ: โหลดข้อมูล RAG และนำไฟล์เข้าโฟลเดอร์เก็บข้อมูลถาวรเรียบร้อย"

    # 3. สรุปใจความสำคัญของข้อมูลใหม่ผ่าน @task.llm (Airflow 3 ฟีเจอร์)
    # ฟังก์ชันนี้จำลองการใช้งาน LLM Operator ที่คุยกับ Gemini คิวรีผลลัพธ์ผ่าน Prompt
    @task.llm(llm_conn_id="gemini_conn")
    def summarize_new_data(ingest_status: str, prompt: str = "กรุณาสรุปใจความสำคัญของข้อมูลที่เพิ่งโหลดเข้าสู่ Vector DB"):
        # หมายเหตุ: ใน Airflow 3 ฟังก์ชันที่มีการตกแต่งด้วย @task.llm จะประมวลผลคำขอ LLM 
        # และการส่งต่อพารามิเตอร์ผ่าน Provider Connection ไปหา Gemini โดยอัตโนมัติ
        return f"{prompt}. สถานะข้อมูล: {ingest_status}"

    # กำหนดความสัมพันธ์
    ingest_task = process_and_ingest_document()
    summary_task = summarize_new_data(ingest_task)

    wait_for_file >> ingest_task
