# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
import shutil
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.sensors.filesystem import FileSensor
from langchain_text_splitters import RecursiveCharacterTextSplitter
import google.generativeai as genai
import chromadb

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
    dag_id='lab03_it_ingestion',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['kx', 'lab3', 'it', 'ingestion', 'airflow3'],
    description='Lab 3: ท่อส่งข้อมูล IT สกัดเอกสาร PDF และบันทึกเวกเตอร์ลงคลัง kx_it_documents'
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_it_document',
        filepath='it_policy.pdf',  # คอยตรวจจับไฟล์ชื่อ it_policy.pdf
        fs_conn_id='fs_default',
        poke_interval=15,
        timeout=600,
        mode='poke'
    )

    @task
    def read_pdf_task():
        import fitz
        file_path = os.path.join(DATA_DIR, "it_policy.pdf")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ไม่พบไฟล์ที่ {file_path}")
            
        doc = fitz.open(file_path)
        document_content = ""
        for page in doc:
            document_content += page.get_text()
            
        return document_content

    @task
    def chunk_text_task(raw_text: str):
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=250,
            chunk_overlap=50,
            separators=["\n\n", "\n", " ", ""]
        )
        return splitter.split_text(raw_text)

    @task
    def embed_text_task(chunks: list):
        gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY is not set!")
        genai.configure(api_key=gemini_api_key)
        
        embedded_data = []
        for i, chunk in enumerate(chunks):
            result = genai.embed_content(
                model="models/text-embedding-004",
                contents=chunk,
                task_type="retrieval_document"
            )
            embedded_data.append({
                "id": f"it_{datetime.now().strftime('%Y%m%d%H%M%S')}_{i}",
                "chunk": chunk,
                "embedding": result['embedding']
            })
        return embedded_data

    @task
    def insert_to_vector_db_task(embedded_data: list):
        chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        collection_name = "kx_it_documents"  # จัดเก็บในถัง IT แยกต่างหาก
        
        try:
            chroma_client.delete_collection(name=collection_name)
        except Exception:
            pass
        collection = chroma_client.create_collection(name=collection_name)
        
        ids = [item["id"] for item in embedded_data]
        embeddings = [item["embedding"] for item in embedded_data]
        documents = [item["chunk"] for item in embedded_data]
        metadatas = [{"source": "it_policy.pdf", "chunk_index": i} for i in range(len(embedded_data))]
        
        collection.add(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas
        )
        
        # ย้ายไฟล์หลังนำเข้าสำเร็จ
        file_path = os.path.join(DATA_DIR, "it_policy.pdf")
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        dest_path = os.path.join(PROCESSED_DIR, f"processed_it_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf")
        shutil.move(file_path, dest_path)
        
        return f"บันทึกข้อมูล IT เรียบร้อยจำนวน {len(embedded_data)} Chunks"

    raw_text = read_pdf_task()
    chunks = chunk_text_task(raw_text)
    embedded_data = embed_text_task(chunks)
    ingestion_status = insert_to_vector_db_task(embedded_data)

    wait_for_file >> raw_text
