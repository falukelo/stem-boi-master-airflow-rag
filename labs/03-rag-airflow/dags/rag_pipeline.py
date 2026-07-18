# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
import glob
import shutil
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.sensors.filesystem import FileSensor
from langchain_text_splitters import RecursiveCharacterTextSplitter
from google import genai
from google.genai import types
import chromadb

DATA_DIR = "/opt/airflow/data"
HR_DIR = "/opt/airflow/data/hr"
PROCESSED_DIR = "/opt/airflow/data/processed"
CHROMA_DB_PATH = "/opt/airflow/data/chromadb"

default_args = {
    'owner': 'KX',
    'start_date': datetime(2026, 6, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='lab03_hr_ingestion',
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
    tags=['kx', 'lab3', 'hr', 'ingestion', 'airflow3'],
    description='Lab 3: ท่อส่งข้อมูล HR สกัดเอกสาร PDF และบันทึกเวกเตอร์ลงคลัง kx_hr_documents'
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_hr_document',
        filepath=os.path.join(HR_DIR, '*.pdf'),  # คอยตรวจจับไฟล์ PDF ใด ๆ ในโฟลเดอร์ data/hr
        fs_conn_id='fs_default',
        poke_interval=15,
        timeout=600,
        mode='poke'
    )

    @task
    def read_pdf_task():
        import fitz
        pdf_files = sorted(glob.glob(os.path.join(HR_DIR, "*.pdf")))
        if not pdf_files:
            raise FileNotFoundError(f"ไม่พบไฟล์ PDF ใน {HR_DIR}")
        file_path = pdf_files[0]

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
        client = genai.Client(api_key=gemini_api_key)

        embedded_data = []
        for i, chunk in enumerate(chunks):
            result = client.models.embed_content(
                model="models/gemini-embedding-2",
                contents=chunk,
                config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT")
            )
            embedded_data.append({
                "id": f"hr_{datetime.now().strftime('%Y%m%d%H%M%S')}_{i}",
                "chunk": chunk,
                "embedding": result.embeddings[0].values
            })
        return embedded_data

    @task
    def insert_to_vector_db_task(embedded_data: list):
        chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        collection_name = "kx_hr_documents"  # จัดเก็บในถัง HR แยกต่างหาก
        
        try:
            chroma_client.delete_collection(name=collection_name)
        except Exception:
            pass
        collection = chroma_client.create_collection(name=collection_name)

        pdf_files = sorted(glob.glob(os.path.join(HR_DIR, "*.pdf")))
        source_name = os.path.basename(pdf_files[0])

        ids = [item["id"] for item in embedded_data]
        embeddings = [item["embedding"] for item in embedded_data]
        documents = [item["chunk"] for item in embedded_data]
        metadatas = [{"source": source_name, "chunk_index": i} for i in range(len(embedded_data))]

        collection.add(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas
        )

        # ย้ายไฟล์หลังนำเข้าสำเร็จ
        file_path = pdf_files[0]
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        dest_path = os.path.join(PROCESSED_DIR, f"processed_hr_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf")
        shutil.move(file_path, dest_path)
        
        return f"บันทึกข้อมูล HR เรียบร้อยจำนวน {len(embedded_data)} Chunks"

    raw_text = read_pdf_task()
    chunks = chunk_text_task(raw_text)
    embedded_data = embed_text_task(chunks)
    ingestion_status = insert_to_vector_db_task(embedded_data)

    wait_for_file >> raw_text
