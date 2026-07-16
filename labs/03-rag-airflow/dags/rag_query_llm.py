# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
import google.generativeai as genai
import chromadb

CHROMA_DB_PATH = "/opt/airflow/data/chromadb"

default_args = {
    'owner': 'Astra',
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='lab03_rag_query_llm',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['kx', 'lab3', 'query', 'llm', 'airflow3'],
    description='Lab 3: สอบถามข้อมูล RAG และตอบกลับผ่านโมเดล Gemini โดยใช้ @task.llm'
) as dag:

    # 1. รับข้อคำถามจากผู้ใช้ผ่านพารามิเตอร์ของ DAG (DAG Run Configuration)
    @task
    def receive_query(**context):
        dag_run_conf = context.get('dag_run').conf
        # ค่าตั้งต้นจะถามหาเรื่องวันลาพักร้อน
        query = dag_run_conf.get('query', "ฉันมีสิทธิ์ได้รับวันลาพักร้อนปีละกี่วันทำการ?")
        return query

    # 2. ค้นหาเอกสารอ้างอิงจากคลังข้อมูลเวกเตอร์ ChromaDB
    @task
    def retrieve_context(query: str):
        try:
            chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
            collection = chroma_client.get_collection(name="kx_airflow_documents")
            
            gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
            genai.configure(api_key=gemini_api_key)
            
            # แปลงคำถามเป็นเวกเตอร์
            query_vector = genai.embed_content(
                model="models/text-embedding-004",
                contents=query,
                task_type="retrieval_query"
            )['embedding']
            
            # ค้นหา 2 ชิ้นที่ใกล้ที่สุด
            results = collection.query(
                query_embeddings=[query_vector],
                n_results=2
            )
            contexts = results['documents'][0]
            return "\n---\n".join(contexts)
        except Exception as e:
            return f"ไม่มีข้อมูลในฐานข้อมูลเวกเตอร์ หรือเกิดข้อผิดพลาด: {str(e)}"

    # 3. ตอบกลับข้อมูลสรุปผ่านฟีเจอร์ระดับสูง @task.llm (Airflow 3)
    @task.llm(llm_conn_id="gemini_conn")
    def generate_response_with_llm(context_data: str, query: str, system_prompt: str = "คุณเป็นเจ้าหน้าที่ HR ตอบคำถามพนักงานอย่างสุภาพโดยยึดตามข้อมูลอ้างอิงเท่านั้น"):
        # ใน Airflow 3 ฟังก์ชันที่มีการตกแต่งด้วย @task.llm จะดึงสิทธิ์ Connection gemini_conn
        # มาเรียกประมวลผลคำตอบจาก Gemini API โดยอัตโนมัติ
        user_prompt = f"เอกสารอ้างอิงสวัสดิการพนักงาน:\n{context_data}\n\nคำถามจากพนักงาน: {query}"
        return f"{system_prompt}. Prompt: {user_prompt}"

    # ลำดับโฟลว์งาน
    query_val = receive_query()
    context_val = retrieve_context(query_val)
    response_val = generate_response_with_llm(context_val, query_val)
