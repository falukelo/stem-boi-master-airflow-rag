# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
import google.generativeai as genai
import chromadb

CHROMA_DB_PATH = "/opt/airflow/data/chromadb"

default_args = {
    'owner': 'Forge',
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='lab4a_task_llm_rag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['kx', 'lab4a', 'llm', 'airflow3'],
    description='Lab 4A: ค้นหาและสรุปคำตอบ RAG ด้วย @task.llm (Airflow 3)'
) as dag:

    # 1. รับข้อคำถามจากผู้ใช้ผ่านพารามิเตอร์ของ DAG (DAG Run Configuration)
    @task
    def receive_query(**context):
        dag_run_conf = context.get('dag_run').conf
        query = dag_run_conf.get('query', "ฉันเบิกค่าจัดฟันได้สูงสุดกี่บาท?")
        return query

    # 2. ค้นหาเอกสารอ้างอิงที่ใกล้เคียงจาก ChromaDB
    @task
    def retrieve_context(query: str):
        try:
            chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
            collection = chroma_client.get_collection(name="kx_airflow_documents")
            
            # แปลงคำถามเป็นเวกเตอร์
            gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
            genai.configure(api_key=gemini_api_key)
            
            query_vector = genai.embed_content(
                model="models/text-embedding-004",
                contents=query,
                task_type="retrieval_query"
            )['embedding']
            
            # ดึง 2 ชิ้นที่ตรงที่สุด
            results = collection.query(
                query_embeddings=[query_vector],
                n_results=2
            )
            contexts = results['documents'][0]
            return "\n---\n".join(contexts)
        except Exception as e:
            return f"ดึงข้อมูลล้มเหลว: {str(e)}"

    # 3. ส่งข้อมูลสรุปคำตอบโดยตรงผ่าน @task.llm (Airflow 3)
    @task.llm(llm_conn_id="gemini_conn")
    def generate_response_with_llm(context_data: str, query: str, system_prompt: str = "คุณเป็น AI HR ตอบคำถามสุภาพโดยยึดหลักข้อมูลอ้างอิงเท่านั้น"):
        # ใน Airflow 3 ตกแต่งฟังก์ชันด้วย @task.llm จะเรียกใช้งาน Gemini API แบบอัตโนมัติ
        # โดยการส่งต่อพารามิเตอร์ทั้งหมดเข้า Prompt
        user_prompt = f"เอกสารอ้างอิง:\n{context_data}\n\nคำถาม: {query}"
        return f"{system_prompt}. Prompt: {user_prompt}"

    # กำหนดเส้นทาง
    query_val = receive_query()
    context_val = retrieve_context(query_val)
    response_val = generate_response_with_llm(context_val, query_val)
