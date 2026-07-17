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
    schedule=None,
    catchup=False,
    tags=['kx', 'lab3', 'query', 'llm', 'airflow3'],
    description='Lab 3: สอบถามข้อมูล RAG (HR/IT) แยกถัง และตอบกลับผ่านโมเดล Gemini โดยใช้ @task.llm'
) as dag:

    # 1. รับข้อคำถามและขอบเขตโดเมน (HR หรือ IT) จากผู้ใช้ผ่านพารามิเตอร์ของ DAG
    @task
    def receive_query_and_domain(**context):
        dag_run_conf = context.get('dag_run').conf or {}
        query = dag_run_conf.get('query', "ฉันมีสิทธิ์ได้รับวันลาพักร้อนปีละกี่วันทำการ?")
        domain = dag_run_conf.get('domain', "hr")  # กำหนดค่าเริ่มต้นเป็น hr (สามารถส่ง 'it' มาได้)
        return {"query": query, "domain": domain}

    # 2. ค้นหาเอกสารอ้างอิงจากคลังข้อมูลเวกเตอร์ ChromaDB ตามขอบเขตโดเมน
    @task
    def retrieve_context(params: dict):
        query = params["query"]
        domain = params["domain"].lower().strip()
        
        # เลือกคอลเลกชันตามโดเมน
        if domain == "it":
            collection_name = "kx_it_documents"
        else:
            collection_name = "kx_hr_documents"
            
        try:
            chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
            collection = chroma_client.get_collection(name=collection_name)
            
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
            return {"context": "\n---\n".join(contexts), "query": query, "domain": domain}
        except Exception as e:
            return {"context": f"ไม่มีข้อมูลในฐานข้อมูลเวกเตอร์ของถัง {domain} หรือเกิดข้อผิดพลาด: {str(e)}", "query": query, "domain": domain}

    # 3. ตอบกลับข้อมูลสรุปผ่านฟีเจอร์ระดับสูง @task.llm (Airflow 3)
    @task.llm(llm_conn_id="gemini_conn")
    def generate_response_with_llm(data: dict, system_prompt: str = "คุณเป็นเจ้าหน้าที่บริการตอบคำถามพนักงานอย่างสุภาพโดยยึดตามข้อมูลอ้างอิงเท่านั้น"):
        context_data = data["context"]
        query = data["query"]
        domain = data["domain"].upper()
        
        user_prompt = f"ขอบเขตข้อมูลวิเคราะห์: แผนก {domain}\nเอกสารอ้างอิง:\n{context_data}\n\nคำถามจากพนักงาน: {query}"
        return f"{system_prompt}. Prompt: {user_prompt}"

    # ลำดับโฟลว์งาน
    params_val = receive_query_and_domain()
    context_data_val = retrieve_context(params_val)
    response_val = generate_response_with_llm(context_data_val)
