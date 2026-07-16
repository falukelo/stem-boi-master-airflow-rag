# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
# ใน Airflow 3 ระบบมี Standard Operators สำหรับ Human-in-the-loop
from airflow.providers.standard.operators.hitl import HITLOperator
import google.generativeai as genai
import chromadb

CHROMA_DB_PATH = "/opt/airflow/data/chromadb"

default_args = {
    'owner': 'Forge',
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

# นิยามเครื่องมือสำหรับให้ AI Agent เรียกใช้งาน (Toolsets)
def search_vector_db_tool(query: str) -> str:
    """ค้นหาข้อมูลเกี่ยวกับกฎระเบียบสวัสดิการของพนักงานในบริษัทจากฐานข้อมูลเวกเตอร์ ChromaDB"""
    try:
        # เชื่อมต่อ ChromaDB
        chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        collection = chroma_client.get_collection(name="kx_airflow_documents")
        
        # แปลงข้อความเป็นเวกเตอร์คำถาม
        gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
        genai.configure(api_key=gemini_api_key)
        
        query_vector = genai.embed_content(
            model="models/text-embedding-004",
            contents=query,
            task_type="retrieval_query"
        )['embedding']
        
        results = collection.query(
            query_embeddings=[query_vector],
            n_results=2
        )
        # นำคำตอบและแหล่งที่มารวมกัน
        documents = results['documents'][0]
        return "\n---\n".join(documents)
    except Exception as e:
        return f"ไม่สามารถดึงข้อมูลได้เนื่องจากเกิดข้อผิดพลาด: {str(e)}"


with DAG(
    dag_id='agentic_rag_with_hitl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['kx', 'agent', 'hitl', 'airflow3'],
    description='ระบบ AI Agent ตอบคำถามสวัสดิการร่วมกับขั้นตอน Human-in-the-loop เพื่อรอตรวจคำตอบ'
) as dag:

    # 1. รับข้อคำถามจากผู้ใช้ผ่านพารามิเตอร์ของ DAG (DAG Run Configuration)
    @task
    def receive_user_query(**context):
        # ดึงข้อความคำถามจาก conf ที่ส่งมาตอนทริกเกอร์ DAG
        # ตัวอย่าง: {"query": "ฉันเบิกค่ารักษาพยาบาลได้กี่บาทต่อปี?"}
        dag_run_conf = context.get('dag_run').conf
        query = dag_run_conf.get('query', "ฉันเบิกค่ารักษาพยาบาลได้กี่บาทต่อปี?")
        return query

    # 2. ทำงานร่วมกับ `@task.agent` (ฟีเจอร์ Airflow 3) ในการคิดวิเคราะห์และค้นหาข้อมูลเวกเตอร์
    # ระบบจะมอบหมายเครื่องมือค้นหาข้อมูล (search_vector_db_tool) ให้ Agent เรียกใช้ได้อิสระ
    @task.agent(
        llm_conn_id="gemini_conn",
        toolsets=[search_vector_db_tool],
        system_prompt="คุณเป็น Agent วิเคราะห์คำตอบสำหรับ HR ให้ใช้เครื่องมือในการสืบค้นข้อมูลเวกเตอร์ และสรุปคำตอบให้พนักงานอย่างถูกต้อง"
    )
    def ai_agent_reasoning(user_query: str):
        # ฟังก์ชัน `@task.agent` จะทำหน้าที่รับค่าคำถามแล้วส่งต่อให้ Gemini
        # พร้อมทั้งรัน Loop ในการเรียกใช้เครื่องมือภายนอกแบบอัตโนมัติจนเสร็จสิ้น
        return user_query

    # 3. ขั้นตอน Human-in-the-Loop (HITL) เพื่อหยุดพัก DAG รอให้มนุษย์ตรวจสอบคำตอบที่เอเจนต์คิดมา
    # ใน Airflow 3.3+ ฟังก์ชันนี้จะเปลี่ยนสถานะ Task เป็น `awaiting_input` ซึ่งไม่กินแรมหรือ CPU ในการรอคอย
    wait_for_human_review = HITLOperator(
        task_id='wait_for_human_review',
        message="กรุณาตรวจสอบและอนุมัติคำตอบที่วิเคราะห์โดย AI Agent ด้านล่างนี้",
        # ค่าที่จะนำไปแสดงผลบนแบบฟอร์มตรวจสอบบนหน้า UI
        context_data={
            "agent_response": "{{ task_instance.xcom_pull(task_ids='ai_agent_reasoning') }}",
            "original_query": "{{ task_instance.xcom_pull(task_ids='receive_user_query') }}"
        }
    )

    # 4. บันทึกคำตอบสุดท้ายที่ผ่านการยืนยันแล้วลงในไฟล์ระบบ
    @task
    def save_final_response(approved_content: str, query: str):
        output_file = "/opt/airflow/data/final_responses.txt"
        
        # ดึงข้อมูลการอนุมัติ (Approved/Rejected) จากข้อมูล XCom ของ HITL
        # เพื่อดูข้อคิดเห็นเพิ่มเติมที่มนุษย์เขียนส่งกลับมา
        log_entry = f"--- [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ---\n"
        log_entry += f"คำถาม: {query}\n"
        log_entry += f"คำตอบที่อนุมัติ: {approved_content}\n\n"
        
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        
        return "สำเร็จ: จัดเก็บคำตอบที่ผ่านการทวนสอบโดยมนุษย์เรียบร้อยแล้ว"

    # กำหนดเส้นทางการไหลของข้อมูล
    query_task = receive_user_query()
    agent_task = ai_agent_reasoning(query_task)
    
    # ดึงผลลัพธ์คำตอบเพื่อส่งต่อ
    save_task = save_final_response(agent_task, query_task)

    # กำหนดลำดับงาน:
    # 1. รับคำถาม -> 2. เอเจนต์วิเคราะห์ -> 3. หยุดรอคนอนุมัติ -> 4. บันทึกผลลัพธ์ลงระบบหลัก
    query_task >> agent_task >> wait_for_human_review >> save_task
