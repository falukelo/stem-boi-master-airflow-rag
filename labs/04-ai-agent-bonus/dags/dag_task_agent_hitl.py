# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.hitl import HITLOperator
import google.generativeai as genai
import chromadb

CHROMA_DB_PATH = "/opt/airflow/data/chromadb"

default_args = {
    'owner': 'KX',
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

# ----------------- เครื่องมือของระบบ (Toolsets) -----------------

def search_hr_db_tool(query: str) -> str:
    """ค้นหาข้อมูลเกี่ยวกับกฎระเบียบและสวัสดิการของแผนก HR (วันลา, WFH, ค่ารักษาพยาบาล) จากคลังข้อมูลเวกเตอร์ kx_hr_documents"""
    try:
        chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        collection = chroma_client.get_collection(name="kx_hr_documents")
        
        gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
        genai.configure(api_key=gemini_api_key)
        query_vector = genai.embed_content(
            model="models/gemini-embedding-2",
            content=query,
            task_type="retrieval_query"
        )['embedding']
        
        results = collection.query(query_embeddings=[query_vector], n_results=2)
        return "\n---\n".join(results['documents'][0])
    except Exception as e:
        return f"ไม่พบข้อมูลสวัสดิการ HR หรือเกิดข้อผิดพลาด: {str(e)}"

def search_it_db_tool(query: str) -> str:
    """ค้นหาข้อมูลเกี่ยวกับคู่มือ IT Support, ความปลอดภัย, คอมพิวเตอร์ และรหัสผ่าน จากคลังข้อมูลเวกเตอร์ kx_it_documents"""
    try:
        chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        collection = chroma_client.get_collection(name="kx_it_documents")
        
        gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
        genai.configure(api_key=gemini_api_key)
        query_vector = genai.embed_content(
            model="models/gemini-embedding-2",
            content=query,
            task_type="retrieval_query"
        )['embedding']

        results = collection.query(query_embeddings=[query_vector], n_results=2)
        return "\n---\n".join(results['documents'][0])
    except Exception as e:
        return f"ไม่พบข้อมูลช่วยเหลือ IT หรือเกิดข้อผิดพลาด: {str(e)}"


with DAG(
    dag_id='lab04_multi_agent_hitl',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['kx', 'lab4', 'multiagent', 'hitl', 'airflow3'],
    description='Lab 4: การทำงานร่วมกันแบบ Multi-Agent (HR/IT Router) และตรวจสอบผลลัพธ์ผ่าน HITL'
) as dag:

    # 1. รับข้อคำถามจากผู้ใช้
    @task
    def receive_user_query(**context):
        dag_run_conf = context.get('dag_run').conf or {}
        # คำถามจำลองเริ่มต้นเกี่ยวกับ IT Support
        query = dag_run_conf.get('query', "รหัสผ่านไอทีต้องตั้งอย่างน้อยกี่ตัวอักษร และเคลมคอมพิวเตอร์ใหม่ได้เมื่อไร?")
        return query

    # 2. วิเคราะห์และแตกแขนงงานแบบมีเงื่อนไขในขั้นตอนเดียวด้วย @task.llm_branch (Airflow 3 ฟีเจอร์)
    @task.llm_branch(llm_conn_id="gemini_conn")
    def route_query_classification(query: str, system_prompt: str = "คุณเป็น LLM Router ตรวจวิเคราะห์และส่งต่อคำถามพนักงาน"):
        prompt = f"""วิเคราะห์คำถามพนักงานต่อไปนี้: "{query}"
ประเภทคำถามแบ่งออกเป็นสองฝ่าย:
- HR Specialist Agent: หากคำถามเกี่ยวกับวันลาพักร้อน, นโยบายการลาทำงาน WFH/Hybrid, ค่ารักษาพยาบาล, dental care, งบการเรียนรู้ (ให้ตอบกลับด้วยคำว่า 'hr_specialist_agent')
- IT Specialist Agent: หากคำถามเกี่ยวกับเครื่องโน้ตบุ๊ก laptop, การขอลิขสิทธิ์โปรแกรม, นโยบายความยากรหัสผ่าน Password complexity, ความปลอดภัยเน็ตเวิร์ก VPN, ปัญหาเทคนิค IT support (ให้ตอบกลับด้วยคำว่า 'it_specialist_agent')

ให้ตอบกลับด้วยคำภาษาอังกฤษที่เป็นชื่อ Task ID ปลายทางที่ต้องการส่งไปทำงานต่อเพียงคำเดียวเท่านั้น ('hr_specialist_agent' หรือ 'it_specialist_agent') ห้ามมีคำชี้แจงหรือประโยคเพิ่มเติมเด็ดขาด"""
        return f"{system_prompt}. Prompt: {prompt}"

    # 4. HR Expert Agent Task (เอเจนต์เฉพาะทาง HR)
    @task.agent(
        llm_conn_id="gemini_conn",
        toolsets=[search_hr_db_tool],
        system_prompt="คุณเป็นเอเจนต์ผู้เชี่ยวชาญด้านกฎระเบียบ HR ให้ใช้เครื่องมือสืบค้นข้อมูล HR และสรุปคำตอบให้ชัดเจนอิงตามแหล่งข้อมูลเท่านั้น"
    )
    def hr_specialist_agent(query: str):
        return query

    # 5. IT Expert Agent Task (เอเจนต์เฉพาะทาง IT)
    @task.agent(
        llm_conn_id="gemini_conn",
        toolsets=[search_it_db_tool],
        system_prompt="คุณเป็นเอเจนต์ผู้เชี่ยวชาญด้านระบบ IT Support และความปลอดภัย ให้ใช้เครื่องมือสืบค้นข้อมูล IT และสรุปคู่มือตอบกลับพนักงานให้ชัดเจนและปลอดภัย"
    )
    def it_specialist_agent(query: str):
        return query

    # 6. รวบรวมคำตอบจากเอเจนต์ย่อยที่ถูกเลือก (Collector Task)
    @task(trigger_rule="one_success")
    def collect_agent_response(hr_resp=None, it_resp=None):
        # ดึงคำตอบจากตัวแปรที่รันสำเร็จ
        final_resp = hr_resp or it_resp
        return final_resp

    # 7. คอยหยุดเพื่อทวนสอบความถูกต้องโดยผู้ดูแลระบบ (Human-in-the-Loop)
    wait_for_human_review = HITLOperator(
        task_id='wait_for_human_review',
        message="กรุณาตรวจสอบเนื้อหาและอนุมัติคำตอบที่วิเคราะห์โดย Multi-Agent ระบบด้านล่างนี้",
        context_data={
            "agent_response": "{{ task_instance.xcom_pull(task_ids='collect_agent_response') }}",
            "original_query": "{{ task_instance.xcom_pull(task_ids='receive_user_query') }}"
        }
    )

    # 8. บันทึกคำตอบลงในเครื่องเซิร์ฟเวอร์หลัก
    @task
    def save_final_response(approved_content: str, query: str):
        output_file = "/opt/airflow/data/final_responses.txt"
        log_entry = f"--- [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ---\n"
        log_entry += f"คำถาม: {query}\n"
        log_entry += f"คำตอบที่ผ่านการอนุมัติ: {approved_content}\n\n"
        
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
        return "สำเร็จ: ข้อมูลคำตอบได้ผ่านการทวนสอบและจัดเก็บเรียบร้อยแล้ว"

    # การโยงลำดับการไหลของข้อมูลความสัมพันธ์
    query_val = receive_user_query()
    branch_val = route_query_classification(query_val)
    
    # กำหนดเส้นทางแบบแตกแขนง
    hr_val = hr_specialist_agent(query_val)
    it_val = it_specialist_agent(query_val)
    
    # รวบรวมผลลัพธ์
    collector_val = collect_agent_response(hr_val, it_val)
    save_val = save_final_response(collector_val, query_val)

    # กำหนดลำดับงานเชิงกราฟ (Graph Dependencies)
    query_val >> branch_val
    branch_val >> hr_specialist_agent >> collector_val
    branch_val >> it_specialist_agent >> collector_val
    collector_val >> wait_for_human_review >> save_val
