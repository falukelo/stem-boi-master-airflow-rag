# Lab 04: AI Agent Orchestration & Human-in-the-Loop (HITL)

ห้องปฏิบัติการนี้แบ่งออกเป็น 2 ส่วนการทดสอบ เพื่อแสดงพัฒนาการจากการทำท่อส่งข้อมูลแบบ RAG ดั้งเดิม ไปจนถึงการใช้งาน **AI Agentic Workflows** บน **Apache Airflow 3.x** ร่วมกับขั้นตอนอนุมัติโดยมนุษย์ (Human-in-the-Loop) ผ่านระบบจัดการ Task State ลำดับสูง

---

## 1. สิ่งที่คุณจะได้เรียนรู้ (Learning Objectives)
*   **Lab 4A**: พัฒนาท่อสืบค้นและตอบคำถามแบบธรรมดาโดยใช้ชุดตกแต่ง `@task.llm` บน Airflow 3.x
*   **Lab 4B**: พัฒนา AI Agent ที่มีอิสระในการเรียกคิวรีข้อมูลเวกเตอร์ผ่าน Toolsets ด้วยตนเองโดยใช้ `@task.agent` ร่วมกับ `HITLOperator` ในการสั่ง Pause พักระบบเพื่อรอผู้ใช้ยืนยันตรวจทานคำตอบก่อนตอบพนักงาน

---

## 2. ขั้นตอนการตั้งค่าและการรันแล็บ (Running Instructions)

*หมายเหตุ: แล็บบทความนี้ใช้งานสภาพแวดล้อมรัน Docker ตัวเดียวกับ Lab 3 ดังนั้นตรวจสอบให้แน่ใจว่าระบบในโฟลเดอร์ `labs/03-rag-airflow` รันอยู่เรียบร้อยแล้ว*

### ขั้นตอนที่ 1: คัดลอกไฟล์ DAG เข้าสู่ระบบรันหลัก
คัดลอกทั้งสองไฟล์ตัวอย่างไปยังโฟลเดอร์รัน DAG ของ Airflow:
```bash
# คัดลอกไฟล์ DAG ทั้งสองตัว
cp labs/04-ai-agent-bonus/dags/dag_task_llm.py labs/03-rag-airflow/dags/
cp labs/04-ai-agent-bonus/dags/dag_task_agent_hitl.py labs/03-rag-airflow/dags/
```

---

## 3. การทดสอบแล็บ 4A: Simple RAG via `@task.llm`
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab4a_task_llm_rag` บนหน้า Airflow UI
2. สั่งรันโดยเลือก **Trigger DAG w/ config** และระบุคำถามเป็น JSON เช่น:
   ```json
   {
     "query": "ฉันเบิกค่าจัดฟันได้สูงสุดกี่บาท?"
   }
   ```
3. เมื่อทำงานสำเร็จ ให้เข้าไปดูผลลัพธ์คำตอบที่คิวรีได้ใน XCom หรือ Logs ของ Task `generate_response_with_llm`

---

## 4. การทดสอบแล็บ 4B: AI Agent & HITL via `@task.agent`
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab4b_task_agent_hitl`
2. สั่งรันโดยเลือก **Trigger DAG w/ config** และป้อนคำถามเป็น JSON เช่น:
   ```json
   {
     "query": "ฉันเบิกเงินค่ารักษาพยาบาลได้สูงสุดปีละเท่าไร และสามารถยื่นเบิกได้ช่องทางไหน?"
   }
   ```
3. สังเกตบนหน้าจอ Airflow UI: DAG จะวิเคราะห์ผ่านเอเจนต์และหยุดพักอยู่ที่ Task `wait_for_human_review` โดยเปลี่ยนสถานะเป็นสีส้ม/น้ำเงินอ่อน (**`awaiting_input`**)
4. กดที่ Task `wait_for_human_review` -> เลือกแถบเมนูย่อย **HITL / Approval Request**
5. ตรวจทานคำถามดั้งเดิมและคำตอบที่ AI Agent คิดค้นมา หากถูกต้อง ให้กดปุ่ม **Approve** เพื่อสั่งดำเนินท่อส่งข้อมูลต่อเพื่อนำผลลัพธ์เก็บลงระบบฐานข้อมูลถาวร

