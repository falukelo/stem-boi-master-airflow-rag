# Mastering Workflow Orchestration: A Data Engineer's Guide to RAG on Airflow

ยินดีต้อนรับสู่คลังข้อมูลการเรียนการสอน (Repository) สำหรับหลักสูตร **"Mastering Workflow Orchestration: A Data Engineer's Guide to RAG on Airflow"** หลักสูตร 1 วัน (6 ชั่วโมง) ที่ปูพื้นฐานงาน Data Engineering และสอนการพัฒนาระบบท่อส่งข้อมูล RAG (Retrieval-Augmented Generation) และ AI Agent แบบอัตโนมัติบนระบบปฏิบัติการ Apache Airflow 3.x ร่วมกับ Google Gemini API (Google AI Studio)

---

## 1. ผลลัพธ์การเรียนรู้ (Learning Outcomes)

เมื่อจบหลักสูตรนี้ ผู้เรียนจะมีความรู้ความสามารถดังนี้:
*   **LO-1 (Airflow 3)**: อธิบายบทบาทหน้าที่ของ Data Engineer และส่วนควบคุมพื้นฐานของ Apache Airflow 3.x ได้
*   **LO-2 (RAG & Extraction)**: เข้าใจสถาปัตยกรรม RAG และเขียนสคริปต์สกัดข้อความจากเอกสาร PDF และทำการแบ่งคำ (Chunking) ได้
*   **LO-3 (Vector Database)**: สร้าง นำเข้าข้อมูลเวกเตอร์ และเรียกค้นหาคำตอบ (Retrieve) จากฐานข้อมูล ChromaDB ได้
*   **LO-4 (Orchestration & @task.llm)**: สร้างท่อข้อมูล RAG อัตโนมัติบน Airflow 3.x โดยใช้ File Sensor และชุดตกแต่ง `@task.llm` ร่วมกับ Gemini API ได้
*   **LO-5 (AI Agent & HITL)**: สร้างระบบเอเจนต์ที่ตัดสินใจเลือกเครื่องมือค้นข้อมูลด้วยตนเองผ่าน `@task.agent` ร่วมกับระบบหยุดรออนุมัติโดยมนุษย์ (`HITLOperator`) ได้
*   **LO-6 (MLOps/DataOps)**: อธิบายแนวทางการควบคุมคุณภาพท่อส่งและวงจรชีวิตของ Embedding/Models ในระบบจริงได้

---

## 2. ลำดับการเรียนการสอน (Sequence)

| เวลา | Module | รูปแบบการสอน | แหล่งเรียนรู้ & ห้องปฏิบัติการ (Labs) |
| :--- | :--- | :--- | :--- |
| **09:00–10:00** | [01 Data Engineers Foundations & Airflow Mastery Concepts](./labs/01-foundation/README.md) | 45 นาทีทฤษฎี + 15 นาที Airflow UI walkthrough | สื่อการสอนพื้นฐาน Data Pipeline และสถาปัตยกรรม Airflow 3.x |
| **10:00–12:00** | [02 RAG Pipeline Architecture for Data Engineers](./labs/02-rag-architecture/README.md) | 60 นาทีทฤษฎี + 60 นาที Lab | [Lab 02: RAG with ChromaDB](./labs/02-rag-architecture/notebooks/rag_intro.ipynb) <br> *(ใช้ทดสอบอ่านไฟล์ PDF `policy_wfh.pdf`)* |
| **12:00–13:00** | *พักกลางวัน (Lunch Break)* | — | — |
| **13:00–14:30** | [03 Project Workshop: Implementing RAG via Airflow Operators](./labs/03-rag-airflow/README.md) | 25 นาที demo + 65 นาที Lab | [Lab 03: RAG Ingestion with Airflow](./labs/03-rag-airflow/dags/rag_pipeline.py) <br> *(ใช้ทดสอบดึง PDF อัตโนมัติผ่าน File Sensor)* |
| **14:30–15:30** | [04 Bonus: Introduction to AI Agent Orchestration](./labs/04-ai-agent-bonus/README.md) | 25 นาทีเนื้อหา + 35 นาที Lab | **Lab 4A (RAG @task.llm):** [dag_task_llm.py](./labs/04-ai-agent-bonus/dags/dag_task_llm.py)<br>**Lab 4B (Agent & HITL):** [dag_task_agent_hitl.py](./labs/04-ai-agent-bonus/dags/dag_task_agent_hitl.py) |
| **15:30–16:00** | [05 MLOps/DataOps](./labs/05-mlops-dataops/README.md) | 22 นาทีสรุป + 8 นาที Q&A และ exit ticket | สรุปกรอบความคิด DataOps และ MLOps สำหรับระบบโปรดักชัน |

---

## 3. โครงสร้างคลังข้อมูล (Repository Structure)

```text
boi-master-airflow-rag/
├── docs/                             # เอกสารทฤษฎีและข้อตัดสินใจเชิงเทคนิค
│   ├── COURSE_CONTEXT.md             # รายละเอียดหลักสูตรและกลุ่มผู้เรียน
│   ├── course-blueprint.md           # แผนพิมพ์เขียวหลักสูตรและเป้าหมาย LO
│   └── technical-decisions.md         # สเปกเวอร์ชันเทคโนโลยีและข้อกำหนดการเขียน DAG
├── labs/                             # โฟลเดอร์แล็บทั้ง 5 เซสชัน
│   ├── mock_documents/               # คลังเอกสาร PDF สำหรับใช้ในการทดสอบแล็บ
│   │   ├── policy_wfh.pdf            # เอกสารนโยบาย WFH (ใช้ในแล็บ 2)
│   │   ├── policy_leave.pdf          # เอกสารนโยบายวันลาพักร้อน (ใช้ในแล็บ 3/4)
│   │   ├── policy_medical.pdf        # เอกสารนโยบายรักษาพยาบาล (ใช้ในแล็บ 3/4)
│   │   └── policy_training.pdf       # เอกสารงบเรียนรู้อัปสกิล (ใช้ในแล็บ 3/4)
│   ├── 01-foundation/                # เอกสารปูพื้นฐาน DE และการสแกนหน้า UI
│   ├── 02-rag-architecture/          # แล็บ Jupyter สกัดข้อความจาก PDF และทำ RAG
│   ├── 03-rag-airflow/               # แล็บรัน Airflow 3 ตรวจจับ PDF และทำ RAG อัตโนมัติ
│   ├── 04-ai-agent-bonus/            # แล็บ AI Agent (@task.agent) + อนุมัติโดยมนุษย์ (HITL)
│   └── 05-mlops-dataops/             # เอกสารทบทวนการมอนิเตอร์และดูแลท่อส่งในโปรดักชัน
└── รายละเอียดหลักสูตร KX_ Mastering Workflow .pdf   # รายละเอียดสเกดดูลหลักสูตรต้นฉบับ
```

---

## 4. วิธีการใช้งานคลังข้อมูลจำลอง (Getting Started)

### ความต้องการของระบบ (Prerequisites)
1.  **Docker & Docker Compose**: สำหรับสั่งเปิดสภาพแวดล้อมจำลอง Jupyter และ Airflow 3
2.  **Google AI Studio API Key**: ลงทะเบียนขอรับ API Key ฟรีเพื่อเปิดสิทธิ์คุยกับ Gemini และแปลงเวกเตอร์ความรู้

### การเตรียมคีย์ใน Terminal ของคุณ
ก่อนเริ่มทำแล็บ ให้พิมพ์คำสั่งส่งออกคีย์ในหน้าต่างคำสั่ง:
```bash
export GEMINI_API_KEY="your-actual-api-key-here"
```

### การตั้งค่าสิทธิ์ผู้เขียนสำหรับ Docker (macOS/Linux)
สำหรับรันในระบบปฏิบัติการ Linux/macOS ให้พิมพ์คำสั่งสร้างสิทธิ์เพื่อให้คอนเทนเนอร์สามารถเขียนไฟล์ลงในเครื่องโฮสต์ของคุณได้สำเร็จ:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > labs/03-rag-airflow/.env
```
