# Mastering Workflow Orchestration: A Data Engineer's Guide to RAG on Airflow

ยินดีต้อนรับสู่คลังข้อมูลการเรียนการสอน (Repository) สำหรับหลักสูตร **"Mastering Workflow Orchestration: A Data Engineer's Guide to RAG on Airflow"** หลักสูตร 1 วัน (6 ชั่วโมง) ที่มุ่งเน้นการปูพื้นฐานการทำ Data Engineering และการสร้างระบบท่อส่งข้อมูลสืบค้นความรู้แบบอัตโนมัติ (RAG Ingestion Pipelines) ด้วย Apache Airflow 3.x และ Google Gemini

---

## 1. ผลลัพธ์การเรียนรู้ (Learning Outcomes)

เมื่อจบหลักสูตรนี้ ผู้เรียนจะมีความรู้ความสามารถดังนี้:
*   **LO-1**: อธิบายบทบาทหน้าที่ของ Data Engineer และหน้าจอควบคุมพื้นฐานของ Apache Airflow 3.x ได้
*   **LO-2**: เข้าใจโครงสร้างสถาปัตยกรรม RAG และสามารถเขียนโค้ดทำการแบ่งคำ (Chunking) และทำ Embedding ตัวอักษรได้
*   **LO-3**: สร้างและเรียกสอบถามข้อมูล (Query) จากฐานข้อมูลเวกเตอร์ (ChromaDB) ได้
*   **LO-4**: พัฒนาท่อส่งข้อมูลอัตโนมัติบน Airflow 3.x โดยใช้ฟังก์ชัน TaskFlow API และ `@task.llm` ร่วมกับ Gemini ได้
*   **LO-5**: พัฒนาเอเจนต์ปัญญาประดิษฐ์ด้วย `@task.agent` ร่วมกับการทำ Human-in-the-loop เพื่อรอตรวจคำตอบก่อนบันทึกจริงได้
*   **LO-6**: อธิบายกระบวนการนำขึ้นระบบโปรดักชันตามหลักการของ DataOps และ MLOps ได้

---

## 2. ลำดับการเรียนการสอน (Sequence)

| เวลา | Module | รูปแบบ | Lab |
| :--- | :--- | :--- | :--- |
| **09:00–10:00** | [01 Data Engineers Foundations & Airflow Mastery Concepts](./labs/01-foundation/README.md) | 45 นาทีเนื้อหา + 15 นาที Airflow UI walkthrough | — |
| **10:00–12:00** | [02 RAG Pipeline Architecture for Data Engineers](./labs/02-rag-architecture/README.md) | 60 นาทีเนื้อหา + 60 นาที Lab | [Lab 02: RAG with ChromaDB](./labs/02-rag-architecture/notebooks/rag_intro.ipynb) |
| **12:00–13:00** | *พักกลางวัน (Lunch Break)* | — | — |
| **13:00–14:30** | [03 Project Workshop: Implementing RAG via Airflow Operators](./labs/03-rag-airflow/README.md) | 25 นาที demo + 65 นาที Lab | [Lab 03: RAG ingestion with Airflow](./labs/03-rag-airflow/dags/rag_pipeline.py) |
| **14:30–15:30** | [04 Bonus: Introduction to AI Agent Orchestration](./labs/04-ai-agent-bonus/README.md) | 25 นาทีเนื้อหา + 35 นาที Lab | [Lab 04A: Simple RAG](./labs/04-ai-agent-bonus/dags/dag_task_llm.py) <br> [Lab 04B: Agent with HITL](./labs/04-ai-agent-bonus/dags/dag_task_agent_hitl.py) |
| **15:30–16:00** | [05 MLOps/DataOps](./labs/05-mlops-dataops/README.md) | 22 นาทีสรุป + 8 นาที Q&A และ exit ticket | — |

---

## 3. โครงสร้างของคลังข้อมูล (Repository Structure)

```text
boi-master-airflow-rag/
├── docs/                             # เอกสารประกอบการสอนเพิ่มเติม
│   ├── COURSE_CONTEXT.md             # รายละเอียดเนื้อหาและขอบเขตหลักสูตร
│   ├── course-blueprint.md           # แผนการสอนและผลลัพธ์การเรียนรู้
│   └── technical-decisions.md         # เอกสารวิเคราะห์สเปกเวอร์ชันและการออกแบบระบบ
├── labs/                             # คู่มือและไฟล์ปฏิบัติการย่อย
│   ├── 01-foundation/                # สไลด์แนะนำทฤษฎี DE & Airflow
│   ├── 02-rag-architecture/          # เวิร์กชอป Jupyter Notebook RAG (ChromaDB + Gemini)
│   ├── 03-rag-airflow/               # ท่อส่ง RAG อัตโนมัติด้วย Airflow 3 (@task.llm)
│   ├── 04-ai-agent-bonus/            # ระบบ AI Agent และการทำ Human-in-the-loop (HITL)
│   └── 05-mlops-dataops/             # เอกสารสรุปการจัดการท่อส่งข้อมูลในระบบจริง
└── รายละเอียดหลักสูตร KX_ Mastering Workflow .pdf   # เอกสารโครงสร้างหลักสูตรฉบับเต็ม
```

---

## 4. ความต้องการของระบบและการติดตั้ง (System Prerequisites)

*   **Docker & Docker Compose**: จำเป็นต้องติดตั้งเพื่อใช้ในการรันระบบแล็บข้อที่ 2, 3 และ 4
*   **Google AI Studio (Gemini) API Key**: คุณต้องเตรียมคีย์ API สำหรับใช้รันโปรแกรมเชื่อมโมเดลสืบค้นความรู้ โดยให้ตั้งค่าผ่านตัวแปรสภาพแวดล้อมดังนี้:
    ```bash
    export GEMINI_API_KEY="your-gemini-api-key"
    ```
*   **สำหรับ macOS/Linux**: จำเป็นต้องระบุรหัสสิทธิ์ผู้ใช้งานเพื่อให้ Docker สามารถบันทึกประวัติไฟล์ลงบนคอมพิวเตอร์ของคุณได้:
    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > labs/03-rag-airflow/.env
    ```
