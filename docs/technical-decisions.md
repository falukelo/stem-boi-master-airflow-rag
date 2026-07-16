# Technical Decisions: Airflow 3 & RAG Orchestration

เอกสารนี้ระบุการตัดสินใจเชิงเทคนิค เวอร์ชันของระบบ และรูปแบบการเขียนโค้ด (Patterns) ที่ใช้ในหลักสูตรเพื่อความเสถียรและความสอดคล้องระหว่างเนื้อหาทฤษฎีและข้อปฏิบัติ (Labs)

---

## 1. Pinned Versions (ข้อกำหนดเวอร์ชัน)

เพื่อให้มั่นใจว่าสภาพแวดล้อมการทำงานของนักเรียนและอาจารย์สามารถสร้างใหม่ได้ 100% (Reproducible) จึงกำหนดเวอร์ชันดังนี้:

*   **Python**: `3.11` (เสถียรที่สุดสำหรับทั้ง Airflow 3 และแพ็คเกจ AI/ML ทั่วไป)
*   **Apache Airflow**: `3.3.0` (เวอร์ชันล่าสุดที่รองรับฟีเจอร์ Human-in-the-Loop แบบ scheduler-managed `awaiting_input` state)
*   **apache-airflow-providers-common-ai**: `1.0.0` (มี TaskFlow decorators `@task.llm` และ `@task.agent` ที่รองรับ Pydantic AI)
*   **apache-airflow-providers-standard**: `1.0.0` (มี `HITLOperator` สำหรับขั้นตอนการยืนยันโดยมนุษย์)
*   **google-generativeai**: `0.5.4` (ตรงตามเวอร์ชันในระบบปัจจุบัน)
*   **ChromaDB**: `0.5.3` (ใช้ทำ Vector Database แบบฝังในแอปพลิเคชันหรือเซิร์ฟเวอร์แบบเบา)
*   **Jupyter Notebook**: `jupyter/base-notebook:python-3.11.9` (Base Docker Image)

---

## 2. DAG Design Patterns (รูปแบบการออกแบบ DAG)

การพัฒนา DAGs ในหลักสูตรนี้ต้องปฏิบัติตามมาตรฐาน Data Engineering ที่ดี:

### A. Idempotency (การทำงานซ้ำได้ผลลัพธ์เท่าเดิม)
*   **RAG Ingestion**: เมื่อมีการประมวลผลไฟล์ซ้ำ หากเอกสารเดิมถูกจัดเก็บไปแล้วใน Vector Database ให้ทำลาย (Delete/Overwrite) ข้อมูลของเอกสารชุดนั้นก่อนเขียนใหม่ เพื่อป้องกันไม่ให้เกิด Vector ซ้ำซ้อน ซึ่งนำไปสู่การตอบข้อมูลซ้ำ (Duplicated context retrieves)
*   **การใช้ `logical_date`**: ใน Airflow 3 จะหลีกเลี่ยงการใช้ `execution_date` และใช้ `logical_date` เพื่อการทำงานแบบย้อนหลัง (Backfill) ที่ถูกต้อง

### B. Error Handling & Retries (การจัดการข้อผิดพลาด)
*   **LLM API Calls Rate Limits**: เนื่องจากการเรียกใช้ Gemini API จาก AI Studio อาจติดขัดด้วยข้อจำกัด API limits จึงต้องตั้งค่า `retries=3` และ `retry_delay=timedelta(seconds=10)` โดยตั้งค่าการ Retry แบบ Exponential Backoff สำหรับ Task ที่คุยกับภายนอก

### C. XComs & Data Flow (การส่งผ่านข้อมูล)
*   **TaskFlow API**: ใช้การ return ค่าแบบปกติในฟังก์ชันตกแต่งด้วย `@task` เพื่อให้ Airflow จัดการ XCom โดยอัตโนมัติ
*   **Limiting Size**: เนื่องจากข้อมูล Text ใน RAG มีขนาดใหญ่ จะไม่ส่งข้อมูล Chunk ทั้งหมดผ่าน XCom แต่จะใช้รูปแบบ **Claim Check Pattern** (ส่งเพียง ID หรือ Path ไปยัง File Storage/Vector Store และให้ Task ถัดไปอ่านข้อมูลตรงจากแหล่งเก็บหลัก)

### D. Connection-Based Configuration (การตั้งค่าความปลอดภัย)
*   หลีกเลี่ยงการ Hardcode `API_KEY` ลงในโค้ด DAG
*   กำหนดค่า Gemini SDK ใน Airflow 3 ผ่านระบบ **Airflow Connections** (ใช้ `llm_conn_id="gemini_conn"` ซึ่งดึงข้อมูลมาผ่าน Provider Standard Connection)

---

## 3. RAG vs Long-Context LLMs Context Analysis

*   **เหตุผลที่ยังต้องมี RAG**: แม้ Gemini 1.5 Pro และโมเดลใหม่ๆ จะมี Context Window สูงถึง 1-2 ล้าน Tokens แต่ในสเกลระดับ Enterprise หรือระบบที่ต้องการ Production Ready ยังคงต้องการ RAG ด้วยเหตุผล:
    1.  **Cost Efficiency**: การยัดข้อมูลทุกอย่างใน Prompt ทุกครั้งทำให้เกิดค่าใช้จ่ายสูงมาก
    2.  **Latency**: ยิ่ง Context ยาวขึ้น Latency ในการประมวลผล (First Token Time) จะยิ่งเพิ่มขึ้น
    3.  **Traceability**: RAG สามารถชี้แหล่งอ้างอิงชัดเจนว่านำคำตอบมาจากเอกสารใดใน Vector DB ป้องกันการคิดไปเอง (Hallucination)
