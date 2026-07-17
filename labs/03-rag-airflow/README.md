# Lab 03: Separated Ingestion Pipelines & Query via Airflow 3

ห้องปฏิบัติการนี้มุ่งเน้นการจัดการสถาปัตยกรรมข้อมูลระดับโปรดักชัน โดยเรียนรู้วิธีการแบ่งแยกถังเก็บข้อมูลความรู้ (Vector Storage Buckets) ออกจากกันตามความรับผิดชอบของหน่วยงาน (HR และ IT Support) เพื่อให้ง่ายต่อการขยายระบบและความปลอดภัยข้อมูล โดยแบ่งออกเป็น **3 DAGs** ในระบบ:

1.  **`lab03_hr_ingestion`** (ท่อข้อมูล HR): ตรวจจับไฟล์ PDF ใด ๆ ในโฟลเดอร์ `data/hr/` นำเข้าถังเวกเตอร์ `kx_hr_documents`
2.  **`lab03_it_ingestion`** (ท่อข้อมูล IT): ตรวจจับไฟล์ PDF ใด ๆ ในโฟลเดอร์ `data/it/` นำเข้าถังเวกเตอร์ `kx_it_documents`
3.  **`lab03_rag_query_llm`** (ท่อเรียกสอบถาม): รับคำถามพนักงาน ระบุหมวดหมู่การดึงข้อมูล และเรียกใช้ `@task.llm` สรุปคำตอบ

---

## 1. การแบ่งโมดูลระดับ Task ในระบบนำเข้าข้อมูล
แต่ละ DAG ของการนำเข้าข้อมูล (HR และ IT) จะถูกแยกโมดูลออกเป็น 5 Tasks ย่อยเพื่อรองรับการทำงานแบบวิศวกรรมข้อมูลที่ดี:
*   **`wait_for_document`** (FileSensor): คอยตรวจจับไฟล์ `.pdf` ใด ๆ ที่วางในโฟลเดอร์ของฝ่าย (`data/hr/` หรือ `data/it/`)
*   **`read_pdf_task`**: เปิดสกัดข้อความจาก PDF ดิบด้วย PyMuPDF (`fitz`)
*   **`chunk_text_task`**: แบ่งข้อความเป็นชิ้นย่อยขนาด 250 ตัวอักษร
*   **`embed_text_task`**: แปลงชิ้นข้อความเป็นเวกเตอร์ผ่าน Gemini API (`text-embedding-004`)
*   **`insert_to_vector_db_task`**: นำข้อความและเวกเตอร์บันทึกลงคอลเลกชันเป้าหมายของตนเอง และย้ายไฟล์เก่าออกเพื่อจบกระบวนการรันอย่างเสถียร

---

## 2. ขั้นตอนการเตรียมระบบและการรัน (Setup Instructions)

### ขั้นตอนที่ 1: การตั้งค่าสิทธิ์ไฟล์และไฟล์ `.env`
เพื่อป้องกันความปลอดภัยและการอนุญาตเข้าถึงสิทธิ์เขียนไฟล์ใน Docker Volume เราจำเป็นต้องตั้งค่าไฟล์ `.env` ก่อนการทำงาน:

1. คัดลอกไฟล์ต้นแบบ `.env.example` ไปเป็น `.env` ในโฟลเดอร์นี้:
   *   **macOS / Linux (Terminal)**:
       ```bash
       cp .env.example .env
       ```
   *   **Windows (Command Prompt / CMD)**:
       ```cmd
       copy .env.example .env
       ```
   *   **Windows (PowerShell)**:
       ```powershell
       Copy-Item .env.example .env
       ```
2. แก้ไขไฟล์ `.env` โดยระบุค่าต่าง ๆ ดังนี้:
   *   **ระบุ GEMINI_API_KEY**: กรอกคีย์ของโครงการคุณ
   *   **ระบุ AIRFLOW_UID (สำคัญมากสำหรับ macOS / Linux)**: สั่งคำนวณสิทธิ์ UID ของคุณลงในไฟล์ `.env` เพื่อไม่ให้ Docker ล็อกสิทธิ์แก้ไขไฟล์บนเครื่องหลัก:
       *   **macOS / Linux (Terminal)**:
           ```bash
           echo "AIRFLOW_UID=$(id -u)" >> .env
           ```
       *   **Windows (PowerShell/CMD)**: สำหรับระบบปฏิบัติการ Windows ไม่ต้องเขียนระบุค่า `AIRFLOW_UID` (ให้ข้ามขั้นตอนนี้ไปได้เลย เนื่องจาก Windows Docker Desktop จะแชร์สิทธิ์เข้าเครื่องโฮสต์ผ่าน WSL2 หรือ Hyper-V เป็น root เสมอ)
3. ค่าตัวอย่างในไฟล์ `.env` ที่ถูกต้อง:
   *   **สำหรับ macOS / Linux**:
       ```text
       GEMINI_API_KEY="your-gemini-key"
       AIRFLOW_UID=1000
       ```
   *   **สำหรับ Windows**:
       ```text
       GEMINI_API_KEY="your-gemini-key"
       ```

---

### ขั้นตอนที่ 2: เริ่มต้นการทำงานระบบ Airflow 3
รันคำสั่ง Docker-Compose เพื่อสร้างและเปิดการทำงานของ Service:
*   **ทุกระบบปฏิบัติการ (macOS, Linux, Windows)**:
    ```bash
    docker-compose up -d
    ```
*(เข้าควบคุมหน้าจอระบบจำลองที่ `http://localhost:8080` (Username: `admin` / Password: `admin`))*

---

### ขั้นตอนที่ 3: ตั้งค่าการเชื่อมต่อในหน้า UI
1. เข้าไปที่ Airflow UI -> เมนู **Admin -> Connections**
2. กดปุ่ม **+** สร้าง Record ใหม่:
   *   **Connection Id**: `gemini_conn`
   *   **Connection Type**: `Pydantic AI` (ต้องเลือกตัวนี้เท่านั้น ตัว `@task.llm` / `@task.agent` / `@task.llm_branch` ถึงจะทำงานได้ และช่อง **Model** จะปรากฏขึ้นหลังเลือก Type นี้)
   *   **Model**: `google:gemini-3.1-flash-lite` (รูปแบบ `provider:model`)
   *   **Password**: ระบุ `GEMINI_API_KEY` ของคุณ
3. กด **Save**

> หมายเหตุ: Connection Type `Pydantic AI` มาจาก provider `apache-airflow-providers-common-ai` (ติดตั้งไว้แล้วผ่าน `_PIP_ADDITIONAL_REQUIREMENTS` ใน `docker-compose.yaml`) โมเดล LLM ระบุที่ช่อง Model ของ connection ไม่ใช่ในโค้ด DAG ส่วนโมเดล embedding (`gemini-embedding-2`) ถูกเรียกผ่าน SDK โดยตรงด้วย `GEMINI_API_KEY` แยกจาก connection นี้

---

## 3. ขั้นตอนการทดสอบทำ RAG

### ขั้นตอนที่ 3.1: รันท่อนำเข้าข้อมูลฝ่าย HR (HR Ingestion Phase)
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab03_hr_ingestion`
2. คัดลอกไฟล์เอกสารนโยบาย HR เช่น `labs/mock_documents/policy_leave.pdf` ไปวางในโฟลเดอร์ `./data/hr/` (ใช้ชื่อไฟล์เดิมได้เลย ไม่ต้องเปลี่ยนชื่อ — ตัว Sensor จะดักจับไฟล์ `.pdf` ใด ๆ ที่วางในโฟลเดอร์นี้)
   *   **macOS / Linux**: `cp ../mock_documents/policy_leave.pdf ./data/hr/`
   *   **Windows (CMD)**: `copy ..\mock_documents\policy_leave.pdf .\data\hr\`
   *   **Windows (PowerShell)**: `Copy-Item ..\mock_documents\policy_leave.pdf .\data\hr\`
3. สังเกตหน้าจอ Airflow: ตัว Sensor จะเริ่มรันผ่าน และนำเข้าข้อมูลเวกเตอร์สู่ถัง `kx_hr_documents` จนเสร็จสิ้น

### ขั้นตอนที่ 3.2: รันท่อนำเข้าข้อมูลฝ่าย IT Support (IT Ingestion Phase)
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab03_it_ingestion`
2. คัดลอกไฟล์เอกสารนโยบายไอทีจำลอง `labs/mock_documents/policy_itsupport.pdf` ไปวางในโฟลเดอร์ `./data/it/` (ใช้ชื่อไฟล์เดิมได้เลย ไม่ต้องเปลี่ยนชื่อ — ตัว Sensor จะดักจับไฟล์ `.pdf` ใด ๆ ที่วางในโฟลเดอร์นี้)
   *   **macOS / Linux**: `cp ../mock_documents/policy_itsupport.pdf ./data/it/`
   *   **Windows (CMD)**: `copy ..\mock_documents\policy_itsupport.pdf .\data\it\`
   *   **Windows (PowerShell)**: `Copy-Item ..\mock_documents\policy_itsupport.pdf .\data\it\`
3. สังเกตตัว Sensor จะตรวจพบไฟล์และประมวลผลนำเข้าเวกเตอร์สู่ถัง `kx_it_documents` จนแสดงผลสำเร็จเป็นสีเขียว

### ขั้นตอนที่ 3.3: ทดสอบสอบถาม RAG ค้นหาข้อมูลรายถัง (Query Phase)
1. ค้นหา DAG ชื่อ `lab03_rag_query_llm` บนหน้าหลัก
2. กดปุ่มลูกศรข้างขวาของปุ่มรัน เลือก **Trigger w/ config**
3. ป้อนคำถามจำลองและระบุถังที่ต้องการค้นหาในรูปแบบ JSON เช่น:
   ```json
   {
     "query": "รหัสผ่านไอทีต้องตั้งอย่างน้อยกี่ตัวอักษร?",
     "domain": "it"
   }
   ```
4. กดปุ่ม **Trigger** และเข้าไปดูผลลัพธ์การทำงานสรุปจาก Gemini ในเมนู XCom ของ Task `generate_response_with_llm`
5. ทดลองเปลี่ยนเป็นถามเรื่องสวัสดิการ HR โดยสลับตัวแปร `"domain": "hr"` เพื่อตรวจผลลัพธ์การคิวรีที่แยกต่างฝ่ายออกจากกันอย่างถูกต้อง
