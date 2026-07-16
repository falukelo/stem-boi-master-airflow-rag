# Lab 03: Separated Ingestion Pipelines & Query via Airflow 3

ห้องปฏิบัติการนี้มุ่งเน้นการจัดการสถาปัตยกรรมข้อมูลระดับโปรดักชัน โดยเรียนรู้วิธีการแบ่งแยกถังเก็บข้อมูลความรู้ (Vector Storage Buckets) ออกจากกันตามความรับผิดชอบของหน่วยงาน (HR และ IT Support) เพื่อให้ง่ายต่อการขยายระบบและความปลอดภัยข้อมูล โดยแบ่งออกเป็น **3 DAGs** ในระบบ:

1.  **`lab03_hr_ingestion`** (ท่อข้อมูล HR): ตรวจจับไฟล์ `hr_policy.pdf` นำเข้าถังเวกเตอร์ `kx_hr_documents`
2.  **`lab03_it_ingestion`** (ท่อข้อมูล IT): ตรวจจับไฟล์ `it_policy.pdf` นำเข้าถังเวกเตอร์ `kx_it_documents`
3.  **`lab03_rag_query_llm`** (ท่อเรียกสอบถาม): รับคำถามพนักงาน ระบุหมวดหมู่การดึงข้อมูล และเรียกใช้ `@task.llm` สรุปคำตอบ

---

## 1. การแบ่งโมดูลระดับ Task ในระบบนำเข้าข้อมูล
แต่ละ DAG ของการนำเข้าข้อมูล (HR และ IT) จะถูกแยกโมดูลออกเป็น 5 Tasks ย่อยเพื่อรองรับการทำงานแบบวิศวกรรมข้อมูลที่ดี:
*   **`wait_for_document`** (FileSensor): คอยตรวจจับไฟล์ชื่อตามที่ฝ่ายกำหนดในคลัง
*   **`read_pdf_task`**: เปิดสกัดข้อความจาก PDF ดิบด้วย PyMuPDF (`fitz`)
*   **`chunk_text_task`**: แบ่งข้อความเป็นชิ้นย่อยขนาด 250 ตัวอักษร
*   **`embed_text_task`**: แปลงชิ้นข้อความเป็นเวกเตอร์ผ่าน Gemini API (`text-embedding-004`)
*   **`insert_to_vector_db_task`**: นำข้อความและเวกเตอร์บันทึกลงคอลเลกชันเป้าหมายของตนเอง และย้ายไฟล์เก่าออกเพื่อจบกระบวนการรันอย่างเสถียร

---

## 2. ขั้นตอนการเตรียมระบบและการรัน (Setup Instructions)

### ขั้นตอนที่ 1: ตั้งค่า API Key และเปิดสิทธิ์ Docker
```bash
export GEMINI_API_KEY="your-gemini-api-key-here"
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up -d
```
*(เข้าควบคุมหน้าจอระบบจำลองที่ `http://localhost:8080` (Username: `admin` / Password: `admin`))*

### ขั้นตอนที่ 2: ตั้งค่าการเชื่อมต่อในหน้า UI
1. เข้าไปที่ Airflow UI -> เมนู **Admin -> Connections**
2. กดปุ่ม **+** สร้าง Record ใหม่:
   *   **Connection Id**: `gemini_conn`
   *   **Connection Type**: Generic หรือ HTTP
   *   **Password/API Key**: ระบุ `GEMINI_API_KEY` ของคุณ
3. กด **Save**

---

## 3. ขั้นตอนการทดสอบทำ RAG

### ขั้นตอนที่ 3.1: รันท่อนำเข้าข้อมูลฝ่าย HR (HR Ingestion Phase)
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab03_hr_ingestion`
2. คัดลอกไฟล์เอกสารนโยบายในเครื่องของคุณ เช่น `labs/mock_documents/policy_leave.pdf` ไปวางในโฟลเดอร์หลักสูตร `./data/` และเปลี่ยนชื่อไฟล์ปลายทางเป็น `hr_policy.pdf`
   *(คำสั่ง: `cp ../mock_documents/policy_leave.pdf ./data/hr_policy.pdf`)*
3. สังเกตหน้าจอ Airflow: ตัว Sensor จะเริ่มรันผ่าน และนำเข้าข้อมูลเวกเตอร์สู่ถัง `kx_hr_documents` จนเสร็จสิ้น

### ขั้นตอนที่ 3.2: รันท่อนำเข้าข้อมูลฝ่าย IT Support (IT Ingestion Phase)
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab03_it_ingestion`
2. คัดลอกไฟล์เอกสารนโยบายไอทีจำลอง `labs/mock_documents/policy_itsupport.pdf` ไปวางในโฟลเดอร์หลักสูตร `./data/` และเปลี่ยนชื่อไฟล์ปลายทางเป็น `it_policy.pdf`
   *(คำสั่ง: `cp ../mock_documents/policy_itsupport.pdf ./data/it_policy.pdf`)*
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
