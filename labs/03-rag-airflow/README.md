# Lab 03: Modular RAG Ingestion & Query via Airflow 3

ห้องปฏิบัติการนี้สอนการออกแบบระบบท่อส่งข้อมูล RAG แบบแยกโมดูลการทำงานและขั้นตอนการเรียกสืบค้นข้อมูลจริงในระดับโปรดักชัน โดยแบ่งออกเป็น **2 DAGs** หลัก:

1.  **`lab03_rag_ingestion`** (ท่อส่งข้อมูลนำเข้า): คอยตรวจจับไฟล์เอกสาร PDF นำมาสกัดเนื้อหา แบ่งคำ ทำเวกเตอร์ และจัดเก็บลงฐานข้อมูลเวกเตอร์แบบ Idempotent
2.  **`lab03_rag_query_llm`** (ท่อเรียกสืบค้นและตอบกลับ): รับข้อความคำถาม ค้นหาข้อมูลอ้างอิง และใช้ `@task.llm` จากโมเดล Gemini ตอบข้อมูลกลับพนักงาน

---

## 1. การแบ่งโมดูลระดับ Task ในท่อส่งข้อมูลนำเข้า (`lab03_rag_ingestion`)
ในการปฏิบัติการจริง เราหลีกเลี่ยงการเขียนโค้ดทั้งหมดกระจุกอยู่ในฟังก์ชันเดียว โดยแยก Task ออกเป็น:
*   **`wait_for_new_document`** (FileSensor): คอยตรวจจับไฟล์ `new_policy.pdf`
*   **`read_pdf_task`**: เปิดสกัดเนื้อหาจาก PDF ออกมาเป็นตัวอักษรดิบด้วย PyMuPDF (`fitz`)
*   **`chunk_text_task`**: แบ่งข้อความเป็นชิ้นเล็กๆ ขนาด 250 ตัวอักษรพร้อม overlap 50 ตัวอักษร
*   **`embed_text_task`**: เรียกแปลงคำศัพท์เป็นเวกเตอร์ 768 มิติด้วยโมเดล `text-embedding-004` ผ่าน Gemini API
*   **`insert_to_vector_db_task`**: นำข้อมูลและเวกเตอร์บันทึกลง ChromaDB และเคลื่อนย้ายเอกสารไปโฟลเดอร์อื่นเพื่อจบกระบวนการรันอย่างมีระเบียบ

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

### ขั้นตอนที่ 3.1: รันท่อข้อมูลนำเข้า (Ingestion Phase)
1. เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `lab03_rag_ingestion`
2. คัดลอกไฟล์เอกสารนโยบายในเครื่องของคุณ เช่น `labs/mock_documents/policy_leave.pdf` ไปวางในโฟลเดอร์หลักสูตร `./data/` และเปลี่ยนชื่อไฟล์ปลายทางเป็น `new_policy.pdf`
   *(คำสั่ง: `cp ../mock_documents/policy_leave.pdf ./data/new_policy.pdf`)*
3. สังเกตหน้าจอ Airflow: ตัว Sensor จะเริ่มรันผ่าน และ Task ต่างๆ (Read -> Chunk -> Embed -> Insert) จะทยอยเปลี่ยนเป็นสีเขียวจนเสร็จสิ้น

### ขั้นตอนที่ 3.2: สอบถามข้อมูลผ่านระบบ RAG (Query Phase)
1. ค้นหา DAG ชื่อ `lab03_rag_query_llm` บนหน้าหลัก
2. กดปุ่มลูกศรข้างขวาของปุ่มรัน เลือก **Trigger w/ config**
3. ป้อนคำถามจำลองที่สอดคล้องกับเอกสารที่คุณนำเข้าเมื่อครู่ในรูปแบบ JSON เช่น:
   ```json
   {
     "query": "ฉันทำงานครบกี่วันถึงจะได้สิทธิ์วันลาพักร้อนประจำปี และได้กี่วันทำการ?"
   }
   ```
4. กดปุ่ม **Trigger**
5. ตรวจสอบคำตอบสรุปของโมเดล Gemini ได้ที่ XCom หรือ Logs ของ Task `generate_response_with_llm`
