# Lab 03: Automated RAG Pipeline via Airflow 3

ห้องปฏิบัติการนี้มุ่งเน้นการแปลงโค้ดจาก Jupyter Notebook ในแล็บ 2 มาสร้างเป็น **Automated Data Pipeline (ท่อส่งข้อมูลอัตโนมัติ)** บน **Apache Airflow 3.x** โดยใช้เทคนิค Sensor คอยจับไฟล์ใหม่ และประมวลผล RAG ผ่านตัวช่วย `@task.llm` ร่วมกับฐานข้อมูล ChromaDB แบบถาวร (Persistent Storage)

---

## 1. สิ่งที่คุณจะได้เรียนรู้ (Learning Objectives)
*   การเริ่มต้นใช้งานและทำความรู้จักหน้าจอ UI ของ Apache Airflow 3.x
*   การตั้งค่าการเชื่อมต่อภายนอก (Connections) และการจัดการ API Keys อย่างปลอดภัย
*   การใช้ `FileSensor` ในการตรวจจับไฟล์ข้อมูลเข้าใหม่แบบอัตโนมัติ
*   การเขียน DAG แบบ TaskFlow API และการประยุกต์ใช้งาน `@task.llm`
*   การประยุกต์ใช้ **Claim Check Pattern** ในการส่งผ่านข้อมูลข้อความขนาดใหญ่ใน DAG

---

## 2. ขั้นตอนการเตรียมระบบและการรัน (Setup Instructions)

### ขั้นตอนที่ 1: ตั้งค่า API Key
ตรวจสอบให้แน่ใจว่าได้ระบุคีย์ Google AI Studio ในระบบ:
```bash
export GEMINI_API_KEY="your-gemini-api-key-here"
```

### ขั้นตอนที่ 2: รันระบบ Airflow 3
เริ่มการติดตั้งฐานข้อมูล Postgres และตัวประมวลผล Airflow:
```bash
# กำหนดสิทธิ์ผู้ใช้งานบนระบบ Linux/macOS
echo -e "AIRFLOW_UID=$(id -u)" > .env

# เริ่มระบบคอนเทนเนอร์
docker-compose up -d
```
*ระบบจะเปิดเว็บบราวเซอร์ให้เข้าควบคุมหน้าจอที่ `http://localhost:8080` (Username: `admin` / Password: `admin`)*

### ขั้นตอนที่ 3: ตั้งค่าการเชื่อมต่อในหน้า UI (สำคัญมาก)
เพื่อให้ DAG ทำงานร่วมกับโมเดลภายนอกได้โดยไม่ใส่คีย์ลงในโค้ดดิบ:
1. เข้าสู่ระบบ Airflow UI (localhost:8080)
2. ไปที่เมนู **Admin -> Connections**
3. กดปุ่ม **+ (Add a new record)**
4. กรอกข้อมูลดังนี้:
   *   **Connection Id**: `gemini_conn`
   *   **Connection Type**: เลือกประเภท AI Provider หรือ Custom (ขึ้นอยู่กับ Provider ที่ลง ในที่นี้ตั้งค่าแบบ HTTP หรือ Generic connection แล้วระบุ API Key)
   *   **Password/API Key**: ระบุ `GEMINI_API_KEY` ของคุณ
5. กดปุ่ม **Save**

### ขั้นตอนที่ 4: ทดสอบกระบวนการทำ RAG อัตโนมัติ
1. ไปที่หน้าจอหลัก เปิดสวิตช์เริ่มการทำงาน DAG ชื่อ `automated_rag_pipeline` (ให้เป็นสีฟ้า)
2. สังเกตว่า Task แรก `wait_for_new_document` จะขึ้นสถานะสีเหลืองเข้ม (Sensor กำลังรอไฟล์ใหม่)
3. ให้คุณคัดลอกไฟล์เอกสารนโยบายจำลองที่เตรียมไว้ในโฟลเดอร์ `labs/mock_documents/` (เช่น `policy_leave.pdf` หรือ `policy_medical.pdf`) ไปวางไว้ในโฟลเดอร์ `./data/` โดยเปลี่ยนชื่อไฟล์เป็น `new_policy.pdf`
   *(ตัวอย่างคำสั่ง: `cp ../mock_documents/policy_leave.pdf ./data/new_policy.pdf`)*
4. สังเกตบนหน้าจอ Airflow UI: Sensor จะตรวจพบไฟล์โดยอัตโนมัติ, ระบบจะดึงข้อความจากไฟล์ PDF มาแบ่งหน้า (Chunk), แปลงเวกเตอร์ลง ChromaDB และสรุปผลผ่าน Gemini (`summarize_new_data`) จนท่อส่งแสดงผลสำเร็จเป็นสีเขียว
5. เข้าไปดูข้อความสรุปของเอกสารที่คุณส่งไปได้ที่เมนู **Logs** หรือ XCom ของ Task `summarize_new_data`

