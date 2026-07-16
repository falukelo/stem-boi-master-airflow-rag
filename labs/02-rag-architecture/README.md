# Lab 02: Retrieval-Augmented Generation (RAG) Architecture

ยินดีต้อนรับสู่ห้องปฏิบัติการที่ 2 ในแล็บนี้คุณจะได้ศึกษาทฤษฎีและทดลองสร้างท่อส่งข้อมูล **RAG** แบบทีละขั้นตอน (Step-by-step) ด้วยภาษา Python บน Jupyter Notebook โดยใช้โมเดลวิเคราะห์ความหมายเวกเตอร์ (Embeddings) และการสรุปผลจาก Gemini API (Google AI Studio) ร่วมกับฐานข้อมูล ChromaDB

---

## 1. สิ่งที่คุณจะได้เรียนรู้ (Learning Objectives)
*   เข้าใจโครงสร้างพื้นฐานของสถาปัตยกรรม RAG
*   การสกัดเอกสารและการทำ **Chunking** ด้วยวิธี Recursive Character Splitter
*   การแปลงข้อความเป็นเวกเตอร์ (**Embedding**) ผ่าน Gemini API (`models/text-embedding-004`)
*   การใช้งาน **Vector Database (ChromaDB)** เพื่อบันทึก ค้นหา และดึงข้อมูล (Retrieval)
*   การสร้าง Prompt (Context-augmented prompt) และการเรียกตอบคำถามด้วยโมเดล `gemini-1.5-flash`

---

## 2. วิธีการตั้งค่าและรันแล็บ (Setup Instructions)

### ขั้นตอนที่ 1: ตั้งค่า API Key
ก่อนที่จะรัน Container ให้แน่ใจว่าได้ระบุ `GEMINI_API_KEY` ในเครื่องคอมพิวเตอร์ของคุณแล้ว:
```bash
export GEMINI_API_KEY="your-gemini-api-key-here"
```

### ขั้นตอนที่ 2: เริ่มต้นระบบ Jupyter
รันคำสั่ง Docker-Compose ในโฟลเดอร์นี้เพื่อเริ่มระบบ Jupyter Notebook:
```bash
docker-compose up -d
```
*ระบบจะเปิดสิทธิ์เว็บเซิร์ฟเวอร์บนพอร์ต `8888` โดยไม่มีการถามรหัสผ่าน (Token-free เพื่อความสะดวกสำหรับผู้เรียน)*

### ขั้นตอนที่ 3: เปิดใช้งาน
1. เปิดเว็บบราวเซอร์แล้วไปที่ `http://localhost:8888`
2. เข้าโฟลเดอร์ `work` และไปที่ `notebooks/rag_intro.ipynb`
3. กดรันเนื้อหาโค้ดทีละบล็อกเพื่อเรียนรู้กระบวนการทำงาน

---

## 3. รายละเอียดไฟล์ในโฟลเดอร์
*   `docker-compose.yaml`: ไฟล์เปิดระบบ Jupyter และดาวน์โหลดไลบรารีเบื้องต้น
*   `notebooks/rag_intro.ipynb`: สมุดแบบฝึกหัดทฤษฎีและปฏิบัติ RAG
