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

### ขั้นตอนที่ 1: ตั้งค่า API Key ด้วยไฟล์ `.env`
เพื่อความสะดวกในการจัดการคีย์และความปลอดภัย เราแนะนำให้ใช้ไฟล์คอนฟิกูเรชัน `.env`

1. ทำการคัดลอกไฟล์ต้นแบบ `.env.example` ไปเป็นไฟล์จริง `.env` ในโฟลเดอร์นี้:
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
2. เปิดไฟล์ `.env` และกรอกคีย์ของโครงการของคุณ:
   ```text
   GEMINI_API_KEY="your-gemini-api-key-here"
   ```

*(หมายเหตุ: หากไม่สะดวกใช้ไฟล์ `.env` สามารถประกาศตัวแปรสภาพแวดล้อมลงในเครื่องตรงๆ ได้ ดังนี้)*
*   **macOS / Linux**: `export GEMINI_API_KEY="your-key"`
*   **Windows (CMD)**: `set GEMINI_API_KEY=your-key`
*   **Windows (PowerShell)**: `$env:GEMINI_API_KEY="your-key"`

---

### ขั้นตอนที่ 2: เริ่มต้นระบบ Jupyter
รันคำสั่ง Docker-Compose ในโฟลเดอร์นี้เพื่อเริ่มระบบ Jupyter Notebook:
*   **ทุกระบบปฏิบัติการ (macOS, Linux, Windows)**:
    ```bash
    docker-compose up -d
    ```
*ระบบจะเปิดสิทธิ์เว็บเซิร์ฟเวอร์บนพอร์ต `8888` โดยไม่มีการถามรหัสผ่าน (Token-free เพื่อความสะดวกสำหรับผู้เรียน)*

---

### ขั้นตอนที่ 3: เปิดใช้งาน
1. เปิดเว็บบราวเซอร์แล้วไปที่ `http://localhost:8888`
2. เข้าโฟลเดอร์ `work` และไปที่ `notebooks/rag_intro.ipynb`
3. กดรันเนื้อหาโค้ดทีละบล็อกเพื่อเรียนรู้กระบวนการทำงาน

---

## 3. รายละเอียดไฟล์ในโฟลเดอร์
*   `docker-compose.yaml`: ไฟล์เปิดระบบ Jupyter และดาวน์โหลดไลบรารีเบื้องต้น
*   `.env.example`: ตัวอย่างโครงสร้างไฟล์ตัวแปรสภาพแวดล้อม
*   `notebooks/rag_intro.ipynb`: สมุดแบบฝึกหัดทฤษฎีและปฏิบัติ RAG
*   `documents/policy_wfh.pdf`: ไฟล์นโยบายบริษัทสำหรับการสกัดข้อมูลในแล็บ
