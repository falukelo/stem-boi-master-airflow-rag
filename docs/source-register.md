# Source Register: Airflow 3 & RAG Orchestration

เอกสารนี้รวบรวมแหล่งอ้างอิงอย่างเป็นทางการ (Official Sources) และเอกสารประกอบสำหรับเทคโนโลยีที่ใช้งานในหลักสูตร เพื่อใช้อ้างอิงและตรวจสอบความถูกต้องเชิงเทคนิค

---

## 1. Apache Airflow 3 & Common AI Providers

*   **Apache Airflow 3.0 Documentation**
    *   **URL**: https://airflow.apache.org/docs/apache-airflow/stable/index.html
    *   **Version**: 3.0.0+ (and upcoming releases)
    *   **Access Date**: 2026-07-16
    *   **Supported Claim**: การใช้งาน TaskFlow API, Decorators ใหม่ๆ และโครงสร้างของ Airflow 3.0 Core
*   **Apache Airflow Providers: Common AI**
    *   **URL**: https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/index.html
    *   **Version**: 1.0.0+ (Released April 2026)
    *   **Access Date**: 2026-07-16
    *   **Supported Claim**: การใช้ `@task.llm` สำหรับเรียก LLM แบบเดี่ยว และ `@task.agent` ร่วมกับ toolsets สำหรับทำ Agentic workflows
*   **Airflow Human-in-the-Loop (HITL) Functionality**
    *   **URL**: https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/hitl.html
    *   **Version**: Airflow 3.1+ / Standard Provider
    *   **Access Date**: 2026-07-16
    *   **Supported Claim**: การใช้ `HITLOperator` หรือ `ApprovalOperator` ในการสั่ง Pause workflow และรออนุมัติผ่าน UI/API

---

## 2. Google Gemini & Google GenAI SDK

*   **Google AI Studio & Gemini API Documentation**
    *   **URL**: https://ai.google.dev/gemini-api/docs
    *   **Version**: Gemini 1.5 Pro, Gemini 1.5 Flash, Gemini 2.5
    *   **Access Date**: 2026-07-16
    *   **Supported Claim**: การเรียกใช้โมเดล Gemini และ embedding model (e.g. `text-embedding-004`)
*   **Google Generative AI Python SDK**
    *   **URL**: https://github.com/google/generative-ai-python
    *   **Version**: 0.5.4+ / 0.8.x
    *   **Access Date**: 2026-07-16
    *   **Supported Claim**: ไวยากรณ์การเชื่อมต่อโมเดลผ่าน Python (`import google.generativeai as genai`)

---

## 3. Vector Database (ChromaDB)

*   **ChromaDB Documentation**
    *   **URL**: https://docs.trychroma.com/
    *   **Version**: 0.5.x
    *   **Access Date**: 2026-07-16
    *   **Supported Claim**: การทำ Vector Storage แบบ Local/In-memory ใน Jupyter notebook และการเชื่อมต่อผ่าน Python client
