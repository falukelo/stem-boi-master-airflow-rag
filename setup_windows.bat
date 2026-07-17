@echo off
echo =======================================================
echo Airflow 3 + RAG Course Setup (Windows)
echo =======================================================

echo 1. Creating necessary directories...
if not exist "labs\03-rag-airflow\dags" mkdir labs\03-rag-airflow\dags
if not exist "labs\03-rag-airflow\logs" mkdir labs\03-rag-airflow\logs
if not exist "labs\03-rag-airflow\plugins" mkdir labs\03-rag-airflow\plugins
if not exist "labs\03-rag-airflow\data" mkdir labs\03-rag-airflow\data

echo 2. Setting up environment variables...
if not exist "labs\03-rag-airflow\.env" (
    copy labs\03-rag-airflow\.env.example labs\03-rag-airflow\.env
    echo Created .env file. Please edit labs\03-rag-airflow\.env and add your GEMINI_API_KEY.
) else (
    echo .env file already exists.
)

echo.
echo Setup completed successfully! 
echo You can now run: cd labs\03-rag-airflow ^&^& docker compose up -d
echo =======================================================
pause
