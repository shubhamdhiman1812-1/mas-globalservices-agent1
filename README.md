# FSR Extraction and Processing Agent

A robust automation agent for extracting, validating, and routing Field Service Report (FSR) PDFs uploaded to Azure Blob Storage. It enforces idempotency, leverages Azure Document Intelligence for extraction, validates per-field confidence, routes low-confidence cases to human review, and outputs structured JSON to downstream systems.

---

## Quick Start

### 1. Create a virtual environment:
```
python -m venv .venv
```

### 2. Activate the virtual environment:

**Windows:**
```
.venv\Scripts\activate
```

**macOS/Linux:**
```
source .venv/bin/activate
```

### 3. Install dependencies:
```
pip install -r requirements.txt
```

### 4. Environment setup:
Copy `.env.example` to `.env` and fill in all required values.
```
cp .env.example .env
```

### 5. Running the agent

- **Direct execution:**
  ```
  python code/agent.py
  ```
- **As a FastAPI server:**
  ```
  uvicorn code.agent:app --reload --host 0.0.0.0 --port 8000
  ```

---

## Environment Variables

**Agent Identity**
- `AGENT_NAME`
- `AGENT_ID`
- `PROJECT_NAME`
- `PROJECT_ID`

**General**
- `ENVIRONMENT`

**Azure Key Vault (optional for production)**
- `USE_KEY_VAULT`
- `KEY_VAULT_URI`
- `AZURE_USE_DEFAULT_CREDENTIAL`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`

**LLM Configuration**
- `MODEL_PROVIDER`
- `LLM_MODEL`
- `LLM_TEMPERATURE`
- `LLM_MAX_TOKENS`
- `LLM_MODELS` (optional, JSON array for observability)

**API Keys / Secrets**
- `OPENAI_API_KEY`
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_ENDPOINT`
- `ANTHROPIC_API_KEY`
- `GOOGLE_API_KEY`
- `AZURE_CONTENT_SAFETY_KEY`
- `AZURE_STORAGE_KEY`
- `DOCUMENT_INTELLIGENCE_KEY`
- `EVENTHUB_CONNECTION_STRING`

**Service Endpoints**
- `AZURE_CONTENT_SAFETY_ENDPOINT`
- `AZURE_OPENAI_ENDPOINT`
- `AZURE_SEARCH_ENDPOINT`
- `DOCUMENT_INTELLIGENCE_ENDPOINT`
- `EVENTHUB_TOPIC`
- `AZURE_SEARCH_INDEX_NAME`

**Observability (Azure SQL)**
- `OBS_DATABASE_TYPE`
- `OBS_AZURE_SQL_SERVER`
- `OBS_AZURE_SQL_DATABASE`
- `OBS_AZURE_SQL_PORT`
- `OBS_AZURE_SQL_USERNAME`
- `OBS_AZURE_SQL_PASSWORD`
- `OBS_AZURE_SQL_SCHEMA`
- `OBS_AZURE_SQL_TRUST_SERVER_CERTIFICATE`

**Agent-Specific**
- `AZURE_STORAGE_ACCOUNT_NAME`
- `FSR_EXTRACTED_CONTAINER`
- `VALIDATION_CONFIG_PATH`
- `CONTENT_SAFETY_ENABLED`
- `CONTENT_SAFETY_SEVERITY_THRESHOLD`

See `.env.example` for all required and optional variables.

---

## API Endpoints

### **GET** `/health`
- **Description:** Health check endpoint.
- **Response:**
  ```
  {
    "status": "ok"
  }
  ```

---

### **POST** `/process_blob_event`
- **Description:** Process a blob created event for FSR PDF extraction and routing.
- **Request body:**
  ```
  {
    "container_name": "string (required)",
    "blob_name": "string (required)",
    "sas_token": "string (required)",
    "blob_properties": { ... } (optional)
  }
  ```
- **Response:**
  ```
  {
    "success": true|false,
    "result": {
      "idempotency_key": "string",
      "routing": {
        "routed_to": "output"|"fsr-needs-review",
        "action": "proceed_to_output"|"human_review",
        "success": true
      },
      "confidence_result": {
        "all_critical_fields_above_threshold": true|false,
        "critical_fields_below_threshold": [ "field_name", ... ],
        "route_to_review": true|false,
        "min_confidence": float
      }
    },
    "error": null|string,
    "error_code": null|string,
    "fixing_tips": null|string
  }
  ```
  - On error, `success` is `false` and `error_code`/`error`/`fixing_tips` are populated.

---

## Running Tests

### 1. Install test dependencies (if not already installed):
```
pip install pytest pytest-asyncio
```

### 2. Run all tests:
```
pytest tests/
```

### 3. Run a specific test file:
```
pytest tests/test_<module_name>.py
```

### 4. Run tests with verbose output:
```
pytest tests/ -v
```

### 5. Run tests with coverage report:
```
pip install pytest-cov
pytest tests/ --cov=code --cov-report=term-missing
```

---

## Deployment with Docker

### 1. Prerequisites: Ensure Docker is installed and running.

### 2. Environment setup: Copy `.env.example` to `.env` and configure all required environment variables.

### 3. Build the Docker image:
```
docker build -t fsr-extraction-agent -f deploy/Dockerfile .
```

### 4. Run the Docker container:
```
docker run -d --env-file .env -p 8000:8000 --name fsr-extraction-agent fsr-extraction-agent
```

### 5. Verify the container is running:
```
docker ps
```

### 6. View container logs:
```
docker logs fsr-extraction-agent
```

### 7. Stop the container:
```
docker stop fsr-extraction-agent
```

---

## Notes

- All run commands must use the `code/` prefix (e.g., `python code/agent.py`, `uvicorn code.agent:app ...`).
- See `.env.example` for all required and optional environment variables.
- The agent requires access to LLM API keys and (optionally) Azure SQL for observability.
- For production, configure Key Vault and secure credentials as needed.

---

**FSR Extraction and Processing Agent** — Automated, reliable, and secure FSR PDF extraction and routing for Azure-based workflows.
