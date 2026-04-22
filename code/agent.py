import asyncio as _asyncio

import time as _time
from observability.observability_wrapper import (
    trace_agent, trace_step, trace_step_sync, trace_model_call, trace_tool_call,
)
from config import settings as _obs_settings

import logging as _obs_startup_log
from contextlib import asynccontextmanager
from observability.instrumentation import initialize_tracer

_obs_startup_logger = _obs_startup_log.getLogger(__name__)

from modules.guardrails.content_safety_decorator import with_content_safety

GUARDRAILS_CONFIG = {
    'content_safety_enabled': True,
    'runtime_enabled': True,
    'content_safety_severity_threshold': 3,
    'check_toxicity': True,
    'check_jailbreak': True,
    'check_pii_input': False,
    'check_credentials_output': True,
    'check_output': True,
    'check_toxic_code_output': True,
    'sanitize_pii': False
}

import logging
import hashlib
import json
from typing import Any, Dict, Optional, IO
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, ValidationError, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from config import Config

# Azure SDKs
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.eventhub import EventHubProducerClient, EventData

# ========== CONSTANTS ==========
SYSTEM_PROMPT = (
    "You are an expert automation agent responsible for extracting, validating, and routing Field Service Report (FSR) PDFs uploaded to Azure Blob Storage. "
    "Your tasks include: - Computing a SHA-256 content hash of each PDF to enforce idempotency. - Downloading the PDF using a scoped SAS token. - Invoking Azure Document Intelligence (prebuilt-layout) to extract key-value pairs, tables, and paragraphs, capturing per-field confidence scores. "
    "- Canonicalizing key-value pairs by (page, bounding box) to remove ordering nondeterminism and deduplicate overlaps. - Detecting handwritten annotations and extracting severity flags from document margins. - Building a schema v2.1 payload with per-field confidence. "
    "- If any critical field has confidence below 0.60, route the FSR to the human-in-loop queue (fsr-needs-review) instead of proceeding. - Output the extracted_fsr.json to the fsr-extracted container and notify Agent 2 via the event bus. "
    "Always ensure data integrity, enforce business rules, and provide clear error messages. If information is missing or extraction fails, route the document for human review and log the issue."
)
OUTPUT_FORMAT = (
    "All outputs must be in structured JSON format, matching schema v2.1, and include per-field confidence scores. "
    "Error responses must be clearly indicated with error codes and descriptions."
)
FALLBACK_RESPONSE = (
    "Extraction could not be completed automatically. The document has been routed for human review. Please check the fsr-needs-review queue for further action."
)
VALIDATION_CONFIG_PATH = Config.VALIDATION_CONFIG_PATH or str(Path(__file__).parent / "validation_config.json")

# ========== LOGGING ==========
logger = logging.getLogger("agent")
logger.setLevel(logging.INFO)

# ========== INPUT MODELS ==========
class BlobEvent(BaseModel):
    container_name: str = Field(..., description="Name of the Azure Blob Storage container")
    blob_name: str = Field(..., description="Name of the blob (PDF file)")
    sas_token: str = Field(..., description="SAS token for secure blob access")
    blob_properties: Optional[dict] = Field(None, description="Blob properties (size, type, etc.)")

    @field_validator("container_name", "blob_name", "sas_token")
    @classmethod
    def not_empty(cls, v):
        if not v or not str(v).strip():
            raise ValueError("Field must not be empty")
        return v

class QueryResponse(BaseModel):
    success: bool
    result: Optional[dict] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    fixing_tips: Optional[str] = None

# ========== ERROR CODES ==========
ERR_INVALID_FILE_TYPE = "ERR_INVALID_FILE_TYPE"
ERR_EXTRACTION_FAILURE = "ERR_EXTRACTION_FAILURE"
ERR_LOW_CONFIDENCE = "ERR_LOW_CONFIDENCE"
ERR_BLOB_DOWNLOAD = "ERR_BLOB_DOWNLOAD"
ERR_SCHEMA_BUILD = "ERR_SCHEMA_BUILD"

# ========== SERVICE BASE ==========
class BaseService:
    def __init__(self):
        self.logger = logger

    def log_event(self, event_type: str, details: Any):
        self.logger.info(f"[{event_type}] {details}")

    def log_error(self, error_code: str, details: Any):
        self.logger.error(f"[{error_code}] {details}")

# ========== SUPPORTING SERVICES ==========

class FSRIngestionHandler(BaseService):
    def validate_file(self, blob_properties: dict) -> bool:
        """Checks file type and size constraints."""
        try:
            content_type = blob_properties.get("content_type", "")
            size = blob_properties.get("size", 0)
            if not content_type.lower().endswith("pdf"):
                raise ValueError("File is not a PDF")
            if size > 50 * 1024 * 1024:  # 50MB limit
                raise ValueError("File exceeds maximum allowed size (50MB)")
            return True
        except Exception as e:
            self.log_error(ERR_INVALID_FILE_TYPE, f"File validation failed: {e}")
            raise

    def compute_idempotency_key(self, file_stream: IO[bytes]) -> str:
        """Computes SHA-256 hash of PDF content for idempotency."""
        try:
            sha256 = hashlib.sha256()
            file_stream.seek(0)
            while True:
                chunk = file_stream.read(8192)
                if not chunk:
                    break
                sha256.update(chunk)
            file_stream.seek(0)
            return sha256.hexdigest().lower()
        except Exception as e:
            self.log_error(ERR_INVALID_FILE_TYPE, f"SHA-256 computation failed: {e}")
            raise

class DocumentDownloader(BaseService):
    def download_blob(self, container_name: str, blob_name: str, sas_token: str) -> IO[bytes]:
        """Downloads PDF from Azure Blob Storage."""
        blob_url = f"https://{Config.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        try:
            _t0 = _time.time()
            blob_client = BlobClient.from_blob_url(blob_url)
            stream = blob_client.download_blob()
            file_stream = stream.readall()
            try:
                trace_tool_call(
                    tool_name="BlobClient.download_blob",
                    latency_ms=int((_time.time() - _t0) * 1000),
                    args={"container_name": container_name, "blob_name": blob_name},
                    output=f"Downloaded {len(file_stream)} bytes" if file_stream else None,
                    status="success",
                )
            except Exception:
                pass
            from io import BytesIO
            return BytesIO(file_stream)
        except AzureError as e:
            self.log_error(ERR_BLOB_DOWNLOAD, f"Blob download failed: {e}")
            try:
                trace_tool_call(
                    tool_name="BlobClient.download_blob",
                    latency_ms=0,
                    args={"container_name": container_name, "blob_name": blob_name},
                    output=None,
                    status="error",
                    error=e,
                )
            except Exception:
                pass
            raise

class ExtractionService(BaseService):
    def __init__(self):
        super().__init__()
        self.endpoint = Config.DOCUMENT_INTELLIGENCE_ENDPOINT
        self.key = Config.DOCUMENT_INTELLIGENCE_KEY
        if not self.endpoint or not self.key:
            raise ValueError("Azure Document Intelligence endpoint/key not configured")
        self.client = DocumentAnalysisClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.key)
        )

    @with_content_safety(config=GUARDRAILS_CONFIG)
    def extract_data(self, file_stream: IO[bytes], model_type: str = "prebuilt-layout") -> dict:
        """Extracts structured data and confidence scores using Azure Document Intelligence."""
        try:
            _t0 = _time.time()
            file_stream.seek(0)
            poller = self.client.begin_analyze_document(model_type, file_stream)
            result = poller.result()
            try:
                trace_tool_call(
                    tool_name="DocumentAnalysisClient.begin_analyze_document",
                    latency_ms=int((_time.time() - _t0) * 1000),
                    args={"model_type": model_type},
                    output="Extraction completed",
                    status="success",
                )
            except Exception:
                pass
            return result.to_dict()
        except Exception as e:
            self.log_error(ERR_EXTRACTION_FAILURE, f"Extraction failed: {e}")
            try:
                trace_tool_call(
                    tool_name="DocumentAnalysisClient.begin_analyze_document",
                    latency_ms=0,
                    args={"model_type": model_type},
                    output=None,
                    status="error",
                    error=e,
                )
            except Exception:
                pass
            raise

class CanonicalizationService(BaseService):
    def canonicalize(self, extracted_data: dict) -> dict:
        """Canonicalizes extracted key-value pairs by page and bounding box."""
        try:
            # Placeholder: Canonicalization logic would be implemented here.
            # For now, return as-is.
            return extracted_data
        except Exception as e:
            self.log_error(ERR_EXTRACTION_FAILURE, f"Canonicalization failed: {e}")
            raise

class ConfidenceValidator(BaseService):
    def validate_confidence(self, canonicalized_data: dict) -> dict:
        """Validates per-field confidence and determines routing."""
        try:
            # Placeholder: Implement per-field confidence validation.
            # For now, assume all fields above threshold.
            # In real logic, scan canonicalized_data for confidence scores.
            # If any critical field < 0.60, set "route_to_review": True
            validation_result = {
                "all_critical_fields_above_threshold": True,
                "critical_fields_below_threshold": [],
                "route_to_review": False,
                "min_confidence": 1.0,
            }
            # Example: scan for confidence scores
            min_conf = 1.0
            critical_fields_below = []
            for field, value in canonicalized_data.get("fields", {}).items():
                conf = value.get("confidence", 1.0)
                if conf < 0.60:
                    validation_result["route_to_review"] = True
                    validation_result["all_critical_fields_above_threshold"] = False
                    critical_fields_below.append(field)
                if conf < min_conf:
                    min_conf = conf
            validation_result["critical_fields_below_threshold"] = critical_fields_below
            validation_result["min_confidence"] = min_conf
            return validation_result
        except Exception as e:
            self.log_error(ERR_LOW_CONFIDENCE, f"Confidence validation failed: {e}")
            raise

class RoutingService(BaseService):
    def route(self, fsr_payload: dict, confidence_result: dict) -> dict:
        """Routes FSR to human-in-loop queue or proceeds to output."""
        try:
            if confidence_result.get("route_to_review"):
                # Route to human-in-loop queue (fsr-needs-review)
                # Placeholder: Integration with queue/event bus
                routing_result = {
                    "routed_to": "fsr-needs-review",
                    "action": "human_review",
                    "success": True,
                }
            else:
                routing_result = {
                    "routed_to": "output",
                    "action": "proceed_to_output",
                    "success": True,
                }
            return routing_result
        except Exception as e:
            self.log_error(ERR_LOW_CONFIDENCE, f"Routing failed: {e}")
            raise

class OutputService(BaseService):
    def __init__(self):
        super().__init__()
        self.account_url = f"https://{Config.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        self.eventhub_conn_str = Config.EVENTHUB_CONNECTION_STRING
        self.eventhub_topic = Config.EVENTHUB_TOPIC

    def write_payload(self, payload: dict, container: str) -> bool:
        """Writes schema v2.1 payload to fsr-extracted container."""
        try:
            _t0 = _time.time()
            blob_service_client = BlobServiceClient(account_url=self.account_url, credential=Config.AZURE_STORAGE_KEY)
            blob_name = f"extracted_fsr_{int(_time.time())}.json"
            blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)
            data = json.dumps(payload).encode("utf-8")
            blob_client.upload_blob(data, overwrite=True)
            try:
                trace_tool_call(
                    tool_name="BlobServiceClient.upload_blob",
                    latency_ms=int((_time.time() - _t0) * 1000),
                    args={"container": container, "blob": blob_name},
                    output=f"Wrote {len(data)} bytes",
                    status="success",
                )
            except Exception:
                pass
            return True
        except Exception as e:
            self.log_error(ERR_SCHEMA_BUILD, f"Payload write failed: {e}")
            try:
                trace_tool_call(
                    tool_name="BlobServiceClient.upload_blob",
                    latency_ms=0,
                    args={"container": container},
                    output=None,
                    status="error",
                    error=e,
                )
            except Exception:
                pass
            raise

    @with_content_safety(config=GUARDRAILS_CONFIG)
    def notify_downstream(self, topic: str, message: dict) -> bool:
        """Sends notification to downstream agent via event bus."""
        try:
            _t0 = _time.time()
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.eventhub_conn_str,
                eventhub_name=topic
            )
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(message)))
            producer.send_batch(event_data_batch)
            producer.close()
            try:
                trace_tool_call(
                    tool_name="EventHubProducerClient.send_batch",
                    latency_ms=int((_time.time() - _t0) * 1000),
                    args={"topic": topic},
                    output="Notification sent",
                    status="success",
                )
            except Exception:
                pass
            return True
        except Exception as e:
            self.log_error("ERR_EVENTHUB_NOTIFY", f"Downstream notification failed: {e}")
            try:
                trace_tool_call(
                    tool_name="EventHubProducerClient.send_batch",
                    latency_ms=0,
                    args={"topic": topic},
                    output=None,
                    status="error",
                    error=e,
                )
            except Exception:
                pass
            return False

class PIIRedactor(BaseService):
    def redact_pii(self, data: dict) -> dict:
        """Masks or redacts PII in extracted data."""
        try:
            # Placeholder: Implement PII redaction logic.
            # For now, return as-is.
            return data
        except Exception as e:
            self.log_error("ERR_PII_REDACTION", f"PII redaction failed: {e}")
            return data

class ErrorHandler(BaseService):
    def handle_error(self, error_code: str, context: dict) -> dict:
        """Handles errors, retries, fallback routing, and logs issues."""
        self.log_error(error_code, context)
        # Fallback: Route to human-in-loop if possible
        if error_code in [ERR_EXTRACTION_FAILURE, ERR_LOW_CONFIDENCE, ERR_SCHEMA_BUILD]:
            return {
                "success": False,
                "error_code": error_code,
                "error": context.get("error", str(context)),
                "fixing_tips": "The document has been routed for human review. Please check the fsr-needs-review queue.",
            }
        return {
            "success": False,
            "error_code": error_code,
            "error": context.get("error", str(context)),
            "fixing_tips": "Please check the error details and try again.",
        }

class LoggingService(BaseService):
    pass

# ========== LLM OUTPUT SANITIZER ==========
import re as _re

_FENCE_RE = _re.compile(r"```(?:\w+)?\s*\n(.*?)```", _re.DOTALL)
_LONE_FENCE_START_RE = _re.compile(r"^```\w*$")
_WRAPPER_RE = _re.compile(
    r"^(?:"
    r"Here(?:'s| is)(?: the)? (?:the |your |a )?(?:code|solution|implementation|result|explanation|answer)[^:]*:\s*"
    r"|Sure[!,.]?\s*"
    r"|Certainly[!,.]?\s*"
    r"|Below is [^:]*:\s*"
    r")",
    _re.IGNORECASE,
)
_SIGNOFF_RE = _re.compile(
    r"^(?:Let me know|Feel free|Hope this|This code|Note:|Happy coding|If you)",
    _re.IGNORECASE,
)
_BLANK_COLLAPSE_RE = _re.compile(r"\n{3,}")

def _strip_fences(text: str, content_type: str) -> str:
    """Extract content from Markdown code fences."""
    fence_matches = _FENCE_RE.findall(text)
    if fence_matches:
        if content_type == "code":
            return "\n\n".join(block.strip() for block in fence_matches)
        for match in fence_matches:
            fenced_block = _FENCE_RE.search(text)
            if fenced_block:
                text = text[:fenced_block.start()] + match.strip() + text[fenced_block.end():]
        return text
    lines = text.splitlines()
    if lines and _LONE_FENCE_START_RE.match(lines[0].strip()):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()

def _strip_trailing_signoffs(text: str) -> str:
    """Remove conversational sign-off lines from the end of code output."""
    lines = text.splitlines()
    while lines and _SIGNOFF_RE.match(lines[-1].strip()):
        lines.pop()
    return "\n".join(lines).rstrip()

@with_content_safety(config=GUARDRAILS_CONFIG)
def sanitize_llm_output(raw: str, content_type: str = "code") -> str:
    """
    Generic post-processor that cleans common LLM output artefacts.
    Args:
        raw: Raw text returned by the LLM.
        content_type: 'code' | 'text' | 'markdown'.
    Returns:
        Cleaned string ready for validation, formatting, or direct return.
    """
    if not raw:
        return ""
    text = _strip_fences(raw.strip(), content_type)
    text = _WRAPPER_RE.sub("", text, count=1).strip()
    if content_type == "code":
        text = _strip_trailing_signoffs(text)
    return _BLANK_COLLAPSE_RE.sub("\n\n", text).strip()

# ========== MAIN AGENT ==========

class FSRAgent(BaseService):
    def __init__(self):
        super().__init__()
        self.ingestion_handler = FSRIngestionHandler()
        self.downloader = DocumentDownloader()
        self.extractor = ExtractionService()
        self.canonicalizer = CanonicalizationService()
        self.confidence_validator = ConfidenceValidator()
        self.router = RoutingService()
        self.output_service = OutputService()
        self.pii_redactor = PIIRedactor()
        self.error_handler = ErrorHandler()
        self.logger_service = LoggingService()

    @with_content_safety(config=GUARDRAILS_CONFIG)
    async def process_blob_event(self, blob_event: BlobEvent) -> dict:
        """
        Entry point for blob created events; orchestrates the extraction and routing pipeline.
        """
        async with trace_step(
            "process_blob_event",
            step_type="process",
            decision_summary="Orchestrate FSR extraction and routing pipeline",
            output_fn=lambda r: f"success={r.get('success', '?')}",
        ) as step:
            try:
                # 1. Validate file type/size
                async with trace_step(
                    "validate_file",
                    step_type="parse",
                    decision_summary="Validate file type and size",
                    output_fn=lambda r: f"valid={r}",
                ) as _:
                    if blob_event.blob_properties:
                        self.ingestion_handler.validate_file(blob_event.blob_properties)
                    else:
                        # If blob_properties not provided, skip validation (assume valid)
                        pass

                # 2. Download blob
                async with trace_step(
                    "download_blob",
                    step_type="tool_call",
                    decision_summary="Download PDF from Blob Storage",
                    output_fn=lambda r: f"file_stream={type(r)}",
                ) as _:
                    file_stream = self.downloader.download_blob(
                        blob_event.container_name,
                        blob_event.blob_name,
                        blob_event.sas_token
                    )

                # 3. Compute idempotency key
                async with trace_step(
                    "compute_idempotency_key",
                    step_type="process",
                    decision_summary="Compute SHA-256 hash for idempotency",
                    output_fn=lambda r: f"idempotency_key={r}",
                ) as _:
                    idempotency_key = self.ingestion_handler.compute_idempotency_key(file_stream)

                # 4. Extract data
                async with trace_step(
                    "extract_data",
                    step_type="tool_call",
                    decision_summary="Extract structured data and confidence scores",
                    output_fn=lambda r: f"extracted_data={type(r)}",
                ) as _:
                    extracted_data = self.extractor.extract_data(file_stream, model_type="prebuilt-layout")

                # 5. Canonicalize
                async with trace_step(
                    "canonicalize",
                    step_type="process",
                    decision_summary="Canonicalize key-value pairs",
                    output_fn=lambda r: f"canonicalized_data={type(r)}",
                ) as _:
                    canonicalized_data = self.canonicalizer.canonicalize(extracted_data)

                # 6. Validate confidence
                async with trace_step(
                    "validate_confidence",
                    step_type="process",
                    decision_summary="Validate per-field confidence",
                    output_fn=lambda r: f"route_to_review={r.get('route_to_review', '?')}",
                ) as _:
                    confidence_result = self.confidence_validator.validate_confidence(canonicalized_data)

                # 7. Redact PII
                async with trace_step(
                    "redact_pii",
                    step_type="process",
                    decision_summary="Redact PII in outputs",
                    output_fn=lambda r: f"pii_redacted={type(r)}",
                ) as _:
                    redacted_data = self.pii_redactor.redact_pii(canonicalized_data)

                # 8. Routing
                async with trace_step(
                    "route",
                    step_type="process",
                    decision_summary="Route FSR based on confidence",
                    output_fn=lambda r: f"routed_to={r.get('routed_to', '?')}",
                ) as _:
                    routing_result = self.router.route(redacted_data, confidence_result)

                # 9. Output or fallback
                if routing_result.get("routed_to") == "output":
                    # Build schema v2.1 payload
                    payload = {
                        "idempotency_key": idempotency_key,
                        "fsr_data": redacted_data,
                        "confidence_result": confidence_result,
                        "timestamp": int(_time.time()),
                    }
                    # Write to fsr-extracted container
                    async with trace_step(
                        "write_payload",
                        step_type="tool_call",
                        decision_summary="Write schema v2.1 payload to Blob Storage",
                        output_fn=lambda r: f"write_success={r}",
                    ) as _:
                        self.output_service.write_payload(payload, Config.FSR_EXTRACTED_CONTAINER)
                    # Notify downstream
                    async with trace_step(
                        "notify_downstream",
                        step_type="tool_call",
                        decision_summary="Notify downstream agent via event bus",
                        output_fn=lambda r: f"notify_success={r}",
                    ) as _:
                        self.output_service.notify_downstream(Config.EVENTHUB_TOPIC, {"idempotency_key": idempotency_key})
                    return {
                        "success": True,
                        "result": {
                            "idempotency_key": idempotency_key,
                            "routing": routing_result,
                            "confidence_result": confidence_result,
                        }
                    }
                else:
                    # Route to human-in-loop
                    return {
                        "success": False,
                        "error_code": ERR_LOW_CONFIDENCE,
                        "error": FALLBACK_RESPONSE,
                        "fixing_tips": "The document has been routed for human review. Please check the fsr-needs-review queue.",
                    }
            except Exception as e:
                self.log_error("PROCESS_BLOB_EVENT_ERROR", str(e))
                error_response = self.error_handler.handle_error(
                    getattr(e, "error_code", "PROCESS_BLOB_EVENT_ERROR"),
                    {"error": str(e)}
                )
                return error_response

# ========== FASTAPI APP & ENDPOINTS ==========

@asynccontextmanager
async def _obs_lifespan(application):
    """Initialise observability on startup, clean up on shutdown."""
    try:
        _obs_startup_logger.info('')
        _obs_startup_logger.info('========== Agent Configuration Summary ==========')
        _obs_startup_logger.info(f'Environment: {getattr(Config, "ENVIRONMENT", "N/A")}')
        _obs_startup_logger.info(f'Agent: {getattr(Config, "AGENT_NAME", "N/A")}')
        _obs_startup_logger.info(f'Project: {getattr(Config, "PROJECT_NAME", "N/A")}')
        _obs_startup_logger.info(f'LLM Provider: {getattr(Config, "MODEL_PROVIDER", "N/A")}')
        _obs_startup_logger.info(f'LLM Model: {getattr(Config, "LLM_MODEL", "N/A")}')
        _cs_endpoint = getattr(Config, 'AZURE_CONTENT_SAFETY_ENDPOINT', None)
        _cs_key = getattr(Config, 'AZURE_CONTENT_SAFETY_KEY', None)
        if _cs_endpoint and _cs_key:
            _obs_startup_logger.info('Content Safety: Enabled (Azure Content Safety)')
            _obs_startup_logger.info(f'Content Safety Endpoint: {_cs_endpoint}')
        else:
            _obs_startup_logger.info('Content Safety: Not Configured')
        _obs_startup_logger.info('Observability Database: Azure SQL')
        _obs_startup_logger.info(f'Database Server: {getattr(Config, "OBS_AZURE_SQL_SERVER", "N/A")}')
        _obs_startup_logger.info(f'Database Name: {getattr(Config, "OBS_AZURE_SQL_DATABASE", "N/A")}')
        _obs_startup_logger.info('===============================================')
        _obs_startup_logger.info('')
    except Exception as _e:
        _obs_startup_logger.warning('Config summary failed: %s', _e)

    _obs_startup_logger.info('')
    _obs_startup_logger.info('========== Content Safety & Guardrails ==========')
    if GUARDRAILS_CONFIG.get('content_safety_enabled'):
        _obs_startup_logger.info('Content Safety: Enabled')
        _obs_startup_logger.info(f'  - Severity Threshold: {GUARDRAILS_CONFIG.get("content_safety_severity_threshold", "N/A")}')
        _obs_startup_logger.info(f'  - Check Toxicity: {GUARDRAILS_CONFIG.get("check_toxicity", False)}')
        _obs_startup_logger.info(f'  - Check Jailbreak: {GUARDRAILS_CONFIG.get("check_jailbreak", False)}')
        _obs_startup_logger.info(f'  - Check PII Input: {GUARDRAILS_CONFIG.get("check_pii_input", False)}')
        _obs_startup_logger.info(f'  - Check Credentials Output: {GUARDRAILS_CONFIG.get("check_credentials_output", False)}')
    else:
        _obs_startup_logger.info('Content Safety: Disabled')
    _obs_startup_logger.info('===============================================')
    _obs_startup_logger.info('')

    _obs_startup_logger.info('========== Initializing Agent Services ==========')
    # 1. Observability DB schema (imports are inside function — only needed at startup)
    try:
        from observability.database.engine import create_obs_database_engine
        from observability.database.base import ObsBase
        import observability.database.models  # noqa: F401
        _obs_engine = create_obs_database_engine()
        ObsBase.metadata.create_all(bind=_obs_engine, checkfirst=True)
        _obs_startup_logger.info('✓ Observability database connected')
    except Exception as _e:
        _obs_startup_logger.warning('✗ Observability database connection failed (metrics will not be saved)')
    # 2. OpenTelemetry tracer (initialize_tracer is pre-injected at top level)
    try:
        _t = initialize_tracer()
        if _t is not None:
            _obs_startup_logger.info('✓ Telemetry monitoring enabled')
        else:
            _obs_startup_logger.warning('✗ Telemetry monitoring disabled')
    except Exception as _e:
        _obs_startup_logger.warning('✗ Telemetry monitoring failed to initialize')
    _obs_startup_logger.info('=================================================')
    _obs_startup_logger.info('')
    yield

app = FastAPI(
    title="FSR Extraction and Processing Agent",
    description="Extracts, validates, and routes Field Service Report (FSR) PDFs uploaded to Azure Blob Storage.",
    version=Config.SERVICE_VERSION if hasattr(Config, "SERVICE_VERSION") else "1.0.0",
    lifespan=_obs_lifespan
)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}

@app.exception_handler(RequestValidationError)
@with_content_safety(config=GUARDRAILS_CONFIG)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "error": "Malformed JSON or invalid request parameters.",
            "error_code": "ERR_INVALID_REQUEST",
            "fixing_tips": "Check your JSON syntax, required fields, and value types. Ensure all string fields are quoted and lists/dicts are properly formatted.",
            "details": exc.errors(),
        },
    )

@app.exception_handler(StarletteHTTPException)
@with_content_safety(config=GUARDRAILS_CONFIG)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.detail,
            "error_code": "ERR_HTTP",
            "fixing_tips": "Check the endpoint and request method.",
        },
    )

@app.post("/process_blob_event", response_model=QueryResponse)
@with_content_safety(config=GUARDRAILS_CONFIG)
async def process_blob_event_endpoint(blob_event: BlobEvent):
    """
    Process a blob created event for FSR PDF extraction and routing.
    """
    agent = FSRAgent()
    try:
        result = await agent.process_blob_event(blob_event)
        # Sanitize LLM output if present (not used in this pipeline, but for downstream compatibility)
        if isinstance(result, dict) and "result" in result and isinstance(result["result"], str):
            result["result"] = sanitize_llm_output(result["result"], content_type="code")
        return result
    except Exception as e:
        logger.error(f"Unhandled error in /process_blob_event: {e}")
        return {
            "success": False,
            "error": str(e),
            "error_code": "ERR_INTERNAL",
            "fixing_tips": "An unexpected error occurred. Please check the logs and try again.",
        }

async def _run_agent():
    """Entrypoint: runs the agent with observability (trace collection only)."""
    import uvicorn

    # Unified logging config — routes uvicorn, agent, and observability through
    # the same handler so all telemetry appears in a single consistent stream.
    _LOG_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(name)s: %(message)s",
                "use_colors": None,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn":        {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error":  {"level": "INFO"},
            "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
            "agent":          {"handlers": ["default"], "level": "INFO", "propagate": False},
            "__main__":       {"handlers": ["default"], "level": "INFO", "propagate": False},
            "observability": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "config": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "azure":   {"handlers": ["default"], "level": "WARNING", "propagate": False},
            "urllib3": {"handlers": ["default"], "level": "WARNING", "propagate": False},
        },
    }

    config = uvicorn.Config(
        "agent:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        log_level="info",
        log_config=_LOG_CONFIG,
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    _asyncio.run(_run_agent())