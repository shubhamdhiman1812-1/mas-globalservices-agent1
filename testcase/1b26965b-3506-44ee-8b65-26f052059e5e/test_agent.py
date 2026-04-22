
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from agent import FSRAgent, BlobEvent

@pytest.fixture
def valid_blob_event():
    """Returns a valid BlobEvent for a PDF under 50MB."""
    return BlobEvent(
        container_name="test-container",
        blob_name="test.pdf",
        sas_token="sastoken",
        blob_properties={
            "content_type": "application/pdf",
            "size": 1024 * 1024 * 2  # 2MB
        }
    )

@pytest.mark.asyncio
async def test_process_blob_event_successful_pdf_extraction(valid_blob_event):
    """
    Validates the end-to-end workflow of FSRAgent.process_blob_event for a valid PDF blob event,
    ensuring extraction, canonicalization, confidence validation, routing, output writing,
    and downstream notification all succeed.
    """
    agent = FSRAgent()

    # Patch all external dependencies in the pipeline
    # 1. DocumentDownloader.download_blob returns a BytesIO-like object
    fake_pdf_bytes = b"%PDF-1.4\n%Fake PDF content"
    fake_file_stream = MagicMock()
    fake_file_stream.read = MagicMock(side_effect=[fake_pdf_bytes, b""])
    fake_file_stream.seek = MagicMock()
    fake_file_stream.__enter__ = lambda s: s
    fake_file_stream.__exit__ = lambda s, exc_type, exc_val, exc_tb: None

    # 2. FSRIngestionHandler.compute_idempotency_key returns a fake hash
    fake_idempotency_key = "deadbeef" * 8

    # 3. ExtractionService.extract_data returns extracted_data dict
    extracted_data = {
        "fields": {
            "field1": {"value": "foo", "confidence": 0.95},
            "field2": {"value": "bar", "confidence": 0.99}
        }
    }

    # 4. CanonicalizationService.canonicalize returns canonicalized_data (same as extracted)
    canonicalized_data = extracted_data

    # 5. ConfidenceValidator.validate_confidence returns all above threshold
    confidence_result = {
        "all_critical_fields_above_threshold": True,
        "critical_fields_below_threshold": [],
        "route_to_review": False,
        "min_confidence": 0.95
    }

    # 6. PIIRedactor.redact_pii returns canonicalized_data as-is
    redacted_data = canonicalized_data

    # 7. RoutingService.route returns routed_to output
    routing_result = {
        "routed_to": "output",
        "action": "proceed_to_output",
        "success": True
    }

    # 8. OutputService.write_payload returns True
    # 9. OutputService.notify_downstream returns True

    with patch.object(agent.downloader, "download_blob", return_value=fake_file_stream) as mock_download_blob, \
         patch.object(agent.ingestion_handler, "compute_idempotency_key", return_value=fake_idempotency_key) as mock_idempotency, \
         patch.object(agent.extractor, "extract_data", return_value=extracted_data) as mock_extract, \
         patch.object(agent.canonicalizer, "canonicalize", return_value=canonicalized_data) as mock_canon, \
         patch.object(agent.confidence_validator, "validate_confidence", return_value=confidence_result) as mock_conf, \
         patch.object(agent.pii_redactor, "redact_pii", return_value=redacted_data) as mock_redact, \
         patch.object(agent.router, "route", return_value=routing_result) as mock_route, \
         patch.object(agent.output_service, "write_payload", return_value=True) as mock_write, \
         patch.object(agent.output_service, "notify_downstream", return_value=True) as mock_notify:

        response = await agent.process_blob_event(valid_blob_event)

        assert isinstance(response, dict)
        assert response["success"] is True
        assert "result" in response
        assert "idempotency_key" in response["result"]
        assert response["result"]["idempotency_key"] == fake_idempotency_key
        assert "routing" in response["result"]
        assert response["result"]["routing"]["routed_to"] == "output"
        assert "confidence_result" in response["result"]
        assert response["result"]["confidence_result"]["all_critical_fields_above_threshold"] is True