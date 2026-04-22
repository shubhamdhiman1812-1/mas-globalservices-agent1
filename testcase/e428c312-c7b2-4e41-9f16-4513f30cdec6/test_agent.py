
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from agent import FSRAgent, BlobEvent

@pytest.mark.asyncio
async def test_process_blob_event_low_confidence_routing():
    """
    Ensures FSRAgent.process_blob_event correctly routes to human review when a critical field's confidence is below threshold.
    """
    # Prepare a valid BlobEvent
    blob_event = BlobEvent(
        container_name="test-container",
        blob_name="test.pdf",
        sas_token="sastoken",
        blob_properties={"content_type": "application/pdf", "size": 1024}
    )

    # Patch all external dependencies in the pipeline
    with patch.object(FSRAgent, "ingestion_handler", create=True) as mock_ingestion_handler, \
         patch.object(FSRAgent, "downloader", create=True) as mock_downloader, \
         patch.object(FSRAgent, "extractor", create=True) as mock_extractor, \
         patch.object(FSRAgent, "canonicalizer", create=True) as mock_canonicalizer, \
         patch.object(FSRAgent, "confidence_validator", create=True) as mock_confidence_validator, \
         patch.object(FSRAgent, "pii_redactor", create=True) as mock_pii_redactor, \
         patch.object(FSRAgent, "router", create=True) as mock_router, \
         patch.object(FSRAgent, "output_service", create=True) as mock_output_service:

        # Set up mocks for each step
        # 1. validate_file: should pass (no exception)
        mock_ingestion_handler.validate_file = MagicMock(return_value=True)
        # 2. download_blob: returns a BytesIO-like object
        import io
        fake_pdf = io.BytesIO(b"%PDF-1.4 fake pdf content")
        mock_downloader.download_blob = MagicMock(return_value=fake_pdf)
        # 3. compute_idempotency_key: return a fake hash
        mock_ingestion_handler.compute_idempotency_key = MagicMock(return_value="deadbeef" * 8)
        # 4. extract_data: returns extracted_data dict
        extracted_data = {"fields": {"critical_field": {"confidence": 0.5}}}
        mock_extractor.extract_data = MagicMock(return_value=extracted_data)
        # 5. canonicalize: returns canonicalized_data dict
        canonicalized_data = extracted_data
        mock_canonicalizer.canonicalize = MagicMock(return_value=canonicalized_data)
        # 6. validate_confidence: returns result indicating route_to_review=True
        confidence_result = {
            "all_critical_fields_above_threshold": False,
            "critical_fields_below_threshold": ["critical_field"],
            "route_to_review": True,
            "min_confidence": 0.5,
        }
        mock_confidence_validator.validate_confidence = MagicMock(return_value=confidence_result)
        # 7. redact_pii: returns canonicalized_data as-is
        mock_pii_redactor.redact_pii = MagicMock(return_value=canonicalized_data)
        # 8. router.route: returns routing to human review
        routing_result = {
            "routed_to": "fsr-needs-review",
            "action": "human_review",
            "success": True,
        }
        mock_router.route = MagicMock(return_value=routing_result)
        # 9. output_service.write_payload and notify_downstream should not be called in this path

        agent = FSRAgent()
        # Patch the agent's attributes to use our mocks
        agent.ingestion_handler = mock_ingestion_handler
        agent.downloader = mock_downloader
        agent.extractor = mock_extractor
        agent.canonicalizer = mock_canonicalizer
        agent.confidence_validator = mock_confidence_validator
        agent.pii_redactor = mock_pii_redactor
        agent.router = mock_router
        agent.output_service = mock_output_service

        response = await agent.process_blob_event(blob_event)

        assert isinstance(response, dict)
        assert response["success"] is False
        assert response["error_code"] == "ERR_LOW_CONFIDENCE"
        assert "routed for human review" in response["error"]
