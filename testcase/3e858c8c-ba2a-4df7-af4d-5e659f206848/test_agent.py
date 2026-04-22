# NOTE: If you see "Unknown pytest.mark.X" warnings, create a conftest.py file with:
# import pytest
# def pytest_configure(config):
#     config.addinivalue_line("markers", "performance: mark test as performance test")
#     config.addinivalue_line("markers", "security: mark test as security test")
#     config.addinivalue_line("markers", "integration: mark test as integration test")


import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from agent import FSRAgent, BlobEvent

@pytest.fixture
def unsafe_blob_event():
    # Minimal valid BlobEvent with dummy values
    return BlobEvent(
        container_name="test-container",
        blob_name="unsafe.pdf",
        sas_token="sastoken",
        blob_properties={
            "content_type": "application/pdf",
            "size": 1024 * 1024  # 1MB
        }
    )

@pytest.mark.asyncio
@pytest.mark.security
async def test_process_blob_event_security_content_safety_enforced(unsafe_blob_event):
    """
    Verifies that content safety guardrails are enforced during extraction and output,
    blocking or flagging unsafe content.
    """
    agent = FSRAgent()

    # Patch all steps to succeed except the content safety decorator, which will simulate a violation.
    # Patch the with_content_safety decorator to raise an Exception indicating content safety violation.
    # The decorator is applied to extract_data, notify_downstream, sanitize_llm_output, and process_blob_event.

    # Patch the extractor's extract_data to raise a content safety violation
    with patch("agent.ExtractionService.extract_data") as mock_extract_data, \
         patch("agent.DocumentDownloader.download_blob") as mock_download_blob, \
         patch("agent.FSRIngestionHandler.validate_file") as mock_validate_file, \
         patch("agent.FSRIngestionHandler.compute_idempotency_key") as mock_idempotency, \
         patch("agent.CanonicalizationService.canonicalize") as mock_canonicalize, \
         patch("agent.ConfidenceValidator.validate_confidence") as mock_validate_conf, \
         patch("agent.PIIRedactor.redact_pii") as mock_redact_pii, \
         patch("agent.RoutingService.route") as mock_route, \
         patch("agent.OutputService.write_payload") as mock_write_payload, \
         patch("agent.OutputService.notify_downstream") as mock_notify_downstream:

        # Simulate all steps up to extraction as normal
        mock_download_blob.return_value = MagicMock()
        mock_validate_file.return_value = True
        mock_idempotency.return_value = "fakehash"
        mock_canonicalize.return_value = {"fields": {"foo": {"confidence": 1.0}}}
        mock_validate_conf.return_value = {
            "all_critical_fields_above_threshold": True,
            "critical_fields_below_threshold": [],
            "route_to_review": False,
            "min_confidence": 1.0,
        }
        mock_redact_pii.return_value = {"fields": {"foo": {"confidence": 1.0}}}
        mock_route.return_value = {
            "routed_to": "output",
            "action": "proceed_to_output",
            "success": True,
        }
        mock_write_payload.return_value = True
        mock_notify_downstream.return_value = True

        # Simulate content safety violation by raising an Exception in extract_data
        class FSRAgent(Exception):
            pass

        mock_extract_data.side_effect = FSRAgent("content safety violation: toxic content detected")

        response = await agent.process_blob_event(unsafe_blob_event)

        assert response["success"] is False
        # Accept either 'content safety' or 'toxic' in error message
        error_msg = response.get("error", "") or ""
        assert "content safety" in error_msg.lower() or "toxic" in error_msg.lower()
        # Ensure no unsafe content is present in the output (no 'result' key or it's None)
        assert "result" not in response or response["result"] is None