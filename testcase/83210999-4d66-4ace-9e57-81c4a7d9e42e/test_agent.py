# NOTE: If you see "Unknown pytest.mark.X" warnings, create a conftest.py file with:
# import pytest
# def pytest_configure(config):
#     config.addinivalue_line("markers", "performance: mark test as performance test")
#     config.addinivalue_line("markers", "security: mark test as security test")
#     config.addinivalue_line("markers", "integration: mark test as integration test")


import pytest
import asyncio
import time
from unittest.mock import patch, MagicMock, AsyncMock
from agent import FSRAgent, BlobEvent

@pytest.fixture
def valid_blob_event():
    return BlobEvent(
        container_name="test-container",
        blob_name="test.pdf",
        sas_token="sastoken123",
        blob_properties={
            "content_type": "application/pdf",
            "size": 1024 * 1024  # 1MB
        }
    )

@pytest.fixture
def mock_agent_pipeline():
    """
    Patch all external dependencies in FSRAgent pipeline for performance test.
    """
    with patch("agent.FSRIngestionHandler.validate_file", return_value=True), \
         patch("agent.DocumentDownloader.download_blob") as mock_download_blob, \
         patch("agent.FSRIngestionHandler.compute_idempotency_key", return_value="deadbeef1234567890"), \
         patch("agent.ExtractionService.extract_data", return_value={"fields": {"foo": {"confidence": 0.99}}}), \
         patch("agent.CanonicalizationService.canonicalize", side_effect=lambda d: d), \
         patch("agent.ConfidenceValidator.validate_confidence", return_value={
             "all_critical_fields_above_threshold": True,
             "critical_fields_below_threshold": [],
             "route_to_review": False,
             "min_confidence": 0.99,
         }), \
         patch("agent.PIIRedactor.redact_pii", side_effect=lambda d: d), \
         patch("agent.RoutingService.route", return_value={
             "routed_to": "output",
             "action": "proceed_to_output",
             "success": True,
         }), \
         patch("agent.OutputService.write_payload", return_value=True), \
         patch("agent.OutputService.notify_downstream", return_value=True):
        # Simulate a file-like object for download_blob
        from io import BytesIO
        mock_download_blob.return_value = BytesIO(b"%PDF-1.4\n%mockpdfcontent\n")
        yield

@pytest.mark.performance
@pytest.mark.asyncio
async def test_process_blob_event_performance_under_load(valid_blob_event, mock_agent_pipeline):
    """
    Measures the performance of FSRAgent.process_blob_event under concurrent requests to ensure response time is within acceptable limits.
    """
    agent = FSRAgent()
    num_requests = 20
    timeout_per_request = 5.0  # seconds

    async def run_single():
        result = await agent.process_blob_event(valid_blob_event)
        assert isinstance(result, dict)
        assert result.get("success") is True
        assert "result" in result
        assert "idempotency_key" in result["result"]
        assert result["result"]["routing"]["routed_to"] == "output"
        assert result["result"]["confidence_result"]["all_critical_fields_above_threshold"] is True
        return result

    start_time = time.time()
    # Run all requests concurrently
    results = await asyncio.gather(
        *[run_single() for _ in range(num_requests)],
        return_exceptions=False
    )
    total_duration = time.time() - start_time

    # Assert all responses are correct and within SLA
    assert len(results) == num_requests
    assert all(r.get("success") is True for r in results)
    assert total_duration < num_requests * timeout_per_request, (
        f"Total duration {total_duration}s exceeds SLA for {num_requests} requests"
    )