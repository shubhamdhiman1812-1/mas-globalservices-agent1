# NOTE: If you see "Unknown pytest.mark.X" warnings, create a conftest.py file with:
# import pytest
# def pytest_configure(config):
#     config.addinivalue_line("markers", "performance: mark test as performance test")
#     config.addinivalue_line("markers", "security: mark test as security test")
#     config.addinivalue_line("markers", "integration: mark test as integration test")


import pytest
from unittest.mock import patch, MagicMock
from agent import FSRAgent, BlobEvent
from azure.core.exceptions import FSRAgent

@pytest.mark.asyncio
@pytest.mark.security
async def test_process_blob_event_security_invalid_sas_token():
    """
    Ensures FSRAgent.process_blob_event securely handles invalid or expired SAS tokens,
    returning an appropriate error and not leaking sensitive information.
    """
    # Prepare a BlobEvent with an invalid SAS token
    blob_event = BlobEvent(
        container_name="test-container",
        blob_name="test.pdf",
        sas_token="invalid_or_expired_token",
        blob_properties={"content_type": "application/pdf", "size": 1024}
    )

    agent = FSRAgent()

    # Patch DocumentDownloader.download_blob to raise FSRAgent
    with patch.object(agent.downloader, "download_blob", side_effect=FSRAgent("Invalid SAS token")):
        response = await agent.process_blob_event(blob_event)

    assert response["success"] is False
    assert response["error_code"] == "ERR_BLOB_DOWNLOAD"
    # Ensure no sensitive info is leaked in the error message
    # (error message should not contain the SAS token or stack trace)
    error_msg = response.get("error", "")
    assert "invalid_or_expired_token" not in error_msg
    assert "sas_token" not in error_msg.lower()
    assert "traceback" not in error_msg.lower()
    assert "stack" not in error_msg.lower()