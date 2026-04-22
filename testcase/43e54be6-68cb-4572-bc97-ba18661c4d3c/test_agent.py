
import pytest
from unittest.mock import patch, MagicMock
from agent import FSRAgent, BlobEvent

@pytest.mark.asyncio
async def test_process_blob_event_invalid_file_type():
    """
    Checks that FSRAgent.process_blob_event rejects non-PDF files and returns the correct error.
    """
    # Prepare a BlobEvent with blob_properties indicating a non-PDF content_type
    blob_event = BlobEvent(
        container_name="test-container",
        blob_name="testfile.png",
        sas_token="sastoken",
        blob_properties={
            "content_type": "image/png",
            "size": 1024
        }
    )

    agent = FSRAgent()

    # Patch DocumentDownloader.download_blob to prevent real Azure calls
    with patch.object(agent.downloader, "download_blob", return_value=MagicMock()), \
         patch.object(agent.ingestion_handler, "validate_file", side_effect=ValueError("File is not a PDF")):
        response = await agent.process_blob_event(blob_event)

    assert response["success"] is False
    assert response["error_code"] == "ERR_INVALID_FILE_TYPE"
    assert "File is not a PDF" in response["error"]