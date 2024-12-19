import pytest
from unittest.mock import MagicMock, patch
from boto3.session import Session as BotoSession

from src.app.service.boto_service import BotoService


@pytest.fixture
def boto_service():
    logger = MagicMock()
    session_provider = MagicMock(spec=BotoSession)
    return BotoService(logger=logger, session_provider=session_provider), logger, session_provider


def test_get_client_creates_new_client(boto_service):
    service, logger, session_provider = boto_service
    session_provider.client.return_value = MagicMock()

    client = service.get_client(service_name="s3", region_name="us-east-1")

    assert client is not None
    session_provider.client.assert_called_once_with(
        "s3", region_name="us-east-1", endpoint_url="http://localhost:4566"
    )


def test_get_client_reuses_existing_client(boto_service):
    service, logger, session_provider = boto_service
    mock_client = MagicMock()
    service._clients[("s3", "us-east-1")] = mock_client

    client = service.get_client(service_name="s3", region_name="us-east-1")

    assert client == mock_client
    session_provider.client.assert_not_called()


def test_get_resource_creates_new_resource(boto_service):
    service, logger, session_provider = boto_service
    session_provider.resource.return_value = MagicMock()

    resource = service.get_resource(service_name="dynamodb", region_name="us-west-2")

    assert resource is not None
    session_provider.resource.assert_called_once_with(
        "dynamodb", region_name="us-west-2", endpoint_url="http://localhost:4566"
    )


def test_get_resource_reuses_existing_resource(boto_service):
    service, logger, session_provider = boto_service
    mock_resource = MagicMock()
    service._resources[("dynamodb", "us-west-2")] = mock_resource

    resource = service.get_resource(service_name="dynamodb", region_name="us-west-2")

    assert resource == mock_resource
    session_provider.resource.assert_not_called()


def test_clear_cache(boto_service):
    service, logger, _ = boto_service
    service._clients[("s3", "us-east-1")] = MagicMock()
    service._resources[("dynamodb", "us-west-2")] = MagicMock()

    service.clear_cache()

    assert len(service._clients) == 0
    assert len(service._resources) == 0


def test_get_localstack_endpoint(boto_service):
    service, _, _ = boto_service
    with patch("os.getenv", return_value="custom-host"):
        endpoint_url = service._get_localstack_endpoint("s3")

    assert endpoint_url == "http://custom-host:4566"
