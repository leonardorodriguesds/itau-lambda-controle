import pytest
from unittest.mock import MagicMock, patch
from aws_lambda_powertools.metrics import MetricUnit
from src.itaufluxcontrol.service.cloud_watch_service import CloudWatchService 

@pytest.fixture
def cloudwatch_service():
    logger = MagicMock()
    with patch("src.itaufluxcontrol.service.cloud_watch_service.Metrics") as mock_metrics:
        metrics_instance = mock_metrics.return_value
        return CloudWatchService(logger), metrics_instance

def test_add_metric(cloudwatch_service):
    service, _ = cloudwatch_service
    name = "TestMetric"
    value = 1.0
    unit = MetricUnit.Count

    service.add_metric(name=name, value=value, unit=unit)

    assert len(service._metric_data) == 1
    assert service._metric_data[0]["name"] == name
    assert service._metric_data[0]["value"] == value
    assert service._metric_data[0]["unit"] == unit

def test_flush_metrics(cloudwatch_service):
    service, mock_metrics = cloudwatch_service
    mock_add_metric = mock_metrics.add_metric
    mock_flush_metrics = mock_metrics.flush_metrics

    service.add_metric(name="Metric1", value=1.0, unit=MetricUnit.Count)
    service.add_metric(name="Metric2", value=2.0, unit=MetricUnit.Seconds)

    service.flush_metrics()

    mock_add_metric.assert_any_call(name="Metric1", value=1.0, unit=MetricUnit.Count)
    mock_add_metric.assert_any_call(name="Metric2", value=2.0, unit=MetricUnit.Seconds)
    mock_flush_metrics.assert_called_once()
