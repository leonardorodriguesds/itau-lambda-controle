import logging
from typing import Dict

from injector import inject
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit

class CloudWatchService:
    """
    Service for aggregating and sending metrics to CloudWatch at the end of Lambda execution.
    """

    @inject
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.metrics = Metrics(namespace="LambdaControleItau")
        self._metric_data = []

    def add_metric(self, name: str, value: float, unit: MetricUnit = MetricUnit.Count):
        """
        Adds a metric to the list to be sent to CloudWatch.

        :param name: Name of the metric.
        :param value: Value of the metric.
        :param unit: Unit of the metric (e.g., Count, Seconds).
        """
        self.logger.debug(f"Adding metric: {name}, Value: {value}, Unit: {unit}")
        self._metric_data.append({"name": name, "value": value, "unit": unit})

    def flush_metrics(self):
        """
        Sends all collected metrics to CloudWatch.
        """
        self.logger.debug(f"[{self.__class__.__name__}] Flushing metrics to CloudWatch.")
        for metric in self._metric_data:
            self.metrics.add_metric(name=metric["name"], value=metric["value"], unit=metric["unit"])
        self.metrics.flush_metrics()
        self.logger.debug(f"[{self.__class__.__name__}] Metrics flushed successfully.")