"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://127.0.0.1:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month, to_celsius=False):
        super().__init__(
            "org.chicago.cta.weather.v1",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0
        self.to_celsius = to_celsius

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    @staticmethod
    def _farenheit_to_celsius(farenheit):
        return round((farenheit - 32) * 5 / 9, 1)

    def run(self, month):
        self._set_weather(month)
        if self.to_celsius:
            temp = self._farenheit_to_celsius(self.temp)
        else:
            temp = self.temp
        data = {
            "key_schema": json.dumps(Weather.key_schema),
            "value_schema": json.dumps(Weather.value_schema),
            "records": [
                {
                    "key": {"timestamp": self.time_millis()},
                    "value": {
                        "temperature": temp,
                        "status": self.status.name,
                    }
                }
            ]
        }
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(data)
        )
        try:
            resp.raise_for_status()
        except Exception as err:
            logger.error(json.dumps(resp.json()))
            raise err

        logger.debug(
            "sent weather data to kafka, temp: {}, status: {}".format(
                temp,
                self.status.name
            )
        )
