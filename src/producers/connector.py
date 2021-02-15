"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://127.0.0.1:8083/connectors"
CONNECTOR_NAME = "stations.v1"
CONNECTION_URL = os.getenv("CONNECTION_URL", "jdbc:postgresql://postgres:5432/cta")
CONNECTION_USER = os.getenv("CONNECTION_USER", "cta_admin")
CONNECTION_PASSWORD = os.getenv("CONNECTION_PASSWORD", "chicago")


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.info("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    "connection.url": CONNECTION_URL,
                    "connection.user": CONNECTION_USER,
                    "connection.password": CONNECTION_PASSWORD,
                    "table.whitelist": "stations",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": "org.chicago.cta.",
                    "poll.interval.ms": "10000"
                }
            }
        )
    )
    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
        logging.info("connector created successfully")
    except Exception as err:
        logging.error(f"Failed with code: {resp.status_code} and reason: {json.dumps(resp.json())}")
        raise err


if __name__ == "__main__":
    configure_connector()
