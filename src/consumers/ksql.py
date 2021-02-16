"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"
KAFKA_TOPIC = "org.chicago.cta.turnstiles.v1"
RAW_TABLE_NAME = "TURNSTILE"
AGGREGATED_TABLE_NAME = "TURNSTILE_SUMMARY"

KSQL_STATEMENT = f"""
CREATE TABLE {RAW_TABLE_NAME} (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='{KAFKA_TOPIC}',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE {AGGREGATED_TABLE_NAME}
WITH (
    VALUE_FORMAT='JSON'
)
AS
    SELECT
        station_id,
        COUNT(station_id) AS count
    FROM
        {RAW_TABLE_NAME}
    GROUP BY
        station_id
;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(AGGREGATED_TABLE_NAME) is True:
        logger.info(f"Table {RAW_TABLE_NAME} already exists. Exiting...")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
        logger.info("Successfully created tables")
    except Exception as err:
        logger.error(json.dumps(resp.json()))
        raise err


if __name__ == "__main__":
    execute_statement()
