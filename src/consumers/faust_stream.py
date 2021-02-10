"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App(
    "stations-stream",
    broker="kafka://localhost:9092",
    store="memory://"
)
topic = app.topic(
    "org.chicago.transit.stations",
    value_type=Station
)
out_topic = app.topic(
    "org.chicago.transit.stations.table.v1",
    partitions=1
)
table = app.Table(
    "org.chicago.transit.stations.table.v1",
    default="int",
    partitions=1, # TODO: Adjust
    changelog_topic=out_topic,
)


class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


def get_line(event):
    lines = set()
    event = event.asdict()
    for color in ["red", "blue", "green"]:
        value = event[color]
        if value:
            logger.debug(f"{color}, {value}")
            lines.add(color)
    if len(lines) == 0:
        logger.error("No color is set to True. One value is neccessary")
        return None
    elif len(lines) > 1:
        logger.error(
            f"{len(lines)} colors set to True. Only one value is allowed"
        )
    return list(lines)[0]


@app.agent(topic)
async def process(stream):
    async for event in stream:
        line = get_line(event)

        table[event.station_id] = TransformedStation(
            station_id=event.station_id,
            station_name=event.station_name,
            order=event.order,
            line=line
        )


if __name__ == "__main__":
    app.main()
