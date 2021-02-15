"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

from models.topic_check import existing_topic_set

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = existing_topic_set()

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
        broker_properties={},
        topic_properties={}
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self._BROKER_URL = "PLAINTEXT://127.0.0.1:9092"
        self._SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
        self.schema_registry = CachedSchemaRegistryClient(
            {"url": self._SCHEMA_REGISTRY_URL}
        )

        self.broker_properties = {
            "bootstrap.servers": self._BROKER_URL,
            "client.id": "0",
            "compression.type": "lz4"
        }
        self.broker_properties.update(broker_properties)

        self.topic_properties = {
            "cleanup.policy": "delete"
        }
        self.topic_properties.update(topic_properties)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=self.schema_registry,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient({"bootstrap.servers": self._BROKER_URL})
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config=self.topic_properties
                )
            ]
        )
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Successfully created topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to create topic: {self.topic_name}")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.debug(f"Shutting down")
        if self.producer is not None:
            self.producer.flush()
