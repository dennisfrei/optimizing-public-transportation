"""Defines core consumer functionality"""
import json
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=.5,
        consume_timeout=0.1,
        broker_properties={}
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self._BROKER_URL = "PLAINTEXT://localhost:9092"
        self._SCHEMA_REGISTRY_URL = "http://localhost:8081"
        self.broker_properties = {
            "bootstrap.servers": self._BROKER_URL,
            "group.id": "0"
        }

        #if offset_earliest is True:
        #    self.broker_properties.update(
        #        {"auto.offset.reset": "earliest"}
        #    )

        if is_avro is True:
            self.broker_properties.update(
                {"schema.registry.url": self._SCHEMA_REGISTRY_URL}
            )
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
        self.broker_properties.update(broker_properties)

        
        self.consumer.subscribe(
            [self.topic_name_pattern],
            on_assign=self.on_assign
        )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        for partition in partitions:
            if self.offset_earliest == True:
                partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)


    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(self.consume_timeout)
            
            assert_message_1 = f"Message should not be None for {self.topic_name_pattern}"
            assert message is not None, assert_message_1

            assert_message_2 = "Error message: {}".format(
                json.dumps(message.error())
            )
            assert message.error() is None, assert_message_2

            result = message.value()
            self.message_handler(message)
            return 1

        except AssertionError as err:
            logger.debug(err)
            return 0

        except KeyError as err:
            logger.error(f"Failed to get message value: {err}")
            return 0

        except Exception as err:
            logger.error(f"{err}")
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        logger.info("Shutting down consumer.")