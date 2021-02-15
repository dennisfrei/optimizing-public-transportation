from confluent_kafka.admin import AdminClient


def existing_topic_set():
    """Returns a set of existing topic in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    topic_metadata = client.list_topics(timeout=5)
    return set(t.topic for t in iter(topic_metadata.topics.values()))


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    return topic in existing_topic_set()
