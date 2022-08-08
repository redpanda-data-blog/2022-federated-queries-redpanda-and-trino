import asyncio
import json
import os
import random
from datetime import datetime
from random import randint

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP_SERVERS = (
    "redpanda:9092"
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER"
    else "127.0.0.1:54810"
)

EVENTS_TOPIC_NAME = "user_events"

MOCK_EVENTS = [
    "email_click",
    "link_1_click",
    "link_2_click",
    "pdf_download",
    "video_play",
    "website_visit",
]

async def generate_user_event_data():
    # Create a Kafka producer to interact with Redpanda
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    # Create a loop to send data to the Redpanda topic
    while True:
        # Generate randomized user event data for a non-existing website
        data = {
            "timestamp": datetime.now().isoformat(),  # ISO 8601 timestamp, because Trino can only parse this
            "event_name": random.choice(MOCK_EVENTS),
            "event_value": randint(0, 1),
        }

        # Send the data to the Redpanda topic
        key = str(randint(0, 1000)).encode("utf-8")
        value = json.dumps(data).encode("utf-8")
        producer.send(
            EVENTS_TOPIC_NAME,
            key=key,
            value=value,
        )
        print(
            f"Sent data to Redpanda topic {EVENTS_TOPIC_NAME}: {key} - {value}, sleeping for 1 second"
        )
        await asyncio.sleep(1)


async def main():
    # Run 5 async workers to speed up data generation
    tasks = [generate_user_event_data() for worker in range(5)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Create kafka topics if running in Docker.
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS, client_id="user-event-producer"
    )
    # Check if topic already exists first
    existing_topics = admin_client.list_topics()
    if EVENTS_TOPIC_NAME not in existing_topics:
        admin_client.create_topics(
            [NewTopic(EVENTS_TOPIC_NAME, num_partitions=1, replication_factor=1)]
        )
    asyncio.run(main())
