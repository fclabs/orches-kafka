import os
import asyncio
from aiokafka import AIOKafkaConsumer
from typing import Tuple
from commons.schemas import Event, EventType, create_event_id

ORCH_EVENT_TOPIC = 'orchestrator_event'
ORCH_TOPIC_GROUP = 'orchestrator_event_group'
ECHO_TASK_TOPIC = 'echo_task_event'

bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:19092')

async def process_event(event: Event ) -> Tuple[str, Event]:

    match event.event_type:
        case EventType.workflow_start:
            new_event = event.copy(
                id = create_event_id(),
                event_type = EventType.task_request,
                source='orch_engine' )
            new_topic = ECHO_TASK_TOPIC
            return new_topic, new_event
        case EventType.task_error:
            new_event = event.copy(
                id=create_event_id(),
                event_type=EventType.workflow_error)
            new_topic = ORCH_EVENT_TOPIC
            return new_topic, new_event
        case EventType.task_response:
            new_event = event.copy(
                id=create_event_id(),
                event_type=EventType.workflow_end)
            new_topic = ORCH_EVENT_TOPIC
            return new_topic, new_event


async def consume():
    consumer = AIOKafkaConsumer(
        ORCH_EVENT_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=ORCH_TOPIC_GROUP)
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())
