import asyncio
import uuid
import os

from aiokafka import AIOKafkaProducer
from commons.schemas import Event, EventType

ORCH_EVENT_TOPIC = 'orchestrator_event'
SCHEDULER_SLEEP = 5

bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:19092')


async def scheduler():
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers)
    # Get cluster layout and join group `my-group`
    await producer.start()
    try:
        # Produce every 5 minutes
        while True:
            event_id = str(uuid.uuid4())
            event = Event(
                id=event_id,

                source='scheduler',
                type=EventType.workflow_start,
                data={'name': event_id}
            )
            await producer.send(
                topic=ORCH_EVENT_TOPIC,
                key=event_id.encode('utf-8'),
                value=event.model_dump_json().encode('utf-8')
            )
            await asyncio.sleep(SCHEDULER_SLEEP)

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(scheduler())
