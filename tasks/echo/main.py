import json
import os
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from commons.schemas import Event, EventType

ECHO_TASK_TOPIC = 'task_echo_event'
ORCH_EVENT_TOPIC = 'orchestrator_event'

ECHO_TASK_GROUP = 'task_echo_event_group'

bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:19092')


async def process_event(event: Event) -> Event:
    if event['type'] == EventType.task_request:
        print('Received: ', event.input)
        new_event = Event(
            event_type=EventType.task_response,
            trace_id=event.trace_id,
            source='task_echo',
            data=event.data
        )
        return new_event
    else:
        return Event(
            event_type=EventType.task_error,
            trace_id=event.trace_id,
            source='task_echo',
            data={
                'error': 'invalid event',
                'event_id': event.id
            }
        )


async def task_echo():
    consumer = AIOKafkaConsumer(
        ECHO_TASK_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=ECHO_TASK_GROUP)
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers)
    await producer.start()
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)

            event = Event(**json.loads(msg.value.decode('utf-8')))

            next_event = await process_event(event=event)
            await producer.send(
                topic=ORCH_EVENT_TOPIC,
                key=next_event.id.encode('utf-8'),
                value=next_event.model_dump_json().encode('utf-8')
            )
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(task_echo())
