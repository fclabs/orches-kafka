import uuid

from pydantic import BaseModel, Field
from enum import Enum
from typing import Mapping, Any, Optional
import time


def get_timestamp():
    return int(time.time() * 1000)


class EventType(Enum):
    workflow_start = 1
    workflow_end = 2
    workflow_error = 3

    task_request = 10
    task_response = 11
    task_error = 12


def create_trace_id():
    return str(uuid.uuid4())


def create_event_id():
    return str(uuid.uuid4())


class Event(BaseModel):
    type: EventType
    timestamp: int = Field(gt=0, default_factory=get_timestamp)
    trace_id: str = Field(default_factory=create_trace_id)
    id: str = Field(gt=0, default_factory=create_event_id)
    source: str
    data: Optional[Mapping[str, Any]] = None
