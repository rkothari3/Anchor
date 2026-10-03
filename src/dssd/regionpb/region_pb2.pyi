from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AgentState(_message.Message):
    __slots__ = ("id", "x", "y", "vx", "vy")
    ID_FIELD_NUMBER: _ClassVar[int]
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    VX_FIELD_NUMBER: _ClassVar[int]
    VY_FIELD_NUMBER: _ClassVar[int]
    id: str
    x: float
    y: float
    vx: float
    vy: float
    def __init__(self, id: _Optional[str] = ..., x: _Optional[float] = ..., y: _Optional[float] = ..., vx: _Optional[float] = ..., vy: _Optional[float] = ...) -> None: ...

class HandOffRequest(_message.Message):
    __slots__ = ("shard_id", "term", "agent")
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    shard_id: str
    term: int
    agent: AgentState
    def __init__(self, shard_id: _Optional[str] = ..., term: _Optional[int] = ..., agent: _Optional[_Union[AgentState, _Mapping]] = ...) -> None: ...

class HandOffResponse(_message.Message):
    __slots__ = ("accepted", "reason")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    reason: str
    def __init__(self, accepted: _Optional[bool] = ..., reason: _Optional[str] = ...) -> None: ...
