from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MemberState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ALIVE: _ClassVar[MemberState]
    SUSPECT: _ClassVar[MemberState]
    DEAD: _ClassVar[MemberState]
ALIVE: MemberState
SUSPECT: MemberState
DEAD: MemberState

class Member(_message.Message):
    __slots__ = ("id", "addr", "state", "incarnation")
    ID_FIELD_NUMBER: _ClassVar[int]
    ADDR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    INCARNATION_FIELD_NUMBER: _ClassVar[int]
    id: str
    addr: str
    state: MemberState
    incarnation: int
    def __init__(self, id: _Optional[str] = ..., addr: _Optional[str] = ..., state: _Optional[_Union[MemberState, str]] = ..., incarnation: _Optional[int] = ...) -> None: ...

class GetMembersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetMembersResponse(_message.Message):
    __slots__ = ("members",)
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    members: _containers.RepeatedCompositeFieldContainer[Member]
    def __init__(self, members: _Optional[_Iterable[_Union[Member, _Mapping]]] = ...) -> None: ...

class LogEntry(_message.Message):
    __slots__ = ("term", "index", "command")
    TERM_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    term: int
    index: int
    command: bytes
    def __init__(self, term: _Optional[int] = ..., index: _Optional[int] = ..., command: _Optional[bytes] = ...) -> None: ...

class RequestVoteArgs(_message.Message):
    __slots__ = ("group", "term", "candidate_id", "last_log_index", "last_log_term")
    GROUP_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    group: str
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    def __init__(self, group: _Optional[str] = ..., term: _Optional[int] = ..., candidate_id: _Optional[str] = ..., last_log_index: _Optional[int] = ..., last_log_term: _Optional[int] = ...) -> None: ...

class RequestVoteReply(_message.Message):
    __slots__ = ("term", "vote_granted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_GRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    vote_granted: bool
    def __init__(self, term: _Optional[int] = ..., vote_granted: _Optional[bool] = ...) -> None: ...

class AppendEntriesArgs(_message.Message):
    __slots__ = ("group", "term", "leader_id", "prev_log_index", "prev_log_term", "entries", "leader_commit")
    GROUP_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    LEADER_COMMIT_FIELD_NUMBER: _ClassVar[int]
    group: str
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: _containers.RepeatedCompositeFieldContainer[LogEntry]
    leader_commit: int
    def __init__(self, group: _Optional[str] = ..., term: _Optional[int] = ..., leader_id: _Optional[str] = ..., prev_log_index: _Optional[int] = ..., prev_log_term: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[LogEntry, _Mapping]]] = ..., leader_commit: _Optional[int] = ...) -> None: ...

class AppendEntriesReply(_message.Message):
    __slots__ = ("term", "success")
    TERM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    term: int
    success: bool
    def __init__(self, term: _Optional[int] = ..., success: _Optional[bool] = ...) -> None: ...
