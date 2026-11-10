"""Module de consensus distribué (Protocole Raft)."""
from src.consensus.raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    HttpRaftTransport,
    InMemoryRaftTransport,
    LogEntry,
    NodeRole,
    RaftNode,
    RaftTransport,
    RequestVoteRequest,
    RequestVoteResponse,
)
from src.consensus.state_machine import (
    NotLeaderError,
    QuorumNotReachedError,
    RaftStateMachine,
)

__all__ = [
    "AppendEntriesRequest",
    "AppendEntriesResponse",
    "HttpRaftTransport",
    "InMemoryRaftTransport",
    "LogEntry",
    "NodeRole",
    "NotLeaderError",
    "QuorumNotReachedError",
    "RaftNode",
    "RaftStateMachine",
    "RaftTransport",
    "RequestVoteRequest",
    "RequestVoteResponse",
]
