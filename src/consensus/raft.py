"""Implémentation du cœur de l'algorithme de consensus Raft."""
from __future__ import annotations

import json
import os
import random
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Protocol

import httpx


class NodeRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogEntry:
        return cls(
            term=int(data["term"]),
            index=int(data["index"]),
            command=dict(data["command"]),
        )


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RequestVoteRequest:
        return cls(
            term=int(data["term"]),
            candidate_id=str(data["candidate_id"]),
            last_log_index=int(data["last_log_index"]),
            last_log_term=int(data["last_log_term"]),
        )


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RequestVoteResponse:
        return cls(term=int(data["term"]), vote_granted=bool(data["vote_granted"]))


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [entry.to_dict() for entry in self.entries],
            "leader_commit": self.leader_commit,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AppendEntriesRequest:
        return cls(
            term=int(data["term"]),
            leader_id=str(data["leader_id"]),
            prev_log_index=int(data["prev_log_index"]),
            prev_log_term=int(data["prev_log_term"]),
            entries=[LogEntry.from_dict(item) for item in data.get("entries", [])],
            leader_commit=int(data.get("leader_commit", 0)),
        )


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AppendEntriesResponse:
        return cls(
            term=int(data["term"]),
            success=bool(data["success"]),
            match_index=int(data.get("match_index", 0)),
        )


class RaftTransport(Protocol):
    """Interface de communication inter-nœuds pour les RPC Raft."""

    def send_request_vote(
        self, target_id: str, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        ...

    def send_append_entries(
        self, target_id: str, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        ...

    def add_peer(self, node_id: str, url: str | None = None) -> None:
        ...

    def remove_peer(self, node_id: str) -> None:
        ...


class HttpRaftTransport:
    """Transport HTTP utilisant httpx pour communiquer avec les autres nœuds."""

    def __init__(self, peers: dict[str, str], timeout: float = 0.5):
        self.peers: dict[str, str] = {k: v.rstrip("/") for k, v in peers.items()}
        self.timeout = timeout

    def add_peer(self, node_id: str, url: str | None = None) -> None:
        if url:
            self.peers[node_id] = url.rstrip("/")

    def remove_peer(self, node_id: str) -> None:
        self.peers.pop(node_id, None)

    def send_request_vote(
        self, target_id: str, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        url = self.peers.get(target_id)
        if not url:
            return None
        try:
            response = httpx.post(
                f"{url}/raft/request_vote",
                json=request.to_dict(),
                timeout=self.timeout,
            )
            response.raise_for_status()
            return RequestVoteResponse.from_dict(response.json())
        except Exception:
            return None

    def send_append_entries(
        self, target_id: str, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        url = self.peers.get(target_id)
        if not url:
            return None
        try:
            response = httpx.post(
                f"{url}/raft/append_entries",
                json=request.to_dict(),
                timeout=self.timeout,
            )
            response.raise_for_status()
            return AppendEntriesResponse.from_dict(response.json())
        except Exception:
            return None


class InMemoryRaftTransport:
    """Transport en mémoire pour tester un cluster Raft et simuler des pannes réseau."""

    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.peer_urls: dict[str, str] = {}
        self.isolated_nodes: set[str] = set()
        self.blocked_links: set[tuple[str, str]] = set()

    def register(self, node: RaftNode) -> None:
        self.nodes[node.node_id] = node

    def add_peer(self, node_id: str, url: str | None = None) -> None:
        if url:
            self.peer_urls[node_id] = url.rstrip("/")

    def remove_peer(self, node_id: str) -> None:
        self.peer_urls.pop(node_id, None)

    def can_communicate(self, source_id: str, target_id: str) -> bool:
        if source_id in self.isolated_nodes or target_id in self.isolated_nodes:
            return False
        if (source_id, target_id) in self.blocked_links:
            return False
        return target_id in self.nodes

    def send_request_vote(
        self, target_id: str, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        if not self.can_communicate(request.candidate_id, target_id):
            return None
        return self.nodes[target_id].handle_request_vote(request)

    def send_append_entries(
        self, target_id: str, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        if not self.can_communicate(request.leader_id, target_id):
            return None
        return self.nodes[target_id].handle_append_entries(request)


class RaftNode:
    """Nœud implémentant le protocole de consensus Raft."""

    def __init__(
        self,
        node_id: str,
        peer_ids: list[str] | None = None,
        transport: RaftTransport | None = None,
        state_file: str | Path | None = None,
        election_timeout_range: tuple[float, float] = (0.15, 0.30),
        heartbeat_interval: float = 0.05,
        on_commit_callback: Callable[[LogEntry], None] | None = None,
    ):
        if not node_id:
            raise ValueError("L'identifiant du nœud Raft ne peut pas être vide.")
        self.node_id = node_id
        self.peer_ids: list[str] = [p for p in (peer_ids or []) if p != node_id]
        self.transport = transport
        self.state_file = Path(state_file) if state_file else None
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.on_commit_callback = on_commit_callback

        self._lock = threading.RLock()

        # État persistant sur tous les serveurs (figure 2 de l'article Raft)
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # État volatil sur tous les serveurs
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: NodeRole = NodeRole.FOLLOWER
        self.leader_id: str | None = None

        # État volatil sur les leaders (réinitialisé après chaque élection)
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Gestion du temps (horloge monotone)
        self._last_contact: float = time.monotonic()
        self._last_heartbeat_sent: float = 0.0
        self._election_timeout: float = self._random_election_timeout()

        # Thread de fond optionnel
        self._running = False
        self._thread: threading.Thread | None = None

        if self.state_file:
            self._load_state()

    def _random_election_timeout(self) -> float:
        low, high = self.election_timeout_range
        return random.uniform(low, high)

    def _reset_election_timer(self) -> None:
        self._last_contact = time.monotonic()
        self._election_timeout = self._random_election_timeout()

    # --- Persistance de l'état Raft ---

    def _load_state(self) -> None:
        if not self.state_file or not self.state_file.exists():
            return
        with self.state_file.open("r", encoding="utf-8") as file:
            data = json.load(file)
        self.current_term = int(data.get("current_term", 0))
        self.voted_for = data.get("voted_for")
        self.log = [LogEntry.from_dict(item) for item in data.get("log", [])]

    def _save_state(self) -> None:
        if not self.state_file:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [entry.to_dict() for entry in self.log],
        }
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{self.state_file.name}.",
            suffix=".tmp",
            dir=self.state_file.parent,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_name, self.state_file)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    # --- Utilitaires sur le journal (indexé à partir de 1) ---

    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def term_at(self, index: int) -> int:
        if index <= 0 or index > len(self.log):
            return 0
        return self.log[index - 1].term

    def entry_at(self, index: int) -> LogEntry | None:
        if index <= 0 or index > len(self.log):
            return None
        return self.log[index - 1]

    def cluster_size(self) -> int:
        return 1 + len(self.peer_ids)

    def quorum_size(self) -> int:
        return (self.cluster_size() // 2) + 1

    # --- Transitions de rôle ---

    def _step_down(self, new_term: int, leader_id: str | None = None) -> None:
        """Repasse à l'état FOLLOWER lorsqu'un terme supérieur est découvert."""
        self.current_term = new_term
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self.leader_id = leader_id
        self._save_state()
        self._reset_election_timer()

    def _become_leader(self) -> None:
        self.role = NodeRole.LEADER
        self.leader_id = self.node_id
        next_idx = self.last_log_index() + 1
        self.next_index = dict.fromkeys(self.peer_ids, next_idx)
        self.match_index = dict.fromkeys(self.peer_ids, 0)
        self._last_heartbeat_sent = 0.0

    # --- 1. Élection du Leader (RequestVote) ---

    def start_election(self) -> bool:
        """Lance une nouvelle élection. Retourne True si le nœud devient leader."""
        with self._lock:
            self.role = NodeRole.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self._save_state()
            self._reset_election_timer()

            current_term = self.current_term
            request = RequestVoteRequest(
                term=current_term,
                candidate_id=self.node_id,
                last_log_index=self.last_log_index(),
                last_log_term=self.last_log_term(),
            )
            peers = list(self.peer_ids)
            if 1 >= self.quorum_size():
                self._become_leader()
                return True

        if not self.transport:
            return False

        votes_received = 1  # Vote pour soi-même
        for peer_id in peers:
            response = self.transport.send_request_vote(peer_id, request)
            if response is None:
                continue
            votes_received, should_stop = self._process_vote_response(
                response, current_term, votes_received
            )
            if should_stop:
                break

        with self._lock:
            is_leader = self.role == NodeRole.LEADER

        if is_leader:
            self.send_heartbeats()
        return is_leader

    def _process_vote_response(
        self,
        response: RequestVoteResponse,
        election_term: int,
        votes_received: int,
    ) -> tuple[int, bool]:
        """Traite une réponse de vote et indique si la collecte de votes doit s'arrêter."""
        with self._lock:
            if response.term > self.current_term:
                self._step_down(response.term)
                return votes_received, True
            if self.role != NodeRole.CANDIDATE or self.current_term != election_term:
                return votes_received, True
            if response.vote_granted:
                votes_received += 1
                if votes_received >= self.quorum_size():
                    self._become_leader()
                    return votes_received, True
            return votes_received, False

    def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """Traite une demande de vote reçue d'un candidat."""
        with self._lock:
            if request.term < self.current_term:
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

            if request.term > self.current_term:
                self._step_down(request.term)

            can_vote = self.voted_for is None or self.voted_for == request.candidate_id
            log_up_to_date = self._is_candidate_log_up_to_date(
                request.last_log_index, request.last_log_term
            )

            if can_vote and log_up_to_date:
                self.voted_for = request.candidate_id
                self._save_state()
                self._reset_election_timer()
                return RequestVoteResponse(term=self.current_term, vote_granted=True)

            return RequestVoteResponse(term=self.current_term, vote_granted=False)

    def _is_candidate_log_up_to_date(
        self, candidate_last_index: int, candidate_last_term: int
    ) -> bool:
        """Vérifie la propriété de sécurité d'élection Raft (§5.4.1)."""
        my_last_term = self.last_log_term()
        my_last_index = self.last_log_index()
        if candidate_last_term != my_last_term:
            return candidate_last_term > my_last_term
        return candidate_last_index >= my_last_index

    # --- 2. Réplication de logs & Heartbeats (AppendEntries) ---

    def append_command(self, command: dict[str, Any]) -> LogEntry:
        """Ajoute une commande au log du leader et tente de la répliquer."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                raise RuntimeError(
                    f"Le nœud {self.node_id} n'est pas le leader (rôle={self.role.value})."
                )
            entry = LogEntry(
                term=self.current_term,
                index=self.last_log_index() + 1,
                command=dict(command),
            )
            self.log.append(entry)
            self._save_state()

            # Si le cluster ne comporte qu'un seul nœud, le commit est immédiat
            if not self.peer_ids:
                self._advance_commit_index()

        if self.peer_ids:
            self.replicate_to_peers()
        return entry

    def send_heartbeats(self) -> None:
        """Envoie des messages AppendEntries (heartbeats ou entrées manquantes) à tous les pairs."""
        self.replicate_to_peers()

    def replicate_to_peers(self) -> bool:
        """Réplique le journal vers tous les pairs et met à jour commit_index."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return False
            self._last_heartbeat_sent = time.monotonic()
            peers = list(self.peer_ids)

        if not peers:
            with self._lock:
                self._advance_commit_index()
                return True

        if not self.transport:
            return False

        for peer_id in peers:
            self._replicate_to_single_peer(peer_id)

        with self._lock:
            return self.commit_index >= self.last_log_index()

    def _replicate_to_single_peer(self, peer_id: str) -> None:
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
            next_idx = self.next_index.get(peer_id, self.last_log_index() + 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.term_at(prev_log_index)
            entries = list(self.log[prev_log_index:])
            request = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index,
            )
            sent_term = self.current_term

        response = self.transport.send_append_entries(peer_id, request) if self.transport else None
        if response is None:
            return

        with self._lock:
            if response.term > self.current_term:
                self._step_down(response.term)
                return
            if self.role != NodeRole.LEADER or self.current_term != sent_term:
                return

            if response.success:
                new_match = max(
                    self.match_index.get(peer_id, 0),
                    response.match_index,
                    prev_log_index + len(entries),
                )
                self.match_index[peer_id] = new_match
                self.next_index[peer_id] = new_match + 1
                self._advance_commit_index()
            else:
                # Recul de next_index en cas d'incohérence de journal
                current_next = self.next_index.get(peer_id, 1)
                self.next_index[peer_id] = max(1, current_next - 1)

    def handle_append_entries(
        self, request: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        """Traite un RPC AppendEntries reçu du leader (heartbeat ou réplication)."""
        with self._lock:
            if request.term < self.current_term:
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    match_index=self.commit_index,
                )

            if request.term > self.current_term or self.role != NodeRole.FOLLOWER:
                self._step_down(request.term, leader_id=request.leader_id)
            else:
                self.leader_id = request.leader_id
                self._reset_election_timer()

            if not self._has_matching_prev_log(request.prev_log_index, request.prev_log_term):
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    match_index=self.last_log_index(),
                )

            self._merge_log_entries(request.entries)

            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, self.last_log_index())
                self._apply_committed_entries()

            return AppendEntriesResponse(
                term=self.current_term,
                success=True,
                match_index=self.last_log_index(),
            )

    def _has_matching_prev_log(self, prev_log_index: int, prev_log_term: int) -> bool:
        """Vérifie la cohérence du log à prev_log_index (§5.3) et tronque en cas de conflit."""
        if prev_log_index <= 0:
            return True
        if prev_log_index > self.last_log_index():
            return False
        if self.term_at(prev_log_index) != prev_log_term:
            self.log = self.log[: prev_log_index - 1]
            self._save_state()
            return False
        return True

    def _merge_log_entries(self, entries: list[LogEntry]) -> None:
        """Insère les nouvelles entrées du leader en résolvant les conflits éventuels."""
        log_modified = False
        for entry in entries:
            existing = self.entry_at(entry.index)
            if existing is not None and existing.term != entry.term:
                self.log = self.log[: entry.index - 1]
                existing = None
                log_modified = True
            if existing is None:
                self.log.append(entry)
                log_modified = True
        if log_modified:
            self._save_state()

    # --- 3. Avancement du commit_index et application ---

    def _advance_commit_index(self) -> None:
        """Met à jour commit_index si un quorum a répliqué une entrée du terme courant."""
        for index in range(self.last_log_index(), self.commit_index, -1):
            if self.term_at(index) != self.current_term:
                continue
            replicated_count = 1  # Le leader possède déjà l'entrée
            for peer_id in self.peer_ids:
                if self.match_index.get(peer_id, 0) >= index:
                    replicated_count += 1
            if replicated_count >= self.quorum_size():
                self.commit_index = index
                break
        self._apply_committed_entries()

    def _apply_committed_entries(self) -> None:
        """Notifie les entrées nouvellement commises via on_commit_callback."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.entry_at(self.last_applied)
            if entry is not None and self.on_commit_callback is not None:
                self.on_commit_callback(entry)

    # --- 4. Gestion des battements de cœur et de la boucle temporelle ---

    def tick(self, now: float | None = None) -> None:
        """Exécute un cycle de vérification des timers (élection ou heartbeat)."""
        current_time = now if now is not None else time.monotonic()
        should_elect = False
        should_heartbeat = False

        with self._lock:
            if self.role == NodeRole.LEADER:
                if current_time - self._last_heartbeat_sent >= self.heartbeat_interval:
                    should_heartbeat = True
            else:
                if current_time - self._last_contact >= self._election_timeout:
                    should_elect = True

        if should_elect:
            self.start_election()
        elif should_heartbeat:
            self.send_heartbeats()

    def start(self, poll_interval: float = 0.02) -> None:
        """Démarre le thread d'arrière-plan gérant les heartbeats et les élections."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._reset_election_timer()
            self._thread = threading.Thread(
                target=self._run_loop,
                args=(poll_interval,),
                name=f"raft-{self.node_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """Arrête proprement le thread d'arrière-plan."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=1.0)

    def _run_loop(self, poll_interval: float) -> None:
        while True:
            with self._lock:
                if not self._running:
                    break
            self.tick()
            time.sleep(poll_interval)

    # --- 5. Dynamisme du cluster (ajout et retrait à chaud) ---

    def add_peer_local(self, node_id: str, url: str | None = None) -> None:
        """Ajoute dynamiquement un pair à la configuration locale du nœud Raft."""
        if not node_id:
            raise ValueError("L'identifiant du nœud ne peut pas être vide.")
        with self._lock:
            if node_id == self.node_id:
                return
            if node_id not in self.peer_ids:
                self.peer_ids.append(node_id)
            if self.role == NodeRole.LEADER:
                self.next_index.setdefault(node_id, 1)
                self.match_index.setdefault(node_id, 0)
            if self.transport is not None and hasattr(self.transport, "add_peer"):
                self.transport.add_peer(node_id, url)

    def remove_peer_local(self, node_id: str) -> None:
        """Retire dynamiquement un pair de la configuration locale du nœud Raft."""
        if not node_id:
            raise ValueError("L'identifiant du nœud ne peut pas être vide.")
        with self._lock:
            if node_id in self.peer_ids:
                self.peer_ids.remove(node_id)
            self.next_index.pop(node_id, None)
            self.match_index.pop(node_id, None)
            if self.transport is not None and hasattr(self.transport, "remove_peer"):
                self.transport.remove_peer(node_id)

    def status(self) -> dict[str, Any]:
        """Retourne un résumé de l'état courant du nœud Raft."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "role": self.role.value,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "voted_for": self.voted_for,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "peers": list(self.peer_ids),
            }
