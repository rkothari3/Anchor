import asyncio

import pytest

from dssd.raft import Config, Raft
from dssd.spinepb import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
)
from tests.support import eventually


class FakeNetwork:
    """An in-memory Transport backing that lets tests simulate partitions
    by cutting connectivity between named nodes."""

    def __init__(self) -> None:
        self.nodes: dict[str, Raft] = {}
        self.connected: dict[str, dict[str, bool]] = {}

    def register(self, node_id: str, node: Raft) -> None:
        self.nodes[node_id] = node

    def allowed(self, a: str, b: str) -> bool:
        return self.connected.get(a, {}).get(b, True)

    def isolate(self, node_id: str) -> None:
        for other in self.nodes:
            if other == node_id:
                continue
            self.connected.setdefault(node_id, {})[other] = False
            self.connected.setdefault(other, {})[node_id] = False

    def connect(self, a: str, b: str) -> None:
        self.connected.setdefault(a, {})[b] = True
        self.connected.setdefault(b, {})[a] = True

    def heal_all(self) -> None:
        for a in self.nodes:
            for b in self.nodes:
                if a != b:
                    self.connected.setdefault(a, {})[b] = True


class FakeTransport:
    def __init__(self, self_id: str, network: FakeNetwork) -> None:
        self._id = self_id
        self._net = network

    async def request_vote(self, peer_id: str, args: RequestVoteArgs) -> RequestVoteReply:
        if not self._net.allowed(self._id, peer_id):
            raise ConnectionError("disconnected")
        return self._net.nodes[peer_id].handle_request_vote(args)

    async def append_entries(self, peer_id: str, args: AppendEntriesArgs) -> AppendEntriesReply:
        if not self._net.allowed(self._id, peer_id):
            raise ConnectionError("disconnected")
        return self._net.nodes[peer_id].handle_append_entries(args)


def make_config(node_id: str, peers: list[str]) -> Config:
    return Config(
        id=node_id,
        peers=peers,
        election_timeout_min=0.04,
        election_timeout_max=0.08,
        heartbeat_interval=0.01,
    )


class Cluster:
    def __init__(self, n: int) -> None:
        self.network = FakeNetwork()
        self.ids = [f"n{i}" for i in range(n)]
        self.nodes: dict[str, Raft] = {}
        self.apply_queues: dict[str, asyncio.Queue[LogEntry]] = {}

        for node_id in self.ids:
            peers = [p for p in self.ids if p != node_id]
            queue: asyncio.Queue[LogEntry] = asyncio.Queue()
            raft = Raft(make_config(node_id, peers), FakeTransport(node_id, self.network), queue.put_nowait)
            self.network.register(node_id, raft)
            self.nodes[node_id] = raft
            self.apply_queues[node_id] = queue

    async def start(self) -> None:
        for node in self.nodes.values():
            await node.start()

    async def stop(self) -> None:
        await asyncio.gather(*(n.stop() for n in self.nodes.values()))

    async def leader(self) -> Raft:
        await eventually(lambda: any(n.state()[1] for n in self.nodes.values()))
        # A deposed leader can briefly coexist with a newer-term one,
        # so return the leader of the highest term.
        return max((n for n in self.nodes.values() if n.state()[1]), key=lambda n: n.state()[0])


async def test_single_leader_elected():
    cluster = Cluster(3)
    await cluster.start()
    try:
        leader = await cluster.leader()
        term, _ = leader.state()
        assert term > 0
    finally:
        await cluster.stop()


async def test_no_two_leaders_in_same_term():
    cluster = Cluster(5)
    await cluster.start()
    try:
        await cluster.leader()
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 0.3
        while loop.time() < deadline:
            terms_seen: dict[int, int] = {}
            for node in cluster.nodes.values():
                term, is_leader = node.state()
                if is_leader:
                    terms_seen[term] = terms_seen.get(term, 0) + 1
            for term, count in terms_seen.items():
                assert count <= 1, f"term {term} has {count} leaders"
            await asyncio.sleep(0.005)
    finally:
        await cluster.stop()


async def test_log_replication():
    cluster = Cluster(3)
    await cluster.start()
    try:
        leader = await cluster.leader()
        index, _, ok = leader.propose(b"hello")
        assert ok

        for queue in cluster.apply_queues.values():
            # Every new leader also proposes an empty no-op on election
            # (see raft.Raft._become_leader), so "hello" isn't
            # necessarily the first message on the queue - skip past it.
            msg = await asyncio.wait_for(queue.get(), timeout=2.0)
            while msg.command == b"":
                msg = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert msg.index == index
            assert msg.command == b"hello"
    finally:
        await cluster.stop()


async def test_new_leader_elected_after_failure():
    cluster = Cluster(3)
    await cluster.start()
    try:
        first = await cluster.leader()
        first_id = first.cfg.id
        cluster.network.isolate(first_id)

        await eventually(lambda: any(nid != first_id and n.state()[1] for nid, n in cluster.nodes.items()))
    finally:
        cluster.network.heal_all()
        await cluster.stop()


async def test_minority_partition_cannot_commit():
    cluster = Cluster(5)
    await cluster.start()
    try:
        leader = await cluster.leader()
        leader_id = leader.cfg.id
        partner_id = next(nid for nid in cluster.ids if nid != leader_id)

        cluster.network.isolate(leader_id)
        cluster.network.isolate(partner_id)
        cluster.network.connect(leader_id, partner_id)

        _, _, is_leader = leader.propose(b"should-not-commit")
        if not is_leader:
            return  # leader stepped down; also proves the minority can't operate

        # The leader's election no-op may already be on the queue; only
        # "should-not-commit" must never show up.
        queue = cluster.apply_queues[leader_id]
        with pytest.raises(TimeoutError):
            while True:
                entry = await asyncio.wait_for(queue.get(), timeout=0.3)
                assert entry.command != b"should-not-commit"
    finally:
        cluster.network.heal_all()
        await cluster.stop()


async def test_append_entries_truncates_conflicting_suffix():
    applied: list[LogEntry] = []
    follower = Raft(make_config("f", []), FakeTransport("f", FakeNetwork()), applied.append)
    await follower.start()
    try:
        # A follower accepts an entry from a leader that is about to be deposed...
        stale = [LogEntry(term=1, index=1, command=b"stale")]
        args = AppendEntriesArgs(term=1, leader_id="old", prev_log_index=0, prev_log_term=0, entries=stale)
        assert follower.handle_append_entries(args).success

        # ...then the new leader (term 2) overwrites that index and commits it.
        # Log matching says the follower must drop its conflicting entry, so
        # what gets applied at index 1 is "fresh", never "stale".
        fresh = [LogEntry(term=2, index=1, command=b"fresh")]
        args = AppendEntriesArgs(term=2, leader_id="new", prev_log_index=0, prev_log_term=0, entries=fresh, leader_commit=1)
        assert follower.handle_append_entries(args).success

        await eventually(lambda: len(applied) == 1)
        assert applied[0].command == b"fresh"
        assert follower.last_index() == 1
    finally:
        await follower.stop()
