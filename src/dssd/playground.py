"""Clusters of the real dssd code on an in-memory network, so they can run in
a browser tab (via Pyodide) or a test, and be poked at: kill a node, split the
network, drop packets. Nothing here re-implements an algorithm: swim.Node,
raft.Raft, ShardStateMachine and RegionServer are the production classes,
only the network under them is fake.

Controls are plain synchronous methods and every cluster reports through
`emit(json_string)` ten times a second, so a UI needs no asyncio knowledge.
"""

from __future__ import annotations

import asyncio
import json
import random
from dataclasses import asdict, dataclass

import grpc

from dssd import raft, regionpb
from dssd.sharding.handoff import RegionOwnerService
from dssd.sharding.region_server import RegionServer
from dssd.sharding.shard_state import ShardStateMachine
from dssd.sharding.world import AgentState, GridConfig, all_shard_ids
from dssd.swim import Config as SwimConfig
from dssd.swim import Node as SwimNode

SWIM_PORT = 7946


@dataclass
class Timing:
    """Seconds. HUMAN is slow enough to watch; FAST is for tests."""

    heartbeat: float = 0.4
    election_min: float = 1.5
    election_max: float = 3.0
    swim_period: float = 1.0
    ping_timeout: float = 0.4
    suspicion: float = 3.0
    resurrect: float = 3.0
    latency: float = 0.08
    tick: float = 0.5  # sharded world: one simulation step


HUMAN = Timing()
FAST = Timing(0.05, 0.2, 0.4, 0.1, 0.05, 0.3, 0.3, 0.005, 0.1)


class MemoryNetwork:
    """Delivers SWIM datagrams and Raft RPCs between named nodes, unless the
    sender or receiver is down, they're on opposite sides of a partition, or
    the packet is randomly lost. Every send is reported to `tap`."""

    def __init__(self, latency: float, tap_raft_heartbeats: bool = True) -> None:
        self.latency = latency
        self.loss = 0.0
        self.down: set[str] = set()
        self.side: dict[str, int] = {}  # node -> partition side; empty = no partition
        self.swim_inbox: dict[str, asyncio.DatagramProtocol] = {}
        self.raft: dict[str, dict[str, raft.Raft]] = {}  # node -> group -> Raft
        self.packets: list[dict] = []
        self._tap_raft_heartbeats = tap_raft_heartbeats
        self._seq = 0

    def delivers(self, src: str, dst: str) -> bool:
        reachable = src not in self.down and dst not in self.down and self.side.get(src, 0) == self.side.get(dst, 0)
        return reachable and random.random() >= self.loss

    def tap(self, plane: str, kind: str, src: str, dst: str, ok: bool, group: str = "") -> None:
        if plane == "raft" and not self._tap_raft_heartbeats and kind in ("heartbeat", "reply"):
            return
        self._seq += 1
        self.packets.append({"i": self._seq, "p": plane, "k": kind, "a": src, "b": dst, "ok": ok, "g": group})
        del self.packets[:-300]

    def swim_listener(self, node_id: str):
        async def listen(protocol):
            self.swim_inbox[node_id] = protocol
            return _Endpoint(self, node_id)

        return listen

    def raft_transport(self, node_id: str, group: str = "") -> _RaftTransport:
        return _RaftTransport(self, node_id, group)


class _Endpoint:
    """The slice of asyncio.DatagramTransport that swim.Node uses."""

    def __init__(self, net: MemoryNetwork, me: str) -> None:
        self._net, self._me = net, me

    def sendto(self, data: bytes, addr) -> None:
        dst = addr[0]
        ok = self._net.delivers(self._me, dst)
        self._net.tap("swim", json.loads(data)["type"], self._me, dst, ok)
        if ok:
            asyncio.get_running_loop().call_later(self._net.latency, self._arrive, dst, data)

    def _arrive(self, dst: str, data: bytes) -> None:
        inbox = self._net.swim_inbox.get(dst)
        if inbox is not None and dst not in self._net.down:
            inbox.datagram_received(data, (self._me, SWIM_PORT))  # type: ignore[attr-defined]

    def close(self) -> None:
        self._net.swim_inbox.pop(self._me, None)

    def get_extra_info(self, name: str):
        return (self._me, SWIM_PORT)


def _copy(message):
    """Round-trip through the wire format, like a real RPC would."""
    return type(message).FromString(message.SerializeToString())


class _RaftTransport:
    """raft.Transport over the memory network. A failed delivery raises, like an unreachable peer."""

    def __init__(self, net: MemoryNetwork, me: str, group: str) -> None:
        self._net, self._me, self._group = net, me, group

    async def request_vote(self, peer_id, args):
        return await self._rpc("vote", peer_id, args, "handle_request_vote")

    async def append_entries(self, peer_id, args):
        return await self._rpc("append" if args.entries else "heartbeat", peer_id, args, "handle_append_entries")

    async def _rpc(self, kind: str, peer: str, args, handler: str):
        net = self._net
        ok = net.delivers(self._me, peer)
        net.tap("raft", kind, self._me, peer, ok, self._group)
        if not ok:
            raise ConnectionError(f"{self._me} -> {peer}")
        await asyncio.sleep(net.latency)
        reply = getattr(net.raft[peer][self._group], handler)(_copy(args))
        ok = net.delivers(peer, self._me)
        net.tap("raft", "reply", peer, self._me, ok, self._group)
        if not ok:
            raise ConnectionError(f"{peer} -> {self._me}")
        await asyncio.sleep(net.latency)
        return _copy(reply)


class _Cluster:
    """What Consensus and World share: a clock, an event feed, and a 10 Hz report."""

    def __init__(self, emit, timing: Timing) -> None:
        self._emit = emit
        self.timing = timing
        self.net = MemoryNetwork(timing.latency)
        self._events: list[dict] = []
        self._t0 = 0.0
        self._task: asyncio.Task | None = None
        self.ready = False  # True once the nodes exist; snapshot() is only safe after that

    def start(self) -> None:
        """Safe to call twice; the cluster boots in the background."""
        if self._task is None:
            self._t0 = asyncio.get_running_loop().time()
            self._task = asyncio.ensure_future(self._run())

    def stop(self) -> None:
        asyncio.ensure_future(self._shutdown())

    def reset(self) -> None:
        async def again():
            await self._shutdown()
            self.__init__(self._emit, self.timing)  # type: ignore[misc]
            self.start()

        asyncio.ensure_future(again())

    def set_loss(self, fraction: float) -> None:
        self.net.loss = max(0.0, min(0.9, fraction))

    def set_latency(self, seconds: float) -> None:
        self.net.latency = max(0.0, min(0.3, seconds))

    def partition(self, groups: list[list[str]] | None) -> None:
        """Splits the network into the given groups of node ids; None heals it."""
        self.net.side = {n: i for i, group in enumerate(groups or []) for n in group}
        self.note("user", "network healed" if not groups else "network split: " + " | ".join(",".join(g) for g in groups))

    def note(self, kind: str, text: str) -> None:
        self._events.append({"t": self.now(), "k": kind, "text": text})

    def now(self) -> float:
        return round(asyncio.get_running_loop().time() - self._t0, 2)

    async def _run(self) -> None:
        await self._boot()
        self.ready = True
        while True:
            await asyncio.sleep(0.1)
            self._watch()
            self._emit(json.dumps(self.snapshot()))

    def _drain(self) -> dict:
        events, self._events = self._events, []
        packets, self.net.packets = self.net.packets, []
        return {"t": self.now(), "events": events, "packets": packets,
                "net": {"loss": self.net.loss, "latency": self.net.latency, "side": self.net.side}}

    # Each cluster supplies these.
    async def _boot(self) -> None:
        raise NotImplementedError

    async def _shutdown(self) -> None:
        raise NotImplementedError

    def _watch(self) -> None:
        raise NotImplementedError

    def snapshot(self) -> dict:
        raise NotImplementedError


class Consensus(_Cluster):
    """Five nodes, each a swim.Node (who is alive?) plus a raft.Raft (who leads, what's committed?)."""

    def __init__(self, emit, timing: Timing = HUMAN, size: int = 5) -> None:
        super().__init__(emit, timing)
        self.ids = [f"n{i + 1}" for i in range(size)]
        self.swim: dict[str, SwimNode] = {}
        self.raft: dict[str, raft.Raft] = {}
        self._last_roles: dict[str, str] = {}
        self._last_trouble: dict[str, set[str]] = {i: set() for i in self.ids}
        self._reported_dead: set[str] = set()

    async def _boot(self) -> None:
        t = self.timing
        for nid in self.ids:
            cfg = SwimConfig(id=nid, protocol_period=t.swim_period, ping_timeout=t.ping_timeout, indirect_ping_count=2,
                             suspicion_timeout=t.suspicion, resurrect_interval=t.resurrect)
            self.swim[nid] = SwimNode(cfg, listen=self.net.swim_listener(nid))
            peers = [p for p in self.ids if p != nid]
            rcfg = raft.Config(id=nid, peers=peers, election_timeout_min=t.election_min,
                               election_timeout_max=t.election_max, heartbeat_interval=t.heartbeat)
            self.raft[nid] = raft.Raft(rcfg, self.net.raft_transport(nid), apply=lambda entry: None)
            self.net.raft[nid] = {"": self.raft[nid]}
        for nid in self.ids:
            await self.swim[nid].start()
        for nid in self.ids[1:]:
            await self.swim[nid].join(f"{self.ids[0]}:{SWIM_PORT}")
        for nid in self.ids:
            await self.raft[nid].start()

    async def _shutdown(self) -> None:
        self.ready = False
        if self._task:
            self._task.cancel()
        self._task = None
        for nid in list(self.swim):
            await self.swim[nid].stop()
            await self.raft[nid].stop()

    # --- controls ---

    def kill(self, nid: str) -> None:
        asyncio.ensure_future(self._kill(nid))

    def revive(self, nid: str) -> None:
        asyncio.ensure_future(self._revive(nid))

    def kill_leader(self) -> None:
        leader = self._leader()
        if leader:
            self.kill(leader)

    def write(self, text: str) -> None:
        leader = self._leader()
        if leader and leader not in self.net.down:
            index, term, _ = self.raft[leader].propose(text.encode())
            self.note("write", f"{leader} accepted write “{text}” (entry {index}, term {term})")
        else:
            self.note("write", f"write “{text}” rejected: no leader yet")

    async def _kill(self, nid: str) -> None:
        if nid in self.net.down:
            return
        self.net.down.add(nid)
        await self.swim[nid].stop()
        await self.raft[nid].stop()
        self.note("user", f"{nid} crashed")

    async def _revive(self, nid: str) -> None:
        if nid not in self.net.down:
            return
        self.net.down.discard(nid)
        await self.swim[nid].start()
        await self.raft[nid].start()
        self.note("user", f"{nid} restarted as a follower")

    def _leader(self) -> str | None:
        leaders = [(self.raft[n].state()[0], n) for n in self.ids if n not in self.net.down and self.raft[n].state()[1]]
        return max(leaders)[1] if leaders else None

    # --- reporting ---

    def _views(self, nid: str) -> dict[str, str]:
        return {m.id: m.state.name.lower() for m in self.swim[nid].members() if m.id != nid}

    def _watch(self) -> None:
        """Turns changes in the real objects into feed lines."""
        for nid in self.ids:
            r = self.raft[nid]
            role = r.role.value if nid not in self.net.down else "down"
            if self._last_roles.get(nid) != role:
                term = r.state()[0]
                if role == "leader":
                    self.note("leader", f"{nid} won the election and leads term {term}")
                elif role == "candidate":
                    self.note("election", f"{nid} timed out and started an election for term {term}")
                elif self._last_roles.get(nid) == "leader" and role == "follower":
                    self.note("stepdown", f"{nid} stepped down (saw a newer term)")
                self._last_roles[nid] = role
        up = [n for n in self.ids if n not in self.net.down]
        for target in self.ids:
            suspecting = {v for v in up if v != target and self._views(v).get(target) in ("suspect", "dead")}
            before = self._last_trouble[target]
            if suspecting and not before:
                self.note("suspect", f"{sorted(suspecting)[0]} suspects {target}: no answer to ping or ping-req")
            dead = {v for v in suspecting if self._views(v).get(target) == "dead"}
            if dead and target not in self._reported_dead:
                self._reported_dead.add(target)
                self.note("dead", f"{sorted(dead)[0]} declared {target} dead after the suspicion timeout")
            if not suspecting and before and target not in self.net.down:
                self._reported_dead.discard(target)
                self.note("recover", f"{target} is back: everyone sees it alive again")
            self._last_trouble[target] = suspecting

    def snapshot(self) -> dict:
        nodes = []
        for nid in self.ids:
            r = self.raft[nid]
            term, _ = r.state()
            nodes.append({
                "id": nid,
                "up": nid not in self.net.down,
                "role": r.role.value,
                "term": term,
                "leader": r.leader_hint()[0],
                "commit": r.commit_index(),
                "last": r.last_index(),
                "log": [[e.term, e.command.decode() or "·"] for e in r.entries()[-14:]],
                "swim": self._views(nid),
            })
        return {**self._drain(), "nodes": nodes, "leader": self._leader()}


class World(_Cluster):
    """Three region servers share a 2x2 grid of shards. Each shard is its own Raft group
    (so its leader owns that region), and agents hand off between shards as they roam."""

    GRID = GridConfig(width=20.0, height=20.0, cols=2, rows=2)

    def __init__(self, emit, timing: Timing = HUMAN, agents: int = 8) -> None:
        super().__init__(emit, timing)
        self.net = MemoryNetwork(timing.latency, tap_raft_heartbeats=False)
        self.ids = ["r0", "r1", "r2"]
        self.shard_ids = all_shard_ids(self.GRID)
        self.shards: dict[str, dict[str, ShardStateMachine]] = {}
        self.regions: dict[str, RegionServer] = {}
        self.region_tasks: dict[str, asyncio.Task] = {}
        self._agent_count = agents
        self._expected: set[str] = set()
        self.spawned = False  # True once every starting agent is durably placed
        self._last_leaders: dict[str, str] = {}

    async def _boot(self) -> None:
        t = self.timing
        for nid in self.ids:
            peers = [p for p in self.ids if p != nid]
            self.shards[nid] = {}
            self.net.raft[nid] = {}
            for sid in self.shard_ids:
                cfg = raft.Config(id=nid, peers=peers, group=sid, election_timeout_min=t.election_min / 2,
                                  election_timeout_max=t.election_max / 2, heartbeat_interval=t.heartbeat / 2)
                sm = ShardStateMachine(cfg, self.net.raft_transport(nid, sid))
                self.shards[nid][sid] = sm
                self.net.raft[nid][sid] = sm.raft
            self.regions[nid] = RegionServer(self.GRID, self.shards[nid], {i: i for i in self.ids},
                                             tick_interval=t.tick, handoff=self._handoff_from(nid))
        for nid in self.ids:
            for sm in self.shards[nid].values():
                await sm.start()
            self.region_tasks[nid] = asyncio.ensure_future(self.regions[nid].run())
        asyncio.ensure_future(self._spawn_agents())  # in the background: elections are part of the show

    async def _spawn_agents(self) -> None:
        rng = random.Random(7)
        for i in range(self._agent_count):
            agent = AgentState(f"a{i + 1}", rng.uniform(1, 19), rng.uniform(1, 19),
                               rng.choice([-1, 1]) * rng.uniform(0.5, 1.5), rng.choice([-1, 1]) * rng.uniform(0.5, 1.5))
            placed = False
            while not placed:  # shard leaders may still be electing
                for n in self.ids:
                    if n not in self.net.down and await self.regions[n].spawn_agent(agent):
                        placed = True
                        break
                else:
                    await asyncio.sleep(0.2)
            self._expected.add(agent.id)  # only agents that durably exist count towards "lost"
        self.spawned = True

    def _handoff_from(self, me: str):
        async def handoff(dest: str, shard_id: str, term: int, agent: AgentState):
            ok = self.net.delivers(me, dest)
            self.net.tap("handoff", "handoff", me, dest, ok, shard_id)
            if not ok:
                raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(), grpc.aio.Metadata())
            await asyncio.sleep(self.net.latency)
            request = regionpb.HandOffRequest(shard_id=shard_id, term=term, agent=regionpb.AgentState(**asdict(agent)))
            reply = await RegionOwnerService(self.shards[dest]).HandOff(request, None)
            await asyncio.sleep(self.net.latency)
            return reply.accepted, reply.reason

        return handoff

    async def _shutdown(self) -> None:
        self.ready = False
        if self._task:
            self._task.cancel()
        self._task = None
        for task in self.region_tasks.values():
            task.cancel()
        for shards in self.shards.values():
            for sm in shards.values():
                await sm.stop()

    # --- controls ---

    def kill(self, nid: str) -> None:
        asyncio.ensure_future(self._kill(nid))

    def revive(self, nid: str) -> None:
        asyncio.ensure_future(self._revive(nid))

    def kill_busiest(self) -> None:
        busiest = max(self.ids, key=lambda n: sum(sm.is_leader() for sm in self.shards[n].values()) - 99 * (n in self.net.down))
        self.kill(busiest)

    async def _kill(self, nid: str) -> None:
        if nid in self.net.down:
            return
        self.net.down.add(nid)
        self.region_tasks[nid].cancel()
        for sm in self.shards[nid].values():
            await sm.stop()
        self.note("user", f"{nid} crashed")

    async def _revive(self, nid: str) -> None:
        if nid not in self.net.down:
            return
        self.net.down.discard(nid)
        for sm in self.shards[nid].values():
            await sm.start()
        self.region_tasks[nid] = asyncio.ensure_future(self.regions[nid].run())
        self.note("user", f"{nid} restarted and is catching up from its peers")

    # --- reporting ---

    def _leader_of(self, sid: str) -> tuple[str | None, int]:
        best = [(self.shards[n][sid].raft.state()[0], n) for n in self.ids
                if n not in self.net.down and self.shards[n][sid].is_leader()]
        return (max(best)[1], max(best)[0]) if best else (None, 0)

    def _watch(self) -> None:
        for sid in self.shard_ids:
            leader, term = self._leader_of(sid)
            if (leader or "") != self._last_leaders.get(sid, ""):
                if leader:
                    self.note("leader", f"{leader} now owns shard {sid} (term {term})")
                else:
                    self.note("election", f"shard {sid} has no leader: election in progress")
                self._last_leaders[sid] = leader or ""

    def snapshot(self) -> dict:
        shards: list[dict] = []
        seen: set[str] = set()
        chosen: list[tuple[str, list[AgentState]]] = []
        for sid in self.shard_ids:
            leader, term = self._leader_of(sid)
            # Show the leader's view; with no leader, the freshest replica still holding state.
            replicas = [self.shards[n][sid] for n in self.ids if n not in self.net.down]
            source = self.shards[leader][sid] if leader else max(replicas, key=lambda s: s.raft.last_applied(), default=None)
            agents = list(source.agents.values()) if source else []
            chosen.append((sid, agents))
            seen.update(a.id for n in self.ids for a in self.shards[n][sid].agents.values())  # crashed nodes keep their state
            shards.append({"id": sid, "leader": leader, "term": term})
        held: dict[str, int] = {}
        for _, agents in chosen:
            for a in agents:
                held[a.id] = held.get(a.id, 0) + 1
        return {
            **self._drain(),
            "nodes": [{"id": n, "up": n not in self.net.down,
                       "leads": [s for s in self.shard_ids if n not in self.net.down and self.shards[n][s].is_leader()]}
                      for n in self.ids],
            "shards": shards,
            "agents": [{"id": a.id, "x": round(a.x, 2), "y": round(a.y, 2), "shard": sid} for sid, agents in chosen for a in agents],
            "stats": {"total": len(self._expected), "lost": len(self._expected - seen),
                      "duplicated": sum(1 for c in held.values() if c > 1)},
            "grid": {"w": self.GRID.width, "h": self.GRID.height, "cols": self.GRID.cols, "rows": self.GRID.rows},
        }
