"""Sharding end to end: three region servers talking real gRPC on localhost,
each one a Raft member of every shard."""

import asyncio
import time
from dataclasses import dataclass

import grpc

from dssd import raft, regionpb, spinepb
from dssd.membership import GRPCTransport, RaftService
from dssd.sharding.handoff import RegionOwnerService, request_handoff
from dssd.sharding.region_server import RegionServer
from dssd.sharding.shard_state import ShardStateMachine
from dssd.sharding.world import AgentState, GridConfig, all_shard_ids
from tests.support import eventually

NODE_IDS = ["n0", "n1", "n2"]


@dataclass
class Node:
    id: str
    addr: str
    server: grpc.aio.Server
    transport: GRPCTransport
    shards: dict[str, ShardStateMachine]
    region: RegionServer
    run_task: asyncio.Task | None = None
    stopped: bool = False

    async def stop(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        if self.run_task is not None:
            self.run_task.cancel()
            await asyncio.gather(self.run_task, return_exceptions=True)
        await self.server.stop(None)
        for sm in self.shards.values():
            await sm.stop()
        await self.transport.close()


async def start_cluster(grid: GridConfig, run: bool = False) -> list[Node]:
    """run=True starts each node's tick loop; otherwise tests call tick() themselves."""
    servers = {nid: grpc.aio.server() for nid in NODE_IDS}
    addrs = {nid: f"127.0.0.1:{s.add_insecure_port('127.0.0.1:0')}" for nid, s in servers.items()}
    nodes = []
    for nid, server in servers.items():
        peers = [p for p in NODE_IDS if p != nid]
        transport = GRPCTransport({p: addrs[p] for p in peers})
        shards = {
            sid: ShardStateMachine(raft.Config(id=nid, peers=peers, group=sid), transport)
            for sid in all_shard_ids(grid)
        }
        spinepb.add_RaftServicer_to_server(RaftService({sid: sm.raft for sid, sm in shards.items()}), server)
        regionpb.add_RegionOwnerServicer_to_server(RegionOwnerService(shards), server)
        node = Node(nid, addrs[nid], server, transport, shards, RegionServer(grid, shards, addrs, tick_interval=0.05))
        nodes.append(node)
        await server.start()
        for sm in shards.values():
            await sm.start()
        if run:
            node.run_task = asyncio.create_task(node.region.run())
    return nodes


async def stop_cluster(nodes: list[Node]) -> None:
    await asyncio.gather(*(n.stop() for n in nodes))


def leader(nodes: list[Node], shard_id: str) -> Node | None:
    return next((n for n in nodes if not n.stopped and n.shards[shard_id].is_leader()), None)


def agents_held(nodes: list[Node], grid: GridConfig) -> set[str]:
    """Agent ids held by each shard's current leader (the authoritative copy)."""
    return {a for sid in all_shard_ids(grid) if (n := leader(nodes, sid)) for a in n.shards[sid].agents}


async def settle(nodes: list[Node], grid: GridConfig) -> None:
    """Waits until every shard has a leader and every node knows who it is
    (a hand-off is deferred while the sender doesn't know the destination's leader)."""
    await eventually(
        lambda: all(leader(nodes, sid) for sid in all_shard_ids(grid))
        and all(n.shards[sid].raft.leader_hint()[0] for n in nodes for sid in all_shard_ids(grid))
    )


async def spawn(nodes: list[Node], agent: AgentState) -> None:
    async def placed() -> bool:
        for n in nodes:
            if await n.region.spawn_agent(agent):  # only the shard's leader accepts
                return True
        return False

    await eventually(placed)


async def test_each_shard_elects_its_own_leader():
    grid = GridConfig(width=20, height=20, cols=2, rows=2)
    nodes = await start_cluster(grid)
    try:
        await eventually(
            lambda: all(sum(n.shards[sid].is_leader() for n in nodes) == 1 for sid in all_shard_ids(grid))
        )
    finally:
        await stop_cluster(nodes)


async def test_handoff_is_fenced_by_the_destination_leader_and_term():
    grid = GridConfig(width=10, height=10, cols=1, rows=1)
    nodes = await start_cluster(grid)
    try:
        await settle(nodes, grid)
        dest = leader(nodes, "0-0")
        follower = next(n for n in nodes if n is not dest)
        term, _ = dest.shards["0-0"].raft.state()

        accepted, reason = await request_handoff(follower.addr, "0-0", term, AgentState("x", 1, 1, 0, 0))
        assert not accepted and "leader" in reason

        accepted, reason = await request_handoff(dest.addr, "0-0", term - 1, AgentState("y", 1, 1, 0, 0))
        assert not accepted and "term" in reason

        accepted, reason = await request_handoff(dest.addr, "0-0", term, AgentState("a", 1, 1, 0, 0))
        assert accepted, reason
        # Accepted means committed, so every replica ends up with it (and nothing rejected).
        await eventually(lambda: all(n.shards["0-0"].agents.keys() == {"a"} for n in nodes))
    finally:
        await stop_cluster(nodes)


async def test_no_agent_loss_when_the_busiest_region_server_dies():
    grid = GridConfig(width=20, height=20, cols=2, rows=2)
    nodes = await start_cluster(grid, run=True)
    try:
        await settle(nodes, grid)
        # Stationary agents, one per shard: this isolates "does a dead
        # leader's replicated state survive, and how fast is a new leader ready".
        for agent_id, x, y in [("a1", 5, 5), ("a2", 15, 15), ("a3", 2, 18), ("a4", 18, 2)]:
            await spawn(nodes, AgentState(agent_id, x, y, 0, 0))
        everyone = {"a1", "a2", "a3", "a4"}
        await eventually(lambda: agents_held(nodes, grid) == everyone)

        victim = max(nodes, key=lambda n: sum(sm.is_leader() for sm in n.shards.values()))
        start = time.monotonic()
        await victim.stop()
        await eventually(
            lambda: all(leader(nodes, sid) for sid in all_shard_ids(grid)) and agents_held(nodes, grid) == everyone
        )
        elapsed = time.monotonic() - start
        print(f"recovered in {elapsed:.3f}s")
        # The README's target is <1s; the bound is looser so a loaded CI box doesn't flake.
        assert elapsed <= 3.0, f"recovery took {elapsed:.3f}s"
    finally:
        await stop_cluster(nodes)


async def test_agent_crossing_a_boundary_is_handed_off():
    grid = GridConfig(width=20, height=10, cols=2, rows=1)
    nodes = await start_cluster(grid)
    try:
        await settle(nodes, grid)
        await spawn(nodes, AgentState("a", x=9.5, y=5, vx=1, vy=0))  # one step from 0-1

        async def handed_off() -> bool:
            await leader(nodes, "0-0").region.tick()
            assert agents_held(nodes, grid) == {"a"}, "agent lost or duplicated"
            return "a" in leader(nodes, "0-1").shards["0-1"].agents

        await eventually(handed_off)
        assert "a" not in leader(nodes, "0-0").shards["0-0"].agents
    finally:
        await stop_cluster(nodes)


async def test_two_agents_leaving_one_shard_in_the_same_tick():
    # Guards RegionServer.tick phase 2: removing the first departed agent
    # must not bring back (or drop) the second one.
    grid = GridConfig(width=30, height=10, cols=3, rows=1)
    nodes = await start_cluster(grid)
    try:
        await settle(nodes, grid)
        await spawn(nodes, AgentState("left", x=10.5, y=5, vx=-1, vy=0))  # -> 0-0
        await spawn(nodes, AgentState("right", x=19.5, y=5, vx=1, vy=0))  # -> 0-2

        source = leader(nodes, "0-1").shards["0-1"]
        await leader(nodes, "0-1").region.tick()  # one tick: both cross and both are handed off
        assert await source.wait_caught_up()  # every snapshot this tick proposed is now applied
        assert not source.agents
        assert "left" in leader(nodes, "0-0").shards["0-0"].agents
        assert "right" in leader(nodes, "0-2").shards["0-2"].agents
    finally:
        await stop_cluster(nodes)


class _AgreeablePeers:
    """A fake transport whose peers grant every vote and accept every entry."""

    async def request_vote(self, peer_id, args):
        return spinepb.RequestVoteReply(term=args.term, vote_granted=True)

    async def append_entries(self, peer_id, args):
        return spinepb.AppendEntriesReply(term=args.term, success=True)


async def test_leader_refuses_to_snapshot_until_its_log_is_applied():
    # A just-elected leader has committed entries it hasn't applied yet,
    # so its in-memory agents are stale. Snapshotting from that state
    # would erase the unapplied entries: an agent lost on failover.
    cfg = raft.Config(
        id="n", peers=["p1", "p2"], election_timeout_min=0.02, election_timeout_max=0.04, heartbeat_interval=0.01
    )
    sm = ShardStateMachine(cfg, _AgreeablePeers())
    grid = GridConfig(width=10, height=10, cols=1, rows=1)
    region = RegionServer(grid, {"0-0": sm}, {})
    await sm.raft.start()  # Raft only: without the apply loop nothing is ever applied
    try:
        await eventually(sm.is_leader)
        await eventually(lambda: not sm.caught_up())  # the election no-op is committed but unapplied

        before = sm.raft.last_index()
        assert not await region.spawn_agent(AgentState("a", 1, 1, 0, 0))
        assert sm.raft.last_index() == before, "proposed a snapshot from stale state"

        sm._apply_task = asyncio.create_task(sm._apply_loop())  # now let entries apply
        await eventually(sm.caught_up)
        assert await region.spawn_agent(AgentState("a", 1, 1, 0, 0))
    finally:
        await sm.stop()
