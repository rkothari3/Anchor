import asyncio

from dssd.swim import Config, EventType, Member, Node, State
from tests.support import eventually


def make_config(id: str) -> Config:
    return Config(
        id=id,
        protocol_period=0.02,
        ping_timeout=0.015,
        indirect_ping_count=2,
        suspicion_timeout=0.06,
        resurrect_interval=0.03,
    )


async def new_cluster(n: int) -> list[Node]:
    nodes = [Node(make_config(chr(ord("a") + i))) for i in range(n)]
    for node in nodes:
        await node.start()
    for node in nodes[1:]:
        await node.join(nodes[0].addr)
    return nodes


async def stop_all(nodes: list[Node]) -> None:
    await asyncio.gather(*(n.stop() for n in nodes))


def count_alive(members: list[Member]) -> int:
    return sum(1 for m in members if m.state == State.ALIVE)


async def test_join_converges():
    nodes = await new_cluster(4)
    try:
        for node in nodes:
            await eventually(lambda n=node: count_alive(n.members()) == len(nodes))
    finally:
        await stop_all(nodes)


async def test_failure_detection():
    nodes = await new_cluster(3)
    try:
        for node in nodes:
            await eventually(lambda n=node: count_alive(n.members()) == len(nodes))

        victim = nodes[2]
        victim_id = victim.id
        await victim.stop()

        survivor = nodes[0]
        found = False
        try:
            async with asyncio.timeout(2.0):
                while not found:
                    evt = await survivor.events.get()
                    found = evt.type == EventType.FAILED and evt.member.id == victim_id
        except TimeoutError:
            pass
        assert found, "survivor never observed a FAILED event for the victim"
    finally:
        await stop_all(nodes)


async def test_leave_is_fast():
    nodes = await new_cluster(3)
    try:
        for node in nodes:
            await eventually(lambda n=node: count_alive(n.members()) == len(nodes))

        leaver = nodes[2]
        leaver_id = leaver.id
        leaver.leave()

        survivor = nodes[0]

        def leaver_is_dead() -> bool:
            return any(m.id == leaver_id and m.state == State.DEAD for m in survivor.members())

        await eventually(leaver_is_dead, timeout=0.5)
    finally:
        await stop_all(nodes)


async def test_refutation_keeps_live_member_alive():
    node = Node(make_config("a"))
    await node.start()
    try:
        false_suspicion = Member(id="a", addr=node.addr, state=State.SUSPECT, incarnation=0)
        node._merge_update(false_suspicion)

        assert node._incarnation > 0

        members = node.members()
        assert len(members) == 1
        assert members[0].state == State.ALIVE
    finally:
        await node.stop()


async def test_falsely_dead_member_is_resurrected():
    a, b = Node(make_config("a")), Node(make_config("b"))
    await a.start()
    await b.start()
    try:
        # b is genuinely alive and reachable, but a's table has it
        # recorded as DEAD - exactly what a false-positive suspicion
        # timeout (e.g. from a load spike) produces. Nothing should
        # ever gossip this DEAD verdict back to b (b was never told),
        # so only a's own periodic re-probe can discover the truth.
        a.learn("b", b.addr)
        a._merge_update(Member(id="b", addr=b.addr, state=State.DEAD, incarnation=0))
        assert any(m.id == "b" and m.state == State.DEAD for m in a.members())

        def b_is_alive_again() -> bool:
            return any(m.id == "b" and m.state == State.ALIVE for m in a.members())

        await eventually(b_is_alive_again, timeout=1.0)
    finally:
        await a.stop()
        await b.stop()
