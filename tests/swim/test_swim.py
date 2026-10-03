import asyncio

from dssd.swim import Config, Member, Node, State
from tests.support import eventually


def make_config(id: str) -> Config:
    return Config(
        id=id,
        protocol_period=0.05,
        ping_timeout=0.05,
        indirect_ping_count=2,
        suspicion_timeout=0.2,
        resurrect_interval=0.1,
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


def has_state(node: Node, id: str, state: State) -> bool:
    return any(m.id == id and m.state == state for m in node.members())


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
        await victim.stop()
        await eventually(lambda: has_state(nodes[0], victim.cfg.id, State.DEAD))
    finally:
        await stop_all(nodes)


async def test_refutation_keeps_live_member_alive():
    node = Node(make_config("a"))
    await node.start()
    try:
        node._merge(Member(id="a", addr=node.addr, state=State.SUSPECT, incarnation=0))

        assert node.members()[0].incarnation > 0  # members()[0] is the node itself
        [me] = node.members()
        assert me.state == State.ALIVE
    finally:
        await node.stop()


async def test_falsely_dead_member_is_resurrected():
    a, b = Node(make_config("a")), Node(make_config("b"))
    await a.start()
    await b.start()
    try:
        # b is alive and reachable, but a has it recorded as DEAD, which is
        # what a false-positive suspicion timeout produces. b was never told,
        # so only a's own periodic re-probe can discover the truth.
        a._merge(Member("b", b.addr))
        a._merge(Member("b", b.addr, State.DEAD))
        assert has_state(a, "b", State.DEAD)

        await eventually(lambda: has_state(a, "b", State.ALIVE), timeout=2.0)
    finally:
        await a.stop()
        await b.stop()
