import asyncio
from functools import partial

from dssd.dashboard.poller import Poller
from tests.diloco.test_worker import start_cluster
from tests import support

eventually = partial(support.eventually, timeout=15.0)


async def test_snapshot_reports_all_reachable_members_alive():
    members = await start_cluster(3, inner_steps=5, batch_size=8)
    try:
        for m in members:
            await eventually(lambda m=m: m.worker.round >= 1)

        peer_addrs = {m.id: m.grpc_addr for m in members}
        poller = Poller(peer_addrs)
        snap = await poller.snapshot()

        assert len(snap["workers"]) == 3
        for worker in snap["workers"]:
            assert worker["reachable"] is True
            assert worker["state"] == "ALIVE"
            assert worker["round"] >= 1
            assert worker["loss"] is not None
    finally:
        await asyncio.gather(*(m.stop() for m in members))


async def test_snapshot_marks_dead_worker_dead_via_membership():
    members = await start_cluster(3, inner_steps=5, batch_size=8)
    try:
        for m in members:
            await eventually(lambda m=m: m.worker.round >= 1)

        victim = members[2]
        victim_id = victim.id
        await victim.stop()

        peer_addrs = {m.id: m.grpc_addr for m in members}
        poller = Poller(peer_addrs)

        async def victim_marked_dead() -> bool:
            snap = await poller.snapshot()
            by_id = {w["id"]: w for w in snap["workers"]}
            return by_id.get(victim_id, {}).get("state") == "DEAD"

        # The victim's own gRPC port is down, so its direct poll fails
        # (reachable=False); once SWIM's failure detector catches up,
        # a survivor's membership view should override that with the
        # authoritative DEAD state.
        await eventually(victim_marked_dead, timeout=5.0)
    finally:
        await asyncio.gather(*(m.stop() for m in members))
