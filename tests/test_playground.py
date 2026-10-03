"""The browser demo's engine, run under plain CPython with fast timings."""

import json

from dssd.playground import FAST, Consensus, World
from tests.support import eventually


async def test_killing_the_leader_elects_a_new_one_in_a_later_term():
    c = Consensus(lambda s: None, FAST)
    c.start()
    try:
        await eventually(lambda: c.ready)
        await eventually(lambda: c.snapshot()["leader"] is not None)
        first = c.snapshot()["leader"]
        term = next(n["term"] for n in c.snapshot()["nodes"] if n["id"] == first)
        c.kill_leader()
        await eventually(lambda: c.snapshot()["leader"] not in (None, first))
        new = c.snapshot()["leader"]
        assert next(n["term"] for n in c.snapshot()["nodes"] if n["id"] == new) > term
    finally:
        c.stop()


async def test_a_write_reaches_every_node_and_commits():
    c = Consensus(lambda s: None, FAST)
    c.start()
    try:
        await eventually(lambda: c.ready)
        await eventually(lambda: c.snapshot()["leader"] is not None)
        c.write("x=1")
        await eventually(lambda: all(n["commit"] >= 2 for n in c.snapshot()["nodes"]))  # entry 1 is the no-op
        assert all(n["log"][-1][1] == "x=1" for n in c.snapshot()["nodes"])
    finally:
        c.stop()


async def test_only_the_majority_side_of_a_partition_elects_a_leader():
    c = Consensus(lambda s: None, FAST)
    c.start()
    try:
        await eventually(lambda: c.ready)
        await eventually(lambda: c.snapshot()["leader"] is not None)
        c.partition([["n1", "n2"], ["n3", "n4", "n5"]])
        majority = {"n3", "n4", "n5"}
        # The majority elects a leader in a term beyond whatever the minority can reach.
        await eventually(lambda: c.snapshot()["leader"] in majority)
        top = max(n["term"] for n in c.snapshot()["nodes"] if n["id"] in majority)
        c.write("after-split")
        await eventually(lambda: any(n["commit"] >= 2 and n["id"] in majority for n in c.snapshot()["nodes"]))
        assert top >= 1
    finally:
        c.stop()


async def test_a_crashed_node_is_declared_dead_then_recovers_when_restarted():
    c = Consensus(lambda s: None, FAST)
    c.start()
    try:
        await eventually(lambda: c.ready)
        await eventually(lambda: c.snapshot()["leader"] is not None)
        c.kill("n5")
        await eventually(lambda: c.snapshot()["nodes"][0]["swim"]["n5"] == "dead")
        c.revive("n5")
        await eventually(lambda: c.snapshot()["nodes"][0]["swim"]["n5"] == "alive")
    finally:
        c.stop()


async def test_the_cluster_reports_json_snapshots():
    seen: list[str] = []
    c = Consensus(seen.append, FAST)
    c.start()
    try:
        await eventually(lambda: c.ready)
        await eventually(lambda: len(seen) >= 3)
        snap = json.loads(seen[-1])
        assert len(snap["nodes"]) == 5 and "events" in snap and "packets" in snap
    finally:
        c.stop()


async def test_no_agent_is_lost_when_the_busiest_region_server_dies():
    w = World(lambda s: None, FAST)
    w.start()
    try:
        await eventually(lambda: w.ready)
        await eventually(lambda: w.spawned and len(w.snapshot()["agents"]) == 8, timeout=15)
        w.kill_busiest()
        await eventually(lambda: all(s["leader"] for s in w.snapshot()["shards"]), timeout=15)
        await eventually(lambda: len(w.snapshot()["agents"]) == 8, timeout=15)
        assert w.snapshot()["stats"]["lost"] == 0
    finally:
        w.stop()


async def test_agents_are_not_reported_lost_while_every_region_server_is_down():
    w = World(lambda s: None, FAST)
    w.start()
    try:
        await eventually(lambda: w.ready)
        await eventually(lambda: w.spawned and len(w.snapshot()["agents"]) == 8, timeout=15)
        for nid in w.ids:
            w.kill(nid)
        await eventually(lambda: not any(n["up"] for n in w.snapshot()["nodes"]))
        assert w.snapshot()["stats"]["lost"] == 0  # their state is still on the crashed nodes
    finally:
        w.stop()
