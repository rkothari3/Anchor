from dssd.sharding.world import AgentState, GridConfig, shard_id_for

GRID = GridConfig(width=100, height=100, cols=2, rows=2)


def test_shard_id_partitions_the_world_into_a_grid():
    assert shard_id_for(10, 10, GRID) == "0-0"
    assert shard_id_for(60, 10, GRID) == "0-1"
    assert shard_id_for(10, 60, GRID) == "1-0"
    assert shard_id_for(60, 60, GRID) == "1-1"


def test_shard_id_clamps_points_on_or_outside_the_edge():
    assert shard_id_for(100, 100, GRID) == "1-1"  # exactly on the far edge
    assert shard_id_for(-5, 150, GRID) == "1-0"


def test_agent_bounces_off_the_edge():
    agent = AgentState(id="a", x=98, y=50, vx=5, vy=0)
    agent.step(dt=1, grid=GRID)  # would land at x=103
    assert (agent.x, agent.vx) == (97, -5)
