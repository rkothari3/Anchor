from dssd.sharding.world import AgentState, GridConfig, shard_id_for


def make_grid() -> GridConfig:
    return GridConfig(width=100.0, height=100.0, cols=2, rows=2)


def test_shard_id_partitions_world_into_grid():
    grid = make_grid()
    assert shard_id_for(10, 10, grid) == "0-0"
    assert shard_id_for(60, 10, grid) == "0-1"
    assert shard_id_for(10, 60, grid) == "1-0"
    assert shard_id_for(60, 60, grid) == "1-1"


def test_shard_id_clamps_boundary_coordinates():
    grid = make_grid()
    assert shard_id_for(0, 0, grid) == "0-0"
    assert shard_id_for(100, 100, grid) == "1-1"  # exactly on the far edge
    assert shard_id_for(-5, -5, grid) == "0-0"  # out of bounds clamps in


def test_agent_step_moves_by_velocity():
    agent = AgentState(id="a1", x=10.0, y=10.0, vx=5.0, vy=0.0)
    agent.step(dt=1.0, world=GridConfig(width=100.0, height=100.0, cols=1, rows=1))
    assert agent.x == 15.0
    assert agent.y == 10.0


def test_agent_bounces_off_right_edge():
    world = GridConfig(width=100.0, height=100.0, cols=1, rows=1)
    agent = AgentState(id="a1", x=98.0, y=50.0, vx=5.0, vy=0.0)
    agent.step(dt=1.0, world=world)  # would land at x=103, past the edge
    assert agent.x == 97.0  # reflected: 2*100 - 103
    assert agent.vx == -5.0


def test_agent_bounces_off_left_edge():
    world = GridConfig(width=100.0, height=100.0, cols=1, rows=1)
    agent = AgentState(id="a1", x=2.0, y=50.0, vx=-5.0, vy=0.0)
    agent.step(dt=1.0, world=world)  # would land at x=-3
    assert agent.x == 3.0  # reflected: -(-3)
    assert agent.vx == 5.0


def test_agent_shard_id_matches_grid():
    grid = make_grid()
    agent = AgentState(id="a1", x=60.0, y=60.0, vx=0.0, vy=0.0)
    assert agent.shard_id(grid) == "1-1"
