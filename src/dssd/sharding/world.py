"""A 2D world cut into a grid of shards (regions). Agents move at constant
velocity and bounce off the edges; the movement is deliberately boring,
since the point is the partitioning, not the simulation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GridConfig:
    width: float
    height: float
    cols: int
    rows: int


def shard_id_for(x: float, y: float, grid: GridConfig) -> str:
    """Shard ids are "row-col"; points outside the world clamp to the edge shard."""
    col = min(max(int(x // (grid.width / grid.cols)), 0), grid.cols - 1)
    row = min(max(int(y // (grid.height / grid.rows)), 0), grid.rows - 1)
    return f"{row}-{col}"


def all_shard_ids(grid: GridConfig) -> list[str]:
    return [f"{row}-{col}" for row in range(grid.rows) for col in range(grid.cols)]


@dataclass
class AgentState:
    id: str
    x: float
    y: float
    vx: float
    vy: float

    def step(self, dt: float, grid: GridConfig) -> None:
        self.x, self.vx = _bounce(self.x + self.vx * dt, self.vx, grid.width)
        self.y, self.vy = _bounce(self.y + self.vy * dt, self.vy, grid.height)

    def shard_id(self, grid: GridConfig) -> str:
        return shard_id_for(self.x, self.y, grid)


def _bounce(pos: float, vel: float, limit: float) -> tuple[float, float]:
    if pos < 0:
        return -pos, -vel
    if pos > limit:
        return 2 * limit - pos, -vel
    return pos, vel
