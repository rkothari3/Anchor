"""A bounded 2D world divided into a grid of shards (regions).

Movement is deliberately simple - constant velocity, bounce off the
world's edges - since the point of this module is the partitioning
scheme, not the movement model (which the README explicitly treats as
plumbing/application content, not core distributed-systems learning).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GridConfig:
    width: float
    height: float
    cols: int
    rows: int

    @property
    def shard_width(self) -> float:
        return self.width / self.cols

    @property
    def shard_height(self) -> float:
        return self.height / self.rows


def shard_id_for(x: float, y: float, grid: GridConfig) -> str:
    col = int(x // grid.shard_width)
    row = int(y // grid.shard_height)
    col = min(max(col, 0), grid.cols - 1)
    row = min(max(row, 0), grid.rows - 1)
    return f"{row}-{col}"


@dataclass
class AgentState:
    id: str
    x: float
    y: float
    vx: float
    vy: float

    def step(self, dt: float, world: GridConfig) -> None:
        self.x += self.vx * dt
        self.y += self.vy * dt
        self.x, self.vx = _bounce(self.x, self.vx, world.width)
        self.y, self.vy = _bounce(self.y, self.vy, world.height)

    def shard_id(self, grid: GridConfig) -> str:
        return shard_id_for(self.x, self.y, grid)


def _bounce(pos: float, vel: float, limit: float) -> tuple[float, float]:
    if pos < 0:
        return -pos, -vel
    if pos > limit:
        return 2 * limit - pos, -vel
    return pos, vel
