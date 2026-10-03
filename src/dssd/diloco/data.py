"""Character-level tokenization and batching for the toy DiLoCo model.

Not meant to produce a good language model — just a fast, deterministic
training signal for exercising the distributed training mechanics.
"""

from __future__ import annotations

import torch


class CharTokenizer:
    def __init__(self, corpus: str) -> None:
        chars = sorted(set(corpus))
        self.vocab_size = len(chars)
        self._stoi = {c: i for i, c in enumerate(chars)}

    def encode(self, text: str) -> list[int]:
        return [self._stoi[c] for c in text]


def synthetic_corpus(length: int) -> str:
    """A short, easily learnable repeating pattern - enough signal for
    inner-loop training and pseudo-gradient averaging to be visibly
    correct without needing a real text corpus."""
    pattern = "the quick brown fox jumps over the lazy dog. "
    reps = length // len(pattern) + 1
    return (pattern * reps)[:length]


def make_batch(data: torch.Tensor, block_size: int, batch_size: int) -> tuple[torch.Tensor, torch.Tensor]:
    max_start = len(data) - block_size - 1
    if max_start <= 0:
        raise ValueError(f"data has {len(data)} tokens, too short for block_size={block_size}")
    starts = torch.randint(0, max_start, (batch_size,))
    x = torch.stack([data[s : s + block_size] for s in starts])
    y = torch.stack([data[s + 1 : s + block_size + 1] for s in starts])
    return x, y
