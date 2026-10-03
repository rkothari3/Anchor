"""Helpers shared across the test suites."""

import asyncio
import inspect


async def eventually(cond, timeout: float = 5.0) -> None:
    """Polls cond (sync or async) until it's truthy; fails after timeout."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        result = cond()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        assert loop.time() < deadline, "condition not met within timeout"
        await asyncio.sleep(0.01)
