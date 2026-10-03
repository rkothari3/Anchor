"""A live dashboard for the membership + DiLoCo cluster: serves a single
self-contained page that shows each worker's state, round, and loss,
updating over a WebSocket.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from aiohttp import web

from dssd.addr import parse_peers, split_addr

from .poller import Poller

logger = logging.getLogger("dashboard")

STATIC_DIR = Path(__file__).parent / "static"
POLL_INTERVAL = 1.0


async def index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html")


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    poller: Poller = request.app["poller"]

    async def push_loop() -> None:
        while True:
            snapshot = await poller.snapshot()
            await ws.send_str(json.dumps(snapshot))
            await asyncio.sleep(POLL_INTERVAL)

    async def receive_loop() -> None:
        # This dashboard is push-only, but a WS connection still needs
        # something reading frames to notice a client-side close
        # promptly - without it, a closed browser tab is only detected
        # whenever the next send_str() happens to fail.
        async for _ in ws:
            pass

    pusher = asyncio.create_task(push_loop())
    receiver = asyncio.create_task(receive_loop())
    try:
        await asyncio.wait({pusher, receiver}, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        pass
    finally:
        pusher.cancel()
        receiver.cancel()
        await asyncio.gather(pusher, receiver, return_exceptions=True)
        if not ws.closed:
            await ws.close()
    return ws


def build_app(peer_addrs: dict[str, str]) -> web.Application:
    app = web.Application()
    app["poller"] = Poller(peer_addrs)
    app.router.add_get("/", index)
    app.router.add_get("/ws", websocket_handler)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--http-addr", default="127.0.0.1:8080", help="address to serve the dashboard on")
    parser.add_argument("--peer", action="append", default=[], required=True, help="worker as id=grpc-host:port; repeat for each worker")
    args = parser.parse_args()

    peer_addrs = parse_peers(args.peer)
    host, port = split_addr(args.http_addr)

    app = build_app(peer_addrs)
    logger.info("dashboard up: http://%s:%d watching %s", host, port, list(peer_addrs.keys()))
    web.run_app(app, host=host, port=port, print=None)


if __name__ == "__main__":
    main()
