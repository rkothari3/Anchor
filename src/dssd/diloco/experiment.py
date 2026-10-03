"""CLI: one 0/N-kill experiment against a live cluster. Polls every
worker's loss over wall-clock while killing --kills random pods, evenly
spaced across --duration, then writes an elapsed,avg_loss CSV - the data
behind the loss-vs-wall-clock-at-N-kills plot.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import random
import subprocess

import grpc

from dssd import trainerpb
from dssd.addr import parse_peers

logger = logging.getLogger("experiment")


def kill_schedule(total_kills: int, duration: float) -> list[float]:
    """Evenly spaced kill times; none lands exactly on the start or end."""
    step = duration / (total_kills + 1)
    return [step * (i + 1) for i in range(total_kills)]


def average_loss(losses: list[float | None]) -> float | None:
    """Mean over the workers that answered (the usual local-SGD convention)."""
    reachable = [loss for loss in losses if loss is not None]
    return sum(reachable) / len(reachable) if reachable else None


async def worker_loss(addr: str) -> float | None:
    try:
        async with grpc.aio.insecure_channel(addr) as channel:
            resp = await trainerpb.TrainerStub(channel).Status(trainerpb.StatusRequest(), timeout=2.0)
        return resp.loss if resp.HasField("loss") else None
    except grpc.aio.AioRpcError:
        return None  # killed or restarting


async def record_losses(addrs: list[str], duration: float, interval: float) -> list[tuple[float, float | None]]:
    loop = asyncio.get_running_loop()
    start = loop.time()
    rows = []
    while (elapsed := loop.time() - start) <= duration:
        losses = await asyncio.gather(*(worker_loss(a) for a in addrs))
        rows.append((elapsed, average_loss(losses)))
        await asyncio.sleep(max(0.0, start + elapsed + interval - loop.time()))
    return rows


async def kill_pods(pods: list[str], kills: int, duration: float, namespace: str, rng: random.Random) -> None:
    loop = asyncio.get_running_loop()
    start = loop.time()
    for i, at in enumerate(kill_schedule(kills, duration)):
        await asyncio.sleep(max(0.0, start + at - loop.time()))
        victim = rng.choice(pods)
        logger.info("chaos: killing pod %s (%d/%d)", victim, i + 1, kills)
        cmd = ["kubectl", "delete", "pod", victim, "--namespace", namespace, "--now", "--wait=false"]
        await asyncio.to_thread(subprocess.run, cmd, check=True, capture_output=True)


async def run(args: argparse.Namespace) -> None:
    peers = parse_peers(args.peer)  # pod name -> grpc addr
    rows, _ = await asyncio.gather(
        record_losses(list(peers.values()), args.duration, args.interval),
        kill_pods(list(peers), args.kills, args.duration, args.namespace, random.Random(args.seed)),
    )
    with open(args.out, "w", newline="") as f:
        csv.writer(f).writerows([("elapsed", "avg_loss"), *rows])
    logger.info("wrote %d rows to %s", len(rows), args.out)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--peer", action="append", required=True, help="worker as pod-name=grpc-host:port; repeat")
    parser.add_argument("--kills", type=int, required=True, help="pods to kill, spread evenly across --duration")
    parser.add_argument("--duration", type=float, required=True, help="run length in seconds")
    parser.add_argument("--interval", type=float, default=2.0, help="loss poll interval in seconds")
    parser.add_argument("--namespace", default="default", help="k8s namespace the pods run in")
    parser.add_argument("--seed", type=int, default=0, help="RNG seed for picking kill victims")
    parser.add_argument("--out", required=True, help="output CSV path")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
