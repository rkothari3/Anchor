"""CLI: runs one 0/N-kill experiment against a live cluster (kind or
otherwise) - polls loss over wall-clock while killing `--kills` pods
evenly spaced across `--duration`, then writes a CSV for the
loss-vs-wall-clock comparison plot.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import subprocess
from pathlib import Path

from dssd.addr import parse_peers
from dssd.dashboard.poller import Poller

from .chaos import ChaosScheduler, pick_victim
from .metrics import MetricsRecorder

logger = logging.getLogger("experiment")


def kubectl_delete_pod(name: str, namespace: str) -> None:
    subprocess.run(
        ["kubectl", "delete", "pod", name, "--namespace", namespace, "--now", "--wait=false"],
        check=True,
        capture_output=True,
    )


async def run(args: argparse.Namespace) -> None:
    peer_addrs = parse_peers(args.peer)
    poller = Poller(peer_addrs)

    async def poll_fn() -> dict[str, float | None]:
        snapshot = await poller.snapshot()
        return {w["id"]: w["loss"] for w in snapshot["workers"]}

    recorder = MetricsRecorder(poll_fn=poll_fn, interval=args.interval)

    rng = random.Random(args.seed)
    pod_names = list(peer_addrs.keys())

    async def kill_fn(index: int) -> None:
        victim = pick_victim(pod_names, rng)
        logger.info("chaos: killing pod %s (%d/%d)", victim, index + 1, args.kills)
        await asyncio.to_thread(kubectl_delete_pod, victim, args.namespace)

    chaos = ChaosScheduler(total_kills=args.kills, duration=args.duration, kill_fn=kill_fn)

    await asyncio.gather(recorder.run(args.duration), chaos.run())

    out = Path(args.out)
    recorder.write_csv(out)
    logger.info("wrote %d rows to %s", len(recorder.rows), out)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--peer", action="append", default=[], required=True, help="worker as id=grpc-host:port; repeat for each worker")
    parser.add_argument("--kills", type=int, required=True, help="total pods to kill, spread evenly across --duration")
    parser.add_argument("--duration", type=float, required=True, help="run length in seconds")
    parser.add_argument("--interval", type=float, default=2.0, help="loss poll interval in seconds")
    parser.add_argument("--namespace", default="default", help="k8s namespace the pods run in")
    parser.add_argument("--seed", type=int, default=0, help="RNG seed for picking kill victims")
    parser.add_argument("--out", required=True, help="output CSV path")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
