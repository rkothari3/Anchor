"""SWIM membership and failure detection (Das, Gupta & Motivala, DSN 2002).

Every protocol_period each node pings one random peer. No ack in time? It
asks a few other peers to ping it indirectly (ping-req). Still nothing?
The peer becomes SUSPECT, and if nobody refutes that before
suspicion_timeout it becomes DEAD. Membership changes ride along on the
pings as gossip, so the whole cluster converges without broadcasts.

Everything runs on one asyncio event loop, so state needs no locks: a
method that doesn't ``await`` mid-update is already atomic.
"""

from __future__ import annotations

import asyncio
import json
import random
from dataclasses import asdict, dataclass
from enum import Enum, IntEnum

from dssd.addr import format_addr, split_addr

MAX_TRANSMITS_PER_UPDATE = 5  # how many times each change is gossiped
MAX_UPDATES_PER_MESSAGE = 8


class State(IntEnum):
    """A member's state. The values double as severity: DEAD > SUSPECT > ALIVE."""

    ALIVE = 0
    SUSPECT = 1
    DEAD = 2


@dataclass(frozen=True)
class Member:
    id: str
    addr: str
    state: State = State.ALIVE
    # Bumped only by the member itself, to refute a SUSPECT/DEAD rumor.
    incarnation: int = 0


def supersedes(update: Member, current: Member) -> bool:
    """SWIM's conflict rule: DEAD always wins; otherwise a higher
    incarnation wins, and at equal incarnation the more severe state wins."""
    if update.state == State.DEAD:
        return current.state != State.DEAD
    if update.incarnation != current.incarnation:
        return update.incarnation > current.incarnation
    return update.state > current.state


class MsgType(str, Enum):
    PING = "ping"
    PING_REQ = "ping-req"  # "please ping target_addr for me"
    ACK = "ack"
    JOIN = "join"
    JOIN_ACK = "join-ack"


@dataclass(frozen=True)
class Message:
    """The UDP wire format, sent as JSON."""

    type: MsgType
    seq: int
    sender: Member  # receiving a message proves its sender is alive
    target_addr: str = ""  # ping-req only
    updates: tuple[Member, ...] = ()  # piggybacked gossip

    def encode(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @staticmethod
    def decode(data: bytes) -> Message:
        obj = json.loads(data)
        member = lambda d: Member(d["id"], d["addr"], State(d["state"]), d["incarnation"])  # noqa: E731
        return Message(
            type=MsgType(obj["type"]),
            seq=obj["seq"],
            sender=member(obj["sender"]),
            target_addr=obj["target_addr"],
            updates=tuple(member(u) for u in obj["updates"]),
        )


@dataclass
class Config:
    id: str
    bind_host: str = "127.0.0.1"
    bind_port: int = 0
    protocol_period: float = 0.2  # how often to probe a random peer
    ping_timeout: float = 0.05  # how long to wait for a direct or indirect ack
    indirect_ping_count: int = 3  # how many peers to ask for a ping-req
    suspicion_timeout: float = 1.0  # SUSPECT -> DEAD if not refuted by then
    resurrect_interval: float = 2.0  # how often to re-probe a DEAD peer


class Node:
    """A SWIM agent. Call start() to begin probing and stop() to shut down.

    By default it talks over a real UDP socket. Pass `listen` to run it on
    any other datagram network (the browser demo uses an in-memory one): an
    async function that takes our asyncio.DatagramProtocol and returns a
    transport with sendto(), close() and get_extra_info("sockname").
    """

    def __init__(self, config: Config, listen=None) -> None:
        self.cfg = config
        self._listen = listen
        self._members: dict[str, Member] = {}  # everyone but us
        self._incarnation = 0
        self._transport: asyncio.DatagramTransport | None = None
        self._bind_addr: str | None = None
        self._waiters: dict[int, asyncio.Future] = {}  # seq -> waiting for its ack
        self._seq_no = 0
        self._broadcasts: dict[str, list] = {}  # member id -> [latest update, times sent]
        self._tasks: set[asyncio.Task] = set()

    @property
    def addr(self) -> str:
        if self._bind_addr is None:
            raise RuntimeError("swim: node not started")
        return self._bind_addr

    async def start(self) -> None:
        if self._listen is not None:
            self._transport = await self._listen(_Protocol(self))
        else:
            local = (self.cfg.bind_host, self.cfg.bind_port)
            self._transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
                lambda: _Protocol(self), local_addr=local
            )
        host, port = self._transport.get_extra_info("sockname")[:2]
        self._bind_addr = format_addr(host, port)
        self._spawn(self._every(self.cfg.protocol_period, self._probe_once))
        self._spawn(self._every(self.cfg.resurrect_interval, self._resurrect_once))

    async def stop(self) -> None:
        if self._transport is not None:
            self._transport.close()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def members(self) -> list[Member]:
        """Everything this node knows about, including itself."""
        return [self._me(), *self._members.values()]

    async def join(self, contact_addr: str) -> None:
        """Bootstraps our member list from any existing member."""
        acked = await self._request(contact_addr, MsgType.JOIN, timeout=3 * self.cfg.ping_timeout)
        if not acked:
            raise TimeoutError(f"swim: join {contact_addr} timed out")

    # --- helpers ---

    def _me(self) -> Member:
        return Member(self.cfg.id, self.addr, State.ALIVE, self._incarnation)

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _every(self, seconds: float, fn) -> None:
        while True:
            await asyncio.sleep(seconds)
            await fn()

    def _send(self, addr: str, msg: Message) -> None:
        assert self._transport is not None
        self._transport.sendto(msg.encode(), split_addr(addr))

    async def _request(self, addr: str, msg_type: MsgType, timeout: float, updates=(), **extra) -> bool:
        """Sends one message and waits for the matching ack. Returns whether it came."""
        self._seq_no += 1
        seq = self._seq_no
        ack = self._waiters[seq] = asyncio.get_running_loop().create_future()
        try:
            self._send(addr, Message(msg_type, seq, self._me(), updates=tuple(updates), **extra))
            await asyncio.wait_for(ack, timeout)
            return True
        except TimeoutError:
            return False
        finally:
            self._waiters.pop(seq, None)

    # --- probing ---

    async def _probe_once(self) -> None:
        candidates = [m for m in self._members.values() if m.state != State.DEAD]
        if not candidates:
            return
        target = random.choice(candidates)
        if await self._request(target.addr, MsgType.PING, self.cfg.ping_timeout, self._take_broadcasts()):
            return

        # No direct ack: ask a few other peers to try (maybe only our link is bad).
        helpers = [m for m in candidates if m.id != target.id]
        helpers = random.sample(helpers, min(self.cfg.indirect_ping_count, len(helpers)))
        asks = [self._request(h.addr, MsgType.PING_REQ, 2 * self.cfg.ping_timeout, target_addr=target.addr) for h in helpers]
        if any(await asyncio.gather(*asks)):
            return
        self._mark(target.id, State.SUSPECT, from_state=State.ALIVE)

    async def _relay_ping(self, req: Message) -> None:
        """We were asked (ping-req) to ping a target on someone's behalf."""
        if await self._request(req.target_addr, MsgType.PING, self.cfg.ping_timeout):
            self._send(req.sender.addr, Message(MsgType.ACK, req.seq, self._me()))

    async def _resurrect_once(self) -> None:
        """Re-pings one DEAD peer. Dead peers are never probed otherwise, so
        without this a false DEAD verdict (e.g. from a load spike) would be
        permanent: the accused node never hears it needs to refute."""
        dead = [m for m in self._members.values() if m.state == State.DEAD]
        if not dead:
            return
        target = random.choice(dead)
        # A generous timeout: this exists to undo verdicts that a tight one caused.
        if await self._request(target.addr, MsgType.PING, 5 * self.cfg.ping_timeout):
            current = self._members.get(target.id)
            if current and current.state == State.DEAD:
                # It can't know it was declared dead, so we bump its incarnation for it.
                self._set(Member(current.id, current.addr, State.ALIVE, current.incarnation + 1))

    def _mark(self, id: str, new_state: State, from_state: State, incarnation: int | None = None) -> None:
        """Moves a member from_state -> new_state, if it's still there and
        (when given) still at that incarnation, i.e. nobody refuted it."""
        current = self._members.get(id)
        if current is None or current.state != from_state:
            return
        if incarnation is not None and current.incarnation != incarnation:
            return
        self._set(Member(current.id, current.addr, new_state, current.incarnation))

    def _set(self, member: Member) -> None:
        """Records a state change locally and queues it for gossip."""
        old = self._members.get(member.id)
        self._members[member.id] = member
        self._broadcasts[member.id] = [member, 0]
        if member.state == State.SUSPECT and (old is None or old.state != State.SUSPECT):
            self._spawn(self._suspicion_timer(member))

    async def _suspicion_timer(self, member: Member) -> None:
        await asyncio.sleep(self.cfg.suspicion_timeout)
        self._mark(member.id, State.DEAD, from_state=State.SUSPECT, incarnation=member.incarnation)

    # --- receiving ---

    def _handle(self, msg: Message) -> None:
        self._merge(msg.sender)
        for update in msg.updates:
            self._merge(update)

        if msg.type == MsgType.PING:
            self._reply(msg, MsgType.ACK, self._take_broadcasts())
        elif msg.type == MsgType.JOIN:
            self._reply(msg, MsgType.JOIN_ACK, list(self._members.values()))
        elif msg.type == MsgType.PING_REQ:
            self._spawn(self._relay_ping(msg))
        else:  # ACK or JOIN_ACK
            waiter = self._waiters.get(msg.seq)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)

    def _reply(self, req: Message, msg_type: MsgType, updates: list[Member]) -> None:
        self._send(req.sender.addr, Message(msg_type, req.seq, self._me(), updates=tuple(updates)))

    def _merge(self, update: Member) -> None:
        """Applies one piece of gossip."""
        if update.id == self.cfg.id:
            # A rumor that we're SUSPECT/DEAD: refute it with a newer incarnation.
            if update.state != State.ALIVE and update.incarnation >= self._incarnation:
                self._incarnation = update.incarnation + 1
                self._broadcasts[self.cfg.id] = [self._me(), 0]
            return
        current = self._members.get(update.id)
        if current is None:
            if update.state != State.DEAD:  # don't learn about strangers that are already dead
                self._set(update)
        elif supersedes(update, current):
            self._set(update)

    def _take_broadcasts(self) -> list[Member]:
        """The least-gossiped updates first; each is dropped after MAX_TRANSMITS."""
        due = sorted(self._broadcasts.values(), key=lambda entry: entry[1])[:MAX_UPDATES_PER_MESSAGE]
        for entry in due:
            entry[1] += 1
            if entry[1] >= MAX_TRANSMITS_PER_UPDATE:
                del self._broadcasts[entry[0].id]
        return [entry[0] for entry in due]


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, node: Node) -> None:
        self._node = node

    def datagram_received(self, data: bytes, addr) -> None:
        try:
            msg = Message.decode(data)
        except Exception:  # noqa: BLE001 - a malformed packet must never crash the node
            return
        self._node._handle(msg)
