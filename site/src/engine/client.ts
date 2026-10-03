import { useSyncExternalStore } from "react";
import type { ClusterName, ConsensusSnapshot, FeedEvent, FromWorker, Packet, Snapshot, ToWorker, WorldSnapshot } from "./types";

export type EngineStatus = "idle" | "loading" | "ready" | "error";
export interface LivePacket extends Packet {
  born: number; // performance.now() when we received it
}
interface ClusterState<S extends Snapshot> {
  snapshot: S | null;
  feed: FeedEvent[]; // newest last
  packets: LivePacket[];
}
export interface EngineState {
  status: EngineStatus;
  stage: string;
  error: string | null;
  consensus: ClusterState<ConsensusSnapshot>;
  world: ClusterState<WorldSnapshot>;
}

const FEED_LIMIT = 80;
const PACKET_TTL_MS = 900;
const empty = <S extends Snapshot>(): ClusterState<S> => ({ snapshot: null, feed: [], packets: [] });

class Engine {
  private worker: Worker | null = null;
  private listeners = new Set<() => void>();
  private wanted = new Set<ClusterName>();
  private hiddenAt = 0;
  private state: EngineState = {
    status: "idle",
    stage: "",
    error: null,
    consensus: empty(),
    world: empty(),
  };

  constructor() {
    if (typeof document !== "undefined") {
      // Background tabs throttle timers, which makes an election cluster flap; start fresh on return.
      document.addEventListener("visibilitychange", () => {
        if (document.hidden) this.hiddenAt = performance.now();
        else if (this.hiddenAt && performance.now() - this.hiddenAt > 20_000) {
          for (const c of this.wanted) this.post({ type: "reset", cluster: c });
          this.hiddenAt = 0;
        }
      });
    }
  }

  subscribe = (fn: () => void) => {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  };
  getState = () => this.state;

  private set(patch: Partial<EngineState>) {
    this.state = { ...this.state, ...patch };
    for (const l of this.listeners) l();
  }

  private post(m: ToWorker) {
    this.worker?.postMessage(m);
  }

  boot() {
    if (this.worker) return;
    this.set({ status: "loading", stage: "Loading the Python runtime" });
    this.worker = new Worker(new URL("./engine.worker.ts", import.meta.url), { type: "module" });
    this.worker.onmessage = (e: MessageEvent<FromWorker>) => this.onMessage(e.data);
    this.worker.onerror = (e) => this.set({ status: "error", error: e.message || "worker crashed" });
    this.post({ type: "init", base: import.meta.env.BASE_URL });
  }

  private onMessage(m: FromWorker) {
    if (m.type === "progress") this.set({ stage: m.stage });
    else if (m.type === "ready") this.set({ status: "ready", stage: "" });
    else if (m.type === "error") this.set({ status: "error", error: m.message });
    else {
      const snap = JSON.parse(m.data) as Snapshot;
      const now = performance.now();
      const prev = this.state[m.cluster] as ClusterState<Snapshot>;
      const packets = [
        ...prev.packets.filter((p) => now - p.born < PACKET_TTL_MS),
        ...snap.packets.map((p) => ({ ...p, born: now })),
      ];
      const feed = snap.events.length ? [...prev.feed, ...snap.events].slice(-FEED_LIMIT) : prev.feed;
      this.set({ [m.cluster]: { snapshot: snap, feed, packets } } as Partial<EngineState>);
    }
  }

  /** Begin running a cluster (idempotent). Boots the Python runtime on first use. */
  start(cluster: ClusterName) {
    this.boot();
    this.wanted.add(cluster);
    this.post({ type: "start", cluster });
  }
  stop(cluster: ClusterName) {
    this.wanted.delete(cluster);
    this.post({ type: "stop", cluster });
    this.set({ [cluster]: empty() } as Partial<EngineState>);
  }
  reset(cluster: ClusterName) {
    this.post({ type: "reset", cluster });
    this.set({ [cluster]: empty() } as Partial<EngineState>);
  }
  call(cluster: ClusterName, method: string, ...args: unknown[]) {
    this.post({ type: "call", cluster, method, args });
  }
}

export const engine = new Engine();
export const useEngine = () => useSyncExternalStore(engine.subscribe, engine.getState);
