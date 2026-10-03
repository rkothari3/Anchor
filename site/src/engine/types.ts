export type ClusterName = "consensus" | "world";
export type Role = "follower" | "candidate" | "leader";
export type SwimState = "alive" | "suspect" | "dead";

export interface Packet {
  i: number; // sequence number
  p: "swim" | "raft" | "handoff";
  k: string; // ping | ack | ping-req | join | vote | heartbeat | append | reply | handoff
  a: string; // from
  b: string; // to
  ok: boolean; // false = dropped (loss, partition, or down node)
  g: string; // raft group / shard id
}

export interface FeedEvent {
  t: number; // cluster seconds
  k: "leader" | "election" | "stepdown" | "suspect" | "dead" | "recover" | "user" | "write";
  text: string;
}

interface Base {
  ready?: boolean;
  t: number;
  events: FeedEvent[];
  packets: Packet[];
  net: { loss: number; latency: number; side: Record<string, number> };
}

export interface ConsensusNode {
  id: string;
  up: boolean;
  role: Role;
  term: number;
  leader: string;
  commit: number;
  last: number;
  log: [number, string][]; // last entries as [term, command]
  swim: Record<string, SwimState>; // this node's view of everyone else
}

export interface ConsensusSnapshot extends Base {
  nodes: ConsensusNode[];
  leader: string | null;
}

export interface WorldSnapshot extends Base {
  nodes: { id: string; up: boolean; leads: string[] }[];
  shards: { id: string; leader: string | null; term: number }[];
  agents: { id: string; x: number; y: number; shard: string }[];
  stats: { total: number; lost: number; duplicated: number };
  grid: { w: number; h: number; cols: number; rows: number };
}

export type Snapshot = ConsensusSnapshot | WorldSnapshot;

export type ToWorker =
  | { type: "init"; base: string }
  | { type: "start" | "stop" | "reset"; cluster: ClusterName }
  | { type: "call"; cluster: ClusterName; method: string; args: unknown[] };

export type FromWorker =
  | { type: "progress"; stage: string }
  | { type: "ready" }
  | { type: "snapshot"; cluster: ClusterName; data: string }
  | { type: "error"; message: string };
