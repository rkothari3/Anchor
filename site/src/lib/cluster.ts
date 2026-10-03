import type { ConsensusNode, ConsensusSnapshot, SwimState } from "../engine/types";

export interface Pt {
  x: number;
  y: number;
}

export const ringPoint = (i: number, n: number, cx: number, cy: number, r: number): Pt => {
  const a = (i / n) * Math.PI * 2 - Math.PI / 2;
  return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
};

/** What the rest of the cluster currently believes about a node (SWIM). */
export function belief(id: string, snap: ConsensusSnapshot): SwimState {
  let worst: SwimState = "alive";
  for (const n of snap.nodes) {
    if (!n.up || n.id === id) continue;
    const s = n.swim[id];
    if (s === "dead") return "dead";
    if (s === "suspect") worst = "suspect";
  }
  return worst;
}

export const partitioned = (snap: ConsensusSnapshot) => Object.keys(snap.net.side).length > 0;

export type Quorum =
  | { kind: "ok"; reachable: number; total: number; leader: string }
  | { kind: "electing"; reachable: number; total: number }
  | { kind: "stalled"; reachable: number; total: number };

/** Can the cluster commit right now? Needs a leader that can reach a majority. */
export function quorum(snap: ConsensusSnapshot): Quorum {
  const total = snap.nodes.length;
  const need = Math.floor(total / 2) + 1;
  const bySide = new Map<number, number>();
  for (const n of snap.nodes) {
    if (!n.up) continue;
    const s = snap.net.side[n.id] ?? 0;
    bySide.set(s, (bySide.get(s) ?? 0) + 1);
  }
  const largest = Math.max(0, ...bySide.values());
  if (largest < need) return { kind: "stalled", reachable: largest, total };
  const leader = snap.leader;
  if (!leader) return { kind: "electing", reachable: largest, total };
  const reachable = bySide.get(snap.net.side[leader] ?? 0) ?? 0;
  return reachable >= need ? { kind: "ok", reachable, total, leader } : { kind: "electing", reachable: largest, total };
}

export const roleLabel = (n: ConsensusNode) => (n.up ? n.role : "crashed");
