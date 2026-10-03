import { useEffect } from "react";
import { engine, useEngine } from "../engine/client";
import type { WorldSnapshot } from "../engine/types";
import { useSeen } from "../lib/useInView";
import { useMedia } from "../lib/useMedia";
import type { Pt } from "../lib/cluster";
import { Boot } from "./Boot";
import { EventFeed } from "./EventFeed";
import { PacketLayer } from "./PacketLayer";

const G = { x: 8, y: 8, size: 400 };
const OWNER = ["#5eead4", "#7aa7ff", "#c9a7ff"];
const wideServer = (i: number): Pt => ({ x: 548, y: 82 + i * 128 });
const narrowServer = (i: number): Pt => ({ x: 76 + i * 132, y: 462 });

/** Mid hand-off an agent is briefly in two shards; draw it once, in the shard its position belongs to. */
function distinctAgents(snap: WorldSnapshot) {
  const { w, h, cols, rows } = snap.grid;
  const home = (a: { x: number; y: number }) => `${Math.min(Math.max(Math.floor(a.y / (h / rows)), 0), rows - 1)}-${Math.min(Math.max(Math.floor(a.x / (w / cols)), 0), cols - 1)}`;
  const byId = new Map<string, WorldSnapshot["agents"][number]>();
  for (const a of snap.agents) if (!byId.has(a.id) || a.shard === home(a)) byId.set(a.id, a);
  return [...byId.values()];
}

export function World() {
  const { world, status } = useEngine();
  const [ref, seen] = useSeen<HTMLElement>();
  const snap: WorldSnapshot | null = world.snapshot;
  const live = snap && status === "ready";
  const narrow = useMedia("(max-width: 640px)");
  const serverPos = narrow ? narrowServer : wideServer;

  useEffect(() => {
    if (seen) engine.start("world");
  }, [seen]);

  const idx = (id: string) => Math.max(0, snap?.nodes.findIndex((n) => n.id === id) ?? 0);
  const color = (id: string | null) => (id ? OWNER[idx(id)] : "var(--faint)");
  const cell = G.size / (snap?.grid.cols ?? 2);
  const scale = G.size / (snap?.grid.w ?? 20);
  const call = (method: string, ...args: unknown[]) => engine.call("world", method, ...args);
  const serverAt = (id: string) => {
    const i = snap?.nodes.findIndex((n) => n.id === id) ?? -1;
    return i < 0 ? undefined : serverPos(i);
  };
  const stats = snap?.stats;
  const intact = stats && stats.lost === 0;

  return (
    <section id="world" aria-labelledby="world-h" ref={ref}>
      <div className="wrap">
        <p className="eyebrow">02 · Sharded world</p>
        <h2 id="world-h">Kill a server. Lose nothing.</h2>
        <p className="lede">
          A 2×2 world is split into four shards. Each shard is its own Raft group, and its leader owns that region. Agents wander across borders and get <strong>handed off</strong> between shards. Crash a region server and watch ownership move while the counter above stays honest.
        </p>

        <div className="panel playground">
          <div className="panel-head">
            <span className="live"><i /> live · same engine, 4 Raft groups</span>
            <p className={`banner ${intact && snap?.nodes.some((n) => n.up) ? "ok" : "stalled"}`} role="status">
              <span className="banner-dot" aria-hidden="true" />
              {snap && !snap.nodes.some((n) => n.up) ? (
                "All servers are down. Their state is kept, but nothing runs until one restarts."
              ) : stats ? (
                <>
                  <b className="mono">{stats.total - stats.lost}/{stats.total}</b>&nbsp;agents · lost <b className="mono">{stats.lost}</b> · in transit <b className="mono">{stats.duplicated}</b>
                </>
              ) : (
                "Waiting for the first snapshot"
              )}
            </p>
          </div>

          <div className="play-grid">
            <div className="stage">
              <svg viewBox={narrow ? "0 0 416 500" : "0 0 640 416"} role="group" aria-label="Sharded world" className="ring">
                {snap?.shards.map((s) => {
                  const [row, col] = s.id.split("-").map(Number);
                  const c = color(s.leader);
                  return (
                    <g key={s.id}>
                      <rect className="shard" x={G.x + col * cell + 3} y={G.y + row * cell + 3} width={cell - 6} height={cell - 6} rx={14} fill={c} fillOpacity={s.leader ? 0.07 : 0.02} stroke={c} strokeOpacity={s.leader ? 0.5 : 0.3} strokeDasharray={s.leader ? undefined : "5 5"} />
                      <text className="shard-label mono" x={G.x + col * cell + 16} y={G.y + row * cell + 28} fill={c}>
                        shard {s.id}
                      </text>
                      <text className="shard-sub mono" x={G.x + col * cell + 16} y={G.y + row * cell + 46} fill="var(--muted)">
                        {s.leader ? `led by ${s.leader} · term ${s.term}` : snap?.nodes.some((n) => n.up) ? "electing a leader…" : "no leader · no server up"}
                      </text>
                    </g>
                  );
                })}
                {snap && distinctAgents(snap).map((a) => (
                  <circle key={a.id} className="agent" r={5.5} cx={0} cy={0} fill={color(snap.shards.find((s) => s.id === a.shard)?.leader ?? null)} style={{ transform: `translate(${G.x + a.x * scale}px, ${G.y + a.y * scale}px)` }}>
                    <title>{a.id}</title>
                  </circle>
                ))}
                {snap && <PacketLayer packets={world.packets} pos={serverAt} ms={700} />}
                {snap?.nodes.map((n, i) => {
                  const p = serverPos(i);
                  const toggle = () => call(n.up ? "kill" : "revive", n.id);
                  return (
                    <g
                      key={n.id}
                      className={`server ${n.up ? "" : "down"}`}
                      transform={`translate(${p.x - 52} ${p.y - 34})`}
                      role="button"
                      tabIndex={0}
                      aria-label={`Region server ${n.id}, ${n.up ? `leads ${n.leads.length} shards. Press to crash` : "crashed. Press to restart"}`}
                      onClick={toggle}
                      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), toggle())}
                    >
                      <rect width={104} height={68} rx={12} fill="var(--surface)" stroke={n.up ? OWNER[i] : "var(--faint)"} strokeWidth={1.5} strokeDasharray={n.up ? undefined : "4 4"} />
                      <text className="node-id mono" x={52} y={28} textAnchor="middle" fill={n.up ? "var(--text)" : "var(--faint)"}>{n.id}</text>
                      <text className="node-sub mono" x={52} y={48} textAnchor="middle" fill={n.up ? OWNER[i] : "var(--faint)"}>
                        {n.up ? `owns ${n.leads.length} shard${n.leads.length === 1 ? "" : "s"}` : "crashed"}
                      </text>
                    </g>
                  );
                })}
              </svg>
              {!live && <Boot />}
              <ul className="legend" aria-label="Legend">
                <li><i style={{ background: "var(--accent)" }} /> agent (colour = owning server)</li>
                <li><i style={{ background: "var(--amber)" }} /> hand-off between servers</li>
              </ul>
            </div>

            <div className="side">
              <div className="controls" role="group" aria-label="Chaos controls">
                <button className="btn danger" disabled={!live} onClick={() => call("kill_busiest")}>
                  Kill the busiest server
                </button>
                <button className="btn" disabled={!live} onClick={() => engine.reset("world")}>
                  Reset
                </button>
              </div>
              <p className="hint">Tip: click a server to crash or restart it. Crashing two of three leaves no majority, and the world freezes safely until one returns.</p>
              <EventFeed events={world.feed} label="Who owns what" />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
