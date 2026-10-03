import { useEngine } from "../engine/client";
import { ringPoint } from "../lib/cluster";
import { PacketLayer } from "./PacketLayer";

const S = 360;
const pts = Array.from({ length: 5 }, (_, i) => ringPoint(i, 5, S / 2, S / 2, 120));
const REPO = "https://github.com/rkothari3/DSSD";

function Orbit() {
  const { consensus, status } = useEngine();
  const snap = consensus.snapshot;
  const pos = (id: string) => pts[Number(id.slice(1)) - 1];
  return (
    <figure className="orbit" aria-hidden="true">
      <svg viewBox={`0 0 ${S} ${S}`}>
        {pts.map((a, i) => pts.slice(i + 1).map((b, j) => <line key={`${i}-${j}`} className="edge" x1={a.x} y1={a.y} x2={b.x} y2={b.y} />))}
        {snap && <PacketLayer packets={consensus.packets} pos={pos} ms={600} max={30} />}
        {pts.map((p, i) => {
          const n = snap?.nodes[i];
          const lead = n?.role === "leader" && n.up;
          const cand = n?.role === "candidate" && n.up;
          const stroke = !n ? "var(--border-strong)" : !n.up ? "var(--faint)" : lead ? "var(--accent)" : cand ? "var(--blue)" : "var(--border-strong)";
          return (
            <g key={i} transform={`translate(${p.x} ${p.y})`}>
              {lead && <circle className="halo" r={30} fill="none" stroke="var(--accent)" />}
              <circle r={16} fill={lead ? "var(--accent-dim)" : "var(--surface)"} stroke={stroke} strokeWidth={lead ? 2.5 : 1.5} strokeDasharray={n && !n.up ? "3 3" : undefined} />
              <text className="orbit-id mono" textAnchor="middle" y={4} fill={n && !n.up ? "var(--faint)" : "var(--muted)"}>{`n${i + 1}`}</text>
            </g>
          );
        })}
      </svg>
      <figcaption className="mono">
        <i className={status === "ready" ? "on" : ""} />
        {status === "ready" ? "live · real Python in this tab" : status === "error" ? "engine unavailable" : "starting Python…"}
      </figcaption>
    </figure>
  );
}

export function Hero() {
  return (
    <header className="hero">
      <div className="wrap hero-grid">
        <div>
          <p className="eyebrow">Anchor · distributed systems from scratch</p>
          <h1>Break a distributed system. Watch it heal.</h1>
          <p className="lede">
            Five computers must always agree who is in charge, even while some of them die. This page runs <strong>the real implementation</strong> live in your browser. Crash a node, cut the network, and see what survives.
          </p>
          <div className="cta">
            <a className="btn primary" href="#consensus">Break it</a>
            <a className="btn" href={REPO} target="_blank" rel="noreferrer">View source</a>
          </div>
          <ul className="chips" aria-label="What's inside">
            <li>SWIM failure detection</li>
            <li>Raft consensus</li>
            <li>DiLoCo training</li>
            <li>Sharded simulation</li>
          </ul>
        </div>
        <Orbit />
      </div>
    </header>
  );
}
