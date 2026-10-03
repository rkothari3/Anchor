import { useEffect, useMemo, useState } from "react";
import { engine, useEngine } from "../engine/client";
import type { ConsensusNode, ConsensusSnapshot } from "../engine/types";
import { belief, partitioned, quorum, ringPoint, roleLabel, type Pt } from "../lib/cluster";
import { Boot } from "./Boot";
import { EventFeed } from "./EventFeed";
import { PacketLayer } from "./PacketLayer";

const H = 440;
const C = { x: 280, y: H / 2 + 6 };
const R = 158;
const SPLIT = [["n1", "n2"], ["n3", "n4", "n5"]];
const DEFAULT_LATENCY = 0.08;

const ROLE_COLOR = { leader: "var(--accent)", candidate: "var(--blue)", follower: "var(--text)" } as const;

function Node({ n, at, snap, valid }: { n: ConsensusNode; at: Pt; snap: ConsensusSnapshot; valid: boolean }) {
  const b = belief(n.id, snap);
  // A leader that can't reach a majority (or was out-termed) keeps its title until it hears a newer term.
  const stale = n.up && n.role === "leader" && !valid;
  const color = !n.up ? "var(--faint)" : stale ? "var(--amber)" : ROLE_COLOR[n.role];
  const toggle = () => (n.up ? engine.call("consensus", "kill", n.id) : engine.call("consensus", "revive", n.id));
  const label = `${n.id}, ${stale ? "stale leader" : roleLabel(n)}, term ${n.term}. ${n.up ? "Press to crash this node" : "Press to restart this node"}`;
  const beliefColor = b === "dead" ? "var(--red)" : b === "suspect" ? "var(--amber)" : "var(--faint)";
  return (
    <g transform={`translate(${at.x} ${at.y})`}>
      {n.role === "leader" && n.up && !stale && <circle className="halo" r={46} fill="none" stroke="var(--accent)" aria-hidden="true" pointerEvents="none" />}
    <g
      className={`node ${n.up ? "" : "down"}`}
      role="button"
      tabIndex={0}
      aria-label={label}
      onClick={toggle}
      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), toggle())}
    >
      <circle r={34} className="node-body" fill={n.role === "leader" && n.up && !stale ? "var(--accent-dim)" : "var(--surface)"} stroke={color} strokeWidth={n.role === "follower" || !n.up ? 1.5 : 2.5} strokeDasharray={n.up ? undefined : "4 4"} />
      {b !== "alive" && <circle r={40} fill="none" stroke={beliefColor} strokeWidth={1.5} strokeDasharray="2 5" strokeLinecap="round" />}
      <text className="node-id mono" textAnchor="middle" y={-3} fill={n.up ? "var(--text)" : "var(--faint)"}>
        {n.id}
      </text>
      <text className="node-sub mono" textAnchor="middle" y={13} fill={n.up ? color : "var(--faint)"}>
        {n.up ? `term ${n.term}` : "crashed"}
      </text>
      <text className="node-role" textAnchor="middle" y={-52} fill={color}>
        {stale ? "stale leader" : n.up ? n.role : ""}
      </text>
      {b !== "alive" && (
        <text className="node-belief mono" textAnchor="middle" y={60} fill={beliefColor}>
          {b === "dead" ? "declared dead" : "suspected"}
        </text>
      )}
    </g>
    </g>
  );
}

function Banner({ snap }: { snap: ConsensusSnapshot }) {
  const q = quorum(snap);
  const text =
    q.kind === "ok"
      ? `Quorum · ${q.reachable} of ${q.total} nodes reach leader ${q.leader}. Writes commit.`
      : q.kind === "electing"
        ? "No leader yet · nodes are holding an election."
        : `No majority · only ${q.reachable} of ${q.total} nodes can talk. The cluster stalls instead of risking split-brain.`;
  return (
    <p className={`banner ${q.kind}`} role="status">
      <span className="banner-dot" aria-hidden="true" />
      {text}
    </p>
  );
}

function Logs({ snap }: { snap: ConsensusSnapshot }) {
  return (
    <div className="logs">
      <h3 className="panel-title">Replicated log, per node</h3>
      {snap.nodes.map((n) => {
        const first = n.last - n.log.length + 1;
        return (
          <div className={`log-row ${n.up ? "" : "off"}`} key={n.id}>
            <span className="log-id mono">{n.id}</span>
            <span className="log-chips">
              {n.log.length === 0 && <span className="log-none">empty</span>}
              {n.log.map(([term, cmd], i) => (
                <span key={first + i} className={`chip ${first + i <= n.commit ? "committed" : ""}`} title={`index ${first + i}, term ${term}${cmd === "·" ? ", leader no-op" : ""}`}>
                  <i>{term}</i>
                  {cmd}
                </span>
              ))}
            </span>
            <span className="log-meta mono">{n.last}/{n.commit}</span>
          </div>
        );
      })}
      <p className="log-key">
        <span className="chip committed"><i>t</i>cmd</span> committed (on a majority) <span className="chip"><i>t</i>cmd</span> not yet · numbers: term · right: last/committed index
      </p>
    </div>
  );
}

export function Consensus() {
  const { consensus, status } = useEngine();
  const snap = consensus.snapshot;
  const [loss, setLoss] = useState(0);
  const [latency, setLatency] = useState(DEFAULT_LATENCY);
  const [writes, setWrites] = useState(0);

  useEffect(() => {
    const go = () => engine.start("consensus");
    const id = "requestIdleCallback" in window ? requestIdleCallback(go, { timeout: 1500 }) : setTimeout(go, 300);
    return () => ("cancelIdleCallback" in window ? cancelIdleCallback(id as number) : clearTimeout(id as number));
  }, []);

  const pos = useMemo(() => {
    const ids = snap?.nodes.map((n) => n.id) ?? [];
    const m = new Map(ids.map((id, i) => [id, ringPoint(i, ids.length, C.x, C.y, R)]));
    return (id: string) => m.get(id);
  }, [snap?.nodes.length]);

  const cut = snap ? partitioned(snap) : false;
  const q = snap ? quorum(snap) : null;
  const live = snap && status === "ready";
  const call = (method: string, ...args: unknown[]) => engine.call("consensus", method, ...args);

  return (
    <section id="consensus" aria-labelledby="consensus-h">
      <div className="wrap">
        <p className="eyebrow">01 · Consensus</p>
        <h2 id="consensus-h">Five nodes. One leader. Break it.</h2>
        <p className="lede">
          This is <strong>the real <code>swim.py</code> and <code>raft.py</code></strong>, running in your browser. Crash a node, cut the network in two, drop packets. The cluster has to elect a leader and keep every committed write.
        </p>

        <div className="panel playground">
          <div className="panel-head">
            <span className="live"><i /> live · CPython on WebAssembly</span>
            {live && <Banner snap={snap} />}
          </div>

          <div className="play-grid">
            <div className="stage">
              <svg viewBox="80 0 400 430" role="group" aria-label="Five-node cluster" className={`ring ${cut ? "cut" : ""}`}>
                {snap && cut && (
                  <g aria-hidden="true">
                    <line className="split" x1={C.x - 120} y1={32} x2={C.x + 20} y2={H - 14} />
                    <text className="split-label mono" x={C.x - 190} y={22}>network split</text>
                  </g>
                )}
                {snap &&
                  snap.nodes.flatMap((a, i) =>
                    snap.nodes.slice(i + 1).map((b) => (
                      <line key={a.id + b.id} className="edge" x1={pos(a.id)!.x} y1={pos(a.id)!.y} x2={pos(b.id)!.x} y2={pos(b.id)!.y} />
                    )),
                  )}
                {snap && <PacketLayer packets={consensus.packets} pos={pos} />}
                {snap?.nodes.map((n) => <Node key={n.id} n={n} at={pos(n.id)!} snap={snap} valid={q?.kind === "ok" && q.leader === n.id} />)}
              </svg>
              {!live && <Boot />}
              <ul className="legend" aria-label="Legend">
                <li><i style={{ background: "var(--accent)" }} /> leader / heartbeat</li>
                <li><i style={{ background: "var(--blue)" }} /> election vote</li>
                <li><i style={{ background: "#8d8d99" }} /> SWIM ping</li>
                <li><i style={{ background: "var(--amber)" }} /> suspected</li>
                <li><i style={{ background: "var(--red)" }} /> dropped / dead</li>
              </ul>
            </div>

            <div className="side">
              <div className="controls" role="group" aria-label="Chaos controls">
                <button className="btn danger" disabled={!live || !snap.leader} onClick={() => call("kill_leader")}>
                  Kill the leader
                </button>
                <button className="btn" disabled={!live} onClick={() => (setWrites(writes + 1), call("write", `x=${writes + 1}`))}>
                  Write a value
                </button>
                <button className="btn" disabled={!live} aria-pressed={cut} onClick={() => call("partition", cut ? null : SPLIT)}>
                  {cut ? "Heal the network" : "Split network 2 | 3"}
                </button>
                <button
                  className="btn"
                  disabled={!live}
                  onClick={() => {
                    setLoss(0);
                    setLatency(DEFAULT_LATENCY);
                    setWrites(0);
                    engine.reset("consensus");
                  }}
                >
                  Reset
                </button>
              </div>

              <div className="sliders">
                <label>
                  <span>Packet loss <b className="mono">{Math.round(loss * 100)}%</b></span>
                  <input type="range" min={0} max={0.5} step={0.05} value={loss} disabled={!live} onChange={(e) => (setLoss(+e.target.value), call("set_loss", +e.target.value))} />
                </label>
                <label>
                  <span>Network latency <b className="mono">{Math.round(latency * 1000)} ms</b></span>
                  <input type="range" min={0.02} max={0.4} step={0.02} value={latency} disabled={!live} onChange={(e) => (setLatency(+e.target.value), call("set_latency", +e.target.value))} />
                </label>
              </div>
              <p className="hint">Tip: click or press <kbd>Enter</kbd> on any node to crash or restart it.</p>

              <EventFeed events={consensus.feed} label="What the cluster is doing" />
            </div>
          </div>

          {live && <Logs snap={snap} />}
        </div>
      </div>
    </section>
  );
}
