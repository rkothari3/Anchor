import { useEffect, useMemo, useRef, useState } from "react";
import data from "../data/training.json";

type Point = [number, number | null];
interface Run {
  kills: number;
  points: Point[];
}
const runs = data.runs as Run[];
const COLOR: Record<number, string> = { 0: "var(--accent)", 5: "var(--blue)", 20: "var(--red)" };

const DURATION = 240; // every run is a 240 s experiment; kills are evenly spaced across it (experiment.py:kill_schedule)
const W = 760;
const H = 360;
const M = { l: 48, r: 16, t: 30, b: 46 };
const YMAX = 1.8;
const x = (t: number) => M.l + (t / DURATION) * (W - M.l - M.r);
const y = (v: number) => M.t + (1 - v / YMAX) * (H - M.t - M.b);

/** A line that breaks wherever the worker was down (loss was null). */
function path(points: Point[], upTo: number) {
  let d = "";
  let pen = false;
  for (const [t, v] of points) {
    if (t > upTo) break;
    if (v == null) {
      pen = false;
      continue;
    }
    d += `${pen ? "L" : "M"}${x(t).toFixed(1)} ${y(v).toFixed(1)}`;
    pen = true;
  }
  return d;
}

const lossAt = (r: Run, t: number) => {
  let last: number | null = null;
  for (const [pt, v] of r.points) {
    if (pt > t) break;
    if (v != null) last = v;
  }
  return last;
};

const killTimes = (k: number) => Array.from({ length: k }, (_, i) => (DURATION / (k + 1)) * (i + 1));

export function Training() {
  const [t, setT] = useState(DURATION);
  const [playing, setPlaying] = useState(false);
  const raf = useRef(0);

  useEffect(() => {
    if (!playing) return;
    let last = performance.now();
    const step = (now: number) => {
      const dt = (now - last) / 1000;
      last = now;
      setT((cur) => {
        const next = cur + dt * (DURATION / 14);
        if (next >= DURATION) {
          setPlaying(false);
          return DURATION;
        }
        return next;
      });
      raf.current = requestAnimationFrame(step);
    };
    raf.current = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf.current);
  }, [playing]);

  const finals = useMemo(() => runs.map((r) => ({ kills: r.kills, loss: lossAt(r, DURATION)! })), []);
  const ticks = [0, 60, 120, 180, 240];
  const yTicks = [0, 0.6, 1.2, 1.8];

  return (
    <section id="training" aria-labelledby="training-h">
      <div className="wrap">
        <p className="eyebrow">03 · Training under chaos</p>
        <h2 id="training-h">Kill the workers. Training keeps going.</h2>
        <p className="lede">
          Fault-tolerant <strong>DiLoCo</strong> on a local Kubernetes cluster: 3 workers train a small model, and random pods are deleted mid-run. These are the <strong>recorded runs</strong> (PyTorch can't run in a browser), straight from <code>results/*.csv</code>.
        </p>

        <div className="panel chart-panel">
          <div className="chart-head">
            <ul className="series" aria-label="Series">
              {runs.map((r) => (
                <li key={r.kills}>
                  <i style={{ background: COLOR[r.kills] }} />
                  {r.kills === 0 ? "No kills" : `${r.kills} pod kills`}
                  <b className="mono">{lossAt(r, t)?.toFixed(2) ?? "…"}</b>
                </li>
              ))}
            </ul>
            <div className="replay">
              <button className="btn" onClick={() => (t >= DURATION && setT(0), setPlaying(!playing))} aria-pressed={playing}>
                {playing ? "Pause" : t >= DURATION ? "Replay" : "Play"}
              </button>
              <label className="sr-only" htmlFor="scrub">Replay position, seconds</label>
              <input id="scrub" type="range" min={0} max={DURATION} step={1} value={Math.round(t)} onChange={(e) => (setPlaying(false), setT(+e.target.value))} />
              <span className="mono scrub-t">{Math.round(t)}s</span>
            </div>
          </div>

          <svg viewBox={`0 0 ${W} ${H}`} className="chart" role="img" aria-label={`Loss versus wall-clock time for three training runs. Final loss: ${finals.map((f) => `${f.kills} kills ${f.loss.toFixed(2)}`).join(", ")}.`}>
            {yTicks.map((v) => (
              <g key={v}>
                <line className="grid" x1={M.l} x2={W - M.r} y1={y(v)} y2={y(v)} />
                <text className="axis mono" x={M.l - 10} y={y(v) + 4} textAnchor="end">{v.toFixed(1)}</text>
              </g>
            ))}
            {ticks.map((s) => (
              <text key={s} className="axis mono" x={x(s)} y={H - M.b + 20} textAnchor="middle">{s}s</text>
            ))}
            <text className="axis-title mono" x={M.l - 38} y={12} textAnchor="start">loss</text>
            {runs.map((r, ri) =>
              killTimes(r.kills).map((k) => (
                <line key={`${r.kills}-${k}`} className="kill" x1={x(k)} x2={x(k)} y1={H - M.b + 28 + ri * 4} y2={H - M.b + 34 + ri * 4} stroke={COLOR[r.kills]} />
              )),
            )}
            {[...runs].reverse().map((r) => (
              <path key={r.kills} d={path(r.points, t)} fill="none" stroke={COLOR[r.kills]} strokeWidth={2.2} strokeLinejoin="round" strokeLinecap="round" />
            ))}
            <line className="cursor" x1={x(t)} x2={x(t)} y1={M.t} y2={H - M.b} />
            {runs.map((r) => {
              const v = lossAt(r, t);
              return v == null ? null : <circle key={r.kills} cx={x(t)} cy={y(v)} r={4} fill={COLOR[r.kills]} stroke="var(--bg)" strokeWidth={2} />;
            })}
          </svg>
          <p className="chart-note">Ticks under the axis mark each pod kill (evenly spaced across the 240 s run). Gaps are the moments a killed worker was restarting.</p>
        </div>

        <div className="stat-row">
          {finals.map((f) => (
            <div className="stat" key={f.kills}>
              <b className="mono" style={{ color: COLOR[f.kills] }}>{f.loss.toFixed(2)}</b>
              <span>final loss · {f.kills === 0 ? "no kills" : `${f.kills} kills`}</span>
            </div>
          ))}
          <p className="stat-note">More kills mean slower convergence, because each kill throws away a worker's in-flight progress. They never stop training or force a restart.</p>
        </div>

        <div className="cards three">
          <article>
            <h3>Inner loop</h3>
            <p>Each worker runs 20 local AdamW steps on its own shard of data. No network, no waiting, so communication stays rare.</p>
          </article>
          <article>
            <h3>Outer step</h3>
            <p>Workers submit how far they moved. The leader averages those pseudo-gradients and applies them with Nesterov SGD, then everyone restarts from the same model.</p>
          </article>
          <article>
            <h3>Why kills don't hang it</h3>
            <p>The round barrier waits for every worker SWIM says is alive, and closes on a timeout with whoever answered. A dead worker shrinks the round instead of freezing it.</p>
          </article>
        </div>
      </div>
    </section>
  );
}
