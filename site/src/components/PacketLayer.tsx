import { useEffect, useRef } from "react";
import type { LivePacket } from "../engine/client";
import type { Pt } from "../lib/cluster";

const COLOR: Record<string, string> = {
  ping: "#8d8d99",
  ack: "#8d8d99",
  "ping-req": "#b9b9c4",
  join: "#8d8d99",
  heartbeat: "var(--accent)",
  append: "var(--accent)",
  reply: "rgba(94,234,212,0.55)",
  vote: "var(--blue)",
  handoff: "var(--amber)",
};

const reducedMotion = () => typeof matchMedia === "function" && matchMedia("(prefers-reduced-motion: reduce)").matches;

function Dot({ packet, a, b, ms }: { packet: LivePacket; a: Pt; b: Pt; ms: number }) {
  const ref = useRef<SVGCircleElement>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el?.animate) return;
    const reach = packet.ok ? 1 : 0.5; // dropped packets die half way
    const at = (t: number) => `translate(${a.x + (b.x - a.x) * t}px, ${a.y + (b.y - a.y) * t}px)`;
    const anim = el.animate(
      [
        { transform: at(0), opacity: 0 },
        { transform: at(reach * 0.12), opacity: 1, offset: 0.12 },
        { transform: at(reach), opacity: packet.ok ? 1 : 0.9, offset: 0.85 },
        { transform: at(reach), opacity: 0 },
      ],
      { duration: reducedMotion() ? 400 : ms, easing: "ease-in-out", fill: "forwards" },
    );
    return () => anim.cancel();
  }, [a.x, a.y, b.x, b.y, ms, packet.ok]);
  const protocol = packet.p === "swim" ? 2.6 : 3.4;
  return <circle ref={ref} r={protocol} fill={packet.ok ? COLOR[packet.k] ?? "#8d8d99" : "var(--red)"} style={{ opacity: 0 }} />;
}

/** Animated message dots between node positions. Purely cosmetic: the real delivery already happened. */
export function PacketLayer({ packets, pos, ms = 550, max = 40 }: { packets: LivePacket[]; pos: (id: string) => Pt | undefined; ms?: number; max?: number }) {
  return (
    <g aria-hidden="true" pointerEvents="none">
      {packets.slice(-max).map((p) => {
        const a = pos(p.a);
        const b = pos(p.b);
        return a && b ? <Dot key={p.i} packet={p} a={a} b={b} ms={ms} /> : null;
      })}
    </g>
  );
}
