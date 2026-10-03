import type { FeedEvent } from "../engine/types";

const DOT: Record<FeedEvent["k"], string> = {
  leader: "var(--accent)",
  election: "var(--blue)",
  stepdown: "var(--blue)",
  suspect: "var(--amber)",
  dead: "var(--red)",
  recover: "var(--accent)",
  user: "var(--text)",
  write: "var(--accent)",
};

export function EventFeed({ events, label }: { events: FeedEvent[]; label: string }) {
  return (
    <div className="feed">
      <h3 className="panel-title">{label}</h3>
      <ol className="feed-list" role="log" aria-live="polite" aria-relevant="additions">
        {events.length === 0 && <li className="feed-empty">Waiting for the first event…</li>}
        {[...events].reverse().map((e, i) => (
          <li key={`${e.t}-${events.length - i}`}>
            <span className="feed-dot" style={{ background: DOT[e.k] }} aria-hidden="true" />
            <span className="feed-text">{e.text}</span>
            <time className="feed-time mono">+{e.t.toFixed(1)}s</time>
          </li>
        ))}
      </ol>
    </div>
  );
}
