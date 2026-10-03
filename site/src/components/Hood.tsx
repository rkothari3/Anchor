const SRC = "https://github.com/rkothari3/DSSD/blob/main/src/dssd/";

const layers = [
  { name: "Applications", body: "DiLoCo trainer · sharded world", files: [["diloco/", "diloco"], ["sharding/", "sharding"]] },
  { name: "Raft", body: "leader election · replicated log · one group per shard", files: [["raft.py", "raft.py"]] },
  { name: "SWIM", body: "ping · ping-req · suspicion · refutation", files: [["swim.py", "swim.py"]] },
  { name: "Transport", body: "UDP + gRPC in production · an in-memory network here", files: [["playground.py", "playground.py"]] },
] as const;

const bugs = [
  { t: "Agents vanished on failover", b: "A new shard leader snapshotted its state before the committed log had been applied, so agents silently disappeared. It now waits until it has caught up." },
  { t: "Old entries never committed", b: "Raft only counts replicas for current-term entries (§5.4.2), so a new leader's inherited log sat uncommitted. Fix: append an empty no-op on election." },
  { t: "Two writers, one shard", b: "The movement tick raced with hand-offs and could write the same agent twice. A per-shard lock serialises them." },
];

export function Hood() {
  return (
    <section id="hood" aria-labelledby="hood-h">
      <div className="wrap">
        <p className="eyebrow">04 · Under the hood</p>
        <h2 id="hood-h">How real Python runs in your tab.</h2>
        <p className="lede">
          No re-implementation, no mock. The same <code>swim.py</code> and <code>raft.py</code> that the tests exercise run on <strong>Pyodide</strong> (CPython compiled to WebAssembly) inside a Web Worker, wired to an in-memory network that the buttons above can break.
        </p>

        <div className="hood-grid">
          <div>
            <h3 className="panel-title">Architecture</h3>
            <ol className="stack" aria-label="Architecture layers, top to bottom">
              {layers.map((l) => (
                <li key={l.name}>
                  <b>{l.name}</b>
                  <span>{l.body}</span>
                  <em>
                    {l.files.map(([label, path]) => (
                      <a key={path} className="mono" href={SRC + path} target="_blank" rel="noreferrer">{label}</a>
                    ))}
                  </em>
                </li>
              ))}
            </ol>
          </div>
          <div>
            <h3 className="panel-title">The seam that makes it possible</h3>
            <pre className="code" tabIndex={0}><code>{`# swim.py: the network is pluggable
def __init__(self, config, listen=None):
    self._listen = listen

async def start(self):
    if self._listen is not None:      # browser: in-memory network
        self._transport = await self._listen(_Protocol(self))
    else:                             # default: a real UDP socket
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self), local_addr=local)`}</code></pre>
            <p className="hint">Raft already took a <code>Transport</code> protocol, so the same trick needed no change there. The browser only needs a tiny <code>grpc</code> stub, because generated protobuf code imports it.</p>
          </div>
        </div>

        <h3 className="panel-title spaced">Bugs the tests caught along the way</h3>
        <div className="cards three">
          {bugs.map((b) => (
            <article key={b.t}>
              <h3>{b.t}</h3>
              <p>{b.b}</p>
            </article>
          ))}
        </div>

        <div className="stat-row">
          <div className="stat"><b className="mono">51</b><span>automated tests</span></div>
          <div className="stat"><b className="mono">4</b><span>Raft groups running in the sharded world</span></div>
          <div className="stat"><b className="mono">0</b><span>servers behind this page. It all runs in your tab</span></div>
        </div>
      </div>
    </section>
  );
}
