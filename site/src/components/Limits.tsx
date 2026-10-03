const does = [
  "Detects failed nodes with SWIM: ping, ping-req, suspicion and refutation",
  "Elects one leader and replicates a log with Raft, tested through crashes and partitions",
  "Keeps training with the surviving workers when pods are killed",
  "Hands agents between shards with fencing, with a live lost-agent counter",
];

const doesNot = [
  "Persist anything: the Raft log is in memory, so a full restart loses state",
  "Change membership at runtime: peers are fixed at startup",
  "Match real systems on speed: it's Python, built to show correctness, not throughput",
  "Secure its traffic: gRPC runs without TLS or authentication",
  "Prove the training claim at scale: a tiny CPU model, 5 workers, one run per setting, and pod deletions standing in for real spot preemptions",
];

export function Limits() {
  return (
    <section id="limits" aria-labelledby="limits-h">
      <div className="wrap">
        <p className="eyebrow">05 · Honest limits</p>
        <h2 id="limits-h">What this is, and what it isn't.</h2>
        <p className="lede">
          Anchor is a from-scratch implementation, built to understand these systems and to measure how they fail. It is <strong>not a replacement for etcd, Consul or Kafka</strong>, and I wouldn't run anything important on it yet.
        </p>
        <div className="cards two">
          <article>
            <h3>What it does</h3>
            <ul className="ticks yes">
              {does.map((t) => <li key={t}>{t}</li>)}
            </ul>
          </article>
          <article>
            <h3>What it doesn't (yet)</h3>
            <ul className="ticks no">
              {doesNot.map((t) => <li key={t}>{t}</li>)}
            </ul>
          </article>
        </div>
        <p className="next">
          <b>Next:</b> a write-ahead log on disk, membership changes, and a measured comparison against checkpoint-and-restart on real spot GPUs.
        </p>
      </div>
    </section>
  );
}
