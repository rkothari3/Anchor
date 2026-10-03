# Distributed Systems Capstone Research Report: Fault-Tolerant Training + "Live City" (2026)

## Implementation Status

This report was the design document; the project, **Anchor**, was then built in Python on top of it (the repo is named `dssd`).

- **Implemented:** Stage 1 (SWIM failure detection + Raft leader election), Stage 2
  (DiLoCo training on a local `kind` cluster, surviving pod kills, with the
  loss-vs-wall-clock data at 0/5/20 kills in `results/`), and Stage 3 (spatial
  sharding: per-shard Raft ownership + fenced cross-shard agent hand-off).
- **Not implemented:** Stage 4 (optional real-cloud chaos).
- **Live demo of Anchor:** [rkothari3.github.io/DSSD](https://rkothari3.github.io/DSSD/). The real SWIM, Raft and sharding code runs in your browser (Pyodide) so you can crash nodes and split the network. Source in `site/`; `src/dssd/playground.py` is the in-memory network that drives it.

**Code map** (`src/dssd/`), in reading order:

| File | What it is |
|---|---|
| `swim.py` | SWIM failure detector: ping, ping-req, suspicion, refutation, gossip |
| `raft.py` | Raft leader election + log replication |
| `membership.py` | gRPC glue: Raft transport/servicer, `GetMembers` |
| `diloco/` | DiLoCo: `model.py`, `data.py`, `outer.py` (outer step), `trainer_service.py` (leader's round barrier), `worker.py` (`dssd-worker`), `experiment.py` (kill experiment) |
| `sharding/` | `world.py` (grid), `shard_state.py` (one shard = one Raft group), `region_server.py` (tick + hand-off), `handoff.py`, `cli.py` (`dssd-region`) |
| `proto/*.proto` | Wire formats; regenerate with `scripts/gen_protos.sh` |

Run the tests with `pip install -e ".[dev]" && pytest`.

## TL;DR
- **Build your OWN membership/failure-detection layer (SWIM-style gossip + heartbeats and a Raft-based leader election), and treat everything else — Kubernetes, gRPC, Prometheus/Grafana, PyTorch — as swappable plumbing.** The reusable "membership + quorum + failure-detection" service is both the genuine distributed-systems learning AND the architectural seam that lets Phase 1 and Phase 2 share a spine. This mirrors how Meta's `torchft` splits a standalone `torchft.coordination` module (Lighthouse + Manager quorum/heartbeat) from its training-specific ProcessGroup logic.
- **You can do ~90% of this for $0.** Local multi-node Kubernetes via `kind`, nanoGPT-scale training on a single consumer GPU or free tier, and chaos via real pod evictions all run on one laptop. You only need to pay for genuine multi-machine (multi-VM/multi-region) chaos, and student credits (GitHub Student Pack → $200 DigitalOcean, Azure for Students $100, Oracle Cloud always-free ARM) cover that scope.
- **The stack is current and hiring-relevant:** Kubernetes + Go/Python + gRPC + Prometheus/Grafana/OpenTelemetry is exactly what OpenAI, NVIDIA, xAI, Anduril, Databricks and Palantir list in 2026 infra postings. DiLoCo/local-SGD is a live 2026 research frontier (DeepMind's Decoupled DiLoCo, April 2026; `torchft` supports DiLoCo/Streaming DiLoCo natively).

---

## Key Findings

1. **The "real learning" is a small set of algorithms; the rest is tooling.** Core CS distributed-systems concepts you should implement yourself: failure detection (SWIM/heartbeat + suspicion), quorum/membership, leader election (Raft), sharding/partitioning, and consistency policy at shard boundaries. Everything else (K8s, gRPC, Prometheus, the dashboard, PyTorch DDP) is infrastructure you should *use*, not *reinvent*.
2. **DiLoCo is genuinely current (2026).** DeepMind published "Decoupled DiLoCo" on April 23, 2026; in simulations involving 1.2 million chips under high failure rates, it maintained a goodput of 88% compared to just 27% for standard Data-Parallel methods, validated by training a 12B-parameter Gemma model across four U.S. regions (Gemma 4 accuracy 64.1% vs 64.4% baseline). Open implementations exist: OpenDiLoCo (Prime Intellect, on Hivemind + PyTorch FSDP) and Meta's `torchft` (fault-tolerant DDP/HSDP/LocalSGD/DiLoCo/Streaming DiLoCo). You should implement the DiLoCo inner/outer loop yourself for learning, but read these as references.
3. **`torchft` is the single best architectural template for your shared spine.** It cleanly separates a general membership/quorum/heartbeat service (Lighthouse + Manager, exposed as a standalone `torchft.coordination` module) from training-specific all-reduce logic — exactly the Phase1/Phase2 reuse pattern you want.
4. **Cost floor is essentially $0 for the core deliverables**; only true multi-machine chaos needs cloud, and student credits comfortably cover it.
5. **The classic solo-builder traps** are: tuning failure-detector timeouts (too aggressive → false positives and spurious leader elections; too slow → split-brain window), forgetting fencing/epoch numbers (zombie leaders), and underestimating the plumbing (K8s RBAC, gRPC, dashboards) that isn't the "real" learning but eats the calendar.

---

## Details

### Q1 — TECH STACK (with "core learning" vs "plumbing" flags)

**Distributed training / DiLoCo implementation**
- *Current tools:* PyTorch is the base. Three reference points: (a) `torch.distributed` DDP/FSDP2 (the standard primitive); (b) **OpenDiLoCo** (Prime Intellect) — reproduces DeepMind's DiLoCo on the Hivemind library + PyTorch FSDP. Per Prime Intellect's paper (Hagemann et al., "OpenDiLoCo," arXiv:2407.07852): "We demonstrate its effectiveness by training a model across two continents and three countries, while maintaining 90-95% compute utilization" — using 4 worker nodes (two Canadian states, Finland, USA), each with 8×H100, scaled to a 1.1B-parameter model; (c) **Meta `torchft`** — "Fault tolerance for PyTorch (HSDP, LocalSGD, DiLoCo, Streaming DiLoCo)," implementing per-step fault tolerance. DiLoCo itself: inner optimizer AdamW does many local steps; an outer optimizer (SGD w/ Nesterov momentum) synchronizes "pseudo-gradients." Per the original DeepMind paper (Douillard et al., "DiLoCo: Distributed Low-Communication Training of Language Models," arXiv:2311.08105): "On the widely used C4 dataset, we show that DiLoCo on 8 workers performs as well as fully synchronous optimization while communicating 500 times less."
- *2026 research signal:* DeepMind's **Decoupled DiLoCo** (April 23, 2026) decouples compute into asynchronous "islands," reported at 88% goodput under high failure rates (vs 27% for standard data-parallel), combining Pathways + DiLoCo for multi-datacenter training.
- **CORE LEARNING:** the DiLoCo inner/outer loop and the pseudo-gradient all-reduce; how communication-efficient optimization interacts with membership changes.
- **PLUMBING:** PyTorch itself, NCCL/Gloo backends, checkpoint I/O.

**Membership / failure detection**
- *Current tools/patterns:* **SWIM** (Scalable Weakly-consistent Infection-style Process group Membership) is the canonical design. Per Das, Gupta & Motivala, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol," Proc. 2002 Int'l Conf. on Dependable Systems and Networks (DSN), pp. 303–312, DOI 10.1109/DSN.2002.1028914, SWIM "separates the failure detection and membership update dissemination functionalities." It uses UDP ping / ping-req / ack with piggybacked gossip, and a "suspicion" state to cut false positives. Production lineage: HashiCorp Serf/Consul/Nomad, Akka Cluster. `torchft` uses a simpler centralized heartbeat model (Lighthouse + per-group Manager) rather than gossip.
- **CORE LEARNING (the heart of the project):** implement SWIM (or a heartbeat+suspicion detector) yourself; understand time-bounded completeness, false-positive rate, and the suspicion mechanism.
- **PLUMBING:** the UDP socket layer, serialization (protobuf).

**Consensus / leader election (Phase 2)**
- *Current tools:* **Raft** is the standard understandable consensus algorithm. Per Diego Ongaro & John Ousterhout (Stanford), 2014 USENIX Annual Technical Conference, pp. 305–319 (Best Paper Award), Raft "separates the key elements of consensus, such as leader election, log replication, and safety." etcd (which underpins Kubernetes) uses Raft. Learn via the Raft paper ("In Search of an Understandable Consensus Algorithm"), the raft.github.io visualization, and implement against MIT 6.5840 (formerly 6.824) lab structure.
- **CORE LEARNING:** leader election, terms/epochs, split-brain avoidance, log replication. This is the marquee distributed-systems concept.
- **PLUMBING:** the RPC transport (gRPC), persistence.

**Chaos injection**
- *Current tools (2026):* **Chaos Mesh** (CNCF, by PingCAP) — CRD-driven, rich fault types (PodChaos, NetworkChaos, IOChaos, StressChaos, etc.), polished dashboard, DaemonSet-based; best for fault *depth* and quick start. **LitmusChaos** (CNCF) — ChaosHub of reusable experiments, ChaosCenter UI, GitOps workflows; best for orchestrating across teams/pipelines. For simplest local use, a plain `kubectl delete pod` or the Kubernetes eviction API is enough for the "kill a random node" button.
- **CORE LEARNING:** designing the steady-state hypothesis and observing recovery; almost none of the *tool* is "real" DS learning.
- **PLUMBING:** Chaos Mesh/Litmus themselves; the button's wiring.

**Spatial sharding (Phase 2)**
- *Patterns:* spatial partitioning divides the map into regions/zones so agents interact only with nearby agents; each region owned by a server process; cross-boundary interaction needs "ghost"/halo regions and agent migration (machine-unawareness, ghost-space management, cross-processor agent management are the known hard problems, per the MASS parallel-agent literature). SpatialOS is the well-known commercial precedent (now largely defunct/criticized for latency); academic references: MASS (UW), Repast HPC.
- **CORE LEARNING:** partitioning scheme, ownership/leases, agent hand-off protocol, and the consistency policy at boundaries (this is the Phase-2 analog of consensus).
- **PLUMBING:** the rendering/visualization, the movement model training.

**Agent movement model (Phase 2)**
- *Approach:* behavior cloning (supervised: map state→action from expert trajectories) or a lightweight RL policy. BC is simple but suffers distributional shift (compounding errors). Datasets: inD (drone trajectories at intersections), PIE (pedestrian), or synthetic social-force trajectories.
- **CORE LEARNING:** none really — this is application content, not distributed systems.
- **PLUMBING/APPLICATION:** the model + dataset.

**Dashboard / observability**
- *Current tools (2026):* **Prometheus + Grafana** remain the default open-source metrics+visualization stack; **OpenTelemetry** is the de-facto open standard for traces/metrics/logs and pairs with Grafana (LGTM stack: Loki/Grafana/Tempo/Mimir). Newer all-in-one options: SigNoz (OTel-native, ClickHouse), OpenObserve (cheap object-storage backend). For a live loss curve + node view, a simple custom web dashboard (WebSocket) is fine, backed by Prometheus for metrics.
- **CORE LEARNING:** none (observability is engineering craft, valuable but not DS theory).
- **PLUMBING:** all of it — but industry-relevant plumbing.

**Orchestration & RPC**
- Kubernetes is the orchestration substrate; gRPC is the RPC layer (used by `torchft` for both Lighthouse and Manager). Both are ubiquitous in the job postings below.

### Q2 — COST (cheapest realistic path)

**What runs $0, entirely local:**
- **Multi-node K8s locally:** `kind` (Kubernetes-in-Docker) is the best choice for *simulated* multi-node clusters — it runs multiple "node" containers on one host and is purpose-built for multi-node; config is a few lines (`role: control-plane` / `role: worker`). `minikube` multi-node is still experimental; `k3d` is the lightest. Real pod evictions on `kind` give genuine failure-detection/reroute behavior.
- **Training:** nanoGPT-scale models train on a single consumer GPU; multiple worker *processes* on one machine simulate DiLoCo workers. `torchft`'s own testing used consumer-like L40S (48GB) with TCP-only networking — you can go far smaller.
- **Chaos, dashboard, membership, Raft:** all free/local.

**Where you genuinely pay (and cheapest options):**
- True multi-VM / multi-region chaos (network partitions across real machines, real NIC failures) needs cloud. Cheapest realistic routes as of 2026:
  - **Oracle Cloud Free Tier** — the most generous always-free ARM compute, but note it was reduced recently: per Oracle's official docs ("Always Free Resources") and InfoQ (Jul 2026), effective June 15, 2026 the Always Free A1 allowance was halved to "the first 1,500 OCPU hours and 9,000 GB hours per month… equivalent to 2 OCPUs and 12 GB of memory" (down from 4 OCPU / 24 GB), plus 2 AMD micro instances. Still enough for a small 2–3 VM cluster at $0.
  - **GitHub Student Developer Pack** → **DigitalOcean $200 credit** (valid through 7/31/26 per current terms; excludes GPU droplets), plus **Azure for Students $100** (no credit card).
  - **AWS**: no student *credit* program per se — AWS Educate gives small credits ($75–$100 at member institutions) and the 12-month Free Tier is usage-based; use spot instances for chaos runs.
  - Google Cloud education credits via institution.
- **Rule of thumb:** do all development and the graded 0/5/20-kill plots on `kind` locally ($0); use a handful of cloud spot VMs only if you want a "real network partition" demo. Budget: $0–$20 realistically, fully covered by student credits.

### Q3 — LEARNING PATH (implementation-paired)

- **MIT 6.5840 (formerly 6.824), Spring 2026** — the gold standard; free complete materials (lectures, labs w/ test suites, exams+answers). Labs in Go: MapReduce → **Raft** → fault-tolerant KV → **sharded KV**. This directly maps onto your project (Raft = Phase 2 leader election; sharded KV = Phase 2 spatial shards). Pair the labs with Jon Gjengset's "Students' Guide to Raft" and the "Instructors' Guide to Raft."
- **Designing Data-Intensive Applications (Kleppmann), 2nd ed. (Feb 2026 update)** — best conceptual grounding on replication, partitioning, consistency, consensus; the 2nd edition adds AI/cloud-native, local-first sync, and reworked consistency/consensus sections.
- **Martin Kleppmann's Cambridge distributed-systems lecture series (YouTube, 8 lectures)** — approachable video companion to DDIA.
- **SWIM paper** (Das/Gupta/Motivala, DSN 2002) + HashiCorp's Go `memberlist`/Serf as a reference implementation — for the failure-detector.
- **Raft paper** (Ongaro/Ousterhout, USENIX ATC 2014) + raft.github.io visualization — for consensus.
- **DiLoCo papers:** original DeepMind DiLoCo (arXiv:2311.08105), OpenDiLoCo (arXiv:2407.07852), and Decoupled DiLoCo (2026) — for the training layer.
- **`torchft` README + design doc + the "Fault-Tolerant Llama" PyTorch blog** — for the shared-spine architecture.

### Q4 — SHARED ARCHITECTURE (reusing the membership layer across phases)

The right design is to build a **standalone membership/failure-detection/quorum service** with a clean, application-agnostic API, and have both phases depend on it. **`torchft` is the canonical example of this separation** and should be your template:

- **Two-tier coordination:** a single global **Lighthouse** (a Rust gRPC server) "coordinates across the different replica groups," and a per-replica-group **Manager** (gRPC, on rank 0) forms quorum and "reconfigur[es] the ProcessGroups" and restores checkpoints. Ranks talk to their Manager; Managers heartbeat the Lighthouse.
- **Failure detection at step granularity:** "Coordination primitives that can determine which workers are healthy via heartbeating on a per-step basis." The quorum "determines which workers are healthy using frequent heartbeats and guarantees … no split-brain conditions." Each step is treated as a distributed transaction (commit or rollback).
- **Explicit decoupling / reusability:** torchft ships a low-level `torchft.coordination` module that "exposes low level coordination APIs to allow you to build your own custom fault tolerance algorithms," and the README states torchft "is designed to provide the primitives required to implement fault tolerance in any application/train script." The `Quorum`/`QuorumMember` objects carry only generic membership metadata (replica_id, address, step, world_size, shrink_only, data) — **no gradient/all-reduce coupling.** Proof of independence: torchft's parameter-server example runs with **no Lighthouse at all.**
- **Failure domain = "replica group":** failures are isolated to one group; other groups reconfigure and continue; a recovered group rejoins via quorum and pulls weights peer-to-peer.

**How this maps to your project:** build a `membership` service exposing: `join`, `leave`, `heartbeat`, `get_members`, `get_quorum`, and (for Phase 2) `who_owns(region)` / `elect_leader(shard)`. In **Phase 1**, the DiLoCo trainer consults it to know which workers are in the current all-reduce group and to reconfigure when a pod is killed. In **Phase 2**, the *same* service provides shard ownership (via Raft leader election per shard) and drives agent hand-off when a region-server dies. The membership/heartbeat core is identical; only the "what do I do when membership changes" callback differs (reconfigure all-reduce vs. migrate agents). Ray/KubeRay is the other reference: Ray's GCS + KubeRay operator separate cluster-membership/lifecycle from the application actors — same principle, heavier weight. Keep your service transport-agnostic (gRPC interface) so the phases are pure clients.

### Q5 — COMMON PITFALLS (solo / first-timer)

- **Failure-detector timeout tuning.** The single most common bug: "too aggressive and you get false positives and unnecessary leader elections; too slow and you have a long window where a split brain can develop undetected." A slow node (GC pause, CPU spike, saturated link) is indistinguishable from a dead one. Mitigation: the SWIM suspicion state; adaptive timeouts.
- **Split-brain / zombie leaders.** After a partition heals, an old leader may still think it's leader ("zombie leader"), issuing conflicting commands. Mitigation: monotonically increasing **epoch/generation numbers** and **fencing** (reject requests from stale epochs); quorum (majority) for any ownership decision. This is *the* correctness trap in Phase 2's shard hand-off.
- **Testing failure is harder than preventing it.** "You can reason about Raft's correctness on paper, but you also need to verify … clock skew, disk full, partial network failures, and leader elections during compaction." Partial partitions (A sees B, B sees C, A can't see C) are nastier than clean splits.
- **Underestimating plumbing.** K8s RBAC, gRPC/protobuf wiring, dashboards, and getting `kind` + pod eviction working reliably will eat far more calendar time than the algorithms. Budget for it, and don't mistake it for the "real" learning.
- **Scope traps:** trying to make agent movement (BC/RL) impressive; chasing model quality instead of systems behavior; building a beautiful dashboard before the failure loop works; implementing Byzantine tolerance (you only need crash-stop + clean partitions). Most production systems only handle crash-stop failures and clean partitions — do the same.
- **DiLoCo-specific:** managing two model copies (θ(t) and θ(t+h)) and FP32 pseudo-gradient buffers correctly; not calling the gradient scaler during the outer step.

### Industry currency check (2026 job postings)

- **OpenAI** — infra roles list Kubernetes, Terraform, Helm, gRPC/FastAPI, Python/Rust/Go, distributed systems, observability; "Debug issues spanning Python, PyTorch, distributed systems, GPUs, networking, and storage." Notably uses Ray to coordinate ChatGPT training; one team is building an in-house orchestration platform "to scale far beyond … Kubernetes."
- **NVIDIA (DGX Cloud / SRE)** — "significant software engineering experience with kubernetes including cluster operations, operator development, node health monitoring and … GPU resource scheduling"; Go/C/Python; NCCL; failure domains, blast radii.
- **xAI** — "large-scale, high-throughput distributed systems"; Golang/Rust/Python; Kubernetes controllers/admission plugins; service meshes (Envoy); observability (Prometheus, Grafana, OpenTelemetry, VictoriaMetrics, ClickHouse); "failure domains, blast radii, and canary testing."
- **Anduril (Lattice OS)** — "robust, fault-tolerant distributed systems that can operate reliably over" degraded networks; gRPC, Kubernetes, edge/cloud.
- **Databricks** — new-grad "Distributed Data Systems" roles: distributed query execution/scheduling, transaction coordination, resource management; Kubernetes, Spark.
- **Palantir (Foundry/Gotham)** — "challenging problems in distributed systems, security, and large-scale data processing."
- **Waymo** — (not fully confirmed in this pass) large-scale simulation + distributed training infra; treat as directionally similar to the above.

**Takeaway:** Kubernetes + Go/Python(+Rust) + gRPC + Prometheus/Grafana/OTel + PyTorch distributed is squarely the current, in-demand stack. Ray/KubeRay is a strong bonus to know (OpenAI, Anyscale ecosystem; Ray joined the PyTorch Foundation).

---

## Recommendations

**Stage 0 — Learn while scaffolding (before building):** Work MIT 6.5840's Raft lab and read the SWIM paper. These two artifacts are the intellectual core of both phases. Read the `torchft` README + design doc and the DiLoCo papers as architecture references.

**Stage 1 — Build the shared spine first:** Implement a standalone `membership` service (heartbeat + suspicion à la SWIM; gRPC API) with a clean, application-agnostic interface. Get it working with plain processes before Kubernetes. **Benchmark that changes the plan:** if false-positive rate under induced latency is high, add the suspicion state / adaptive timeouts before moving on.

**Stage 2 — Phase 1 on `kind`, locally, $0:** DiLoCo inner/outer loop over N worker pods on a local `kind` cluster; wire the membership service to reconfigure the all-reduce group on pod death; Prometheus + a WebSocket loss-curve dashboard; a "kill random pod" button using the eviction API (or Chaos Mesh if you want richer faults). Produce the loss-vs-wall-clock plot at 0/5/20 kills. **Threshold:** if training can't continue through a kill without restart, the membership→reconfigure callback is wrong — fix before scaling kill count.

**Stage 3 — Phase 2 reuses the spine:** Add per-shard **Raft leader election** (reuse the membership service for the peer set) for region ownership; implement agent hand-off with epoch/fencing to avoid zombie owners; define the cross-boundary consistency policy explicitly (e.g., primary-owns-writes + read-only ghosts). Movement model: start with behavior cloning on inD/PIE; keep it lightweight. **Threshold for "done":** kill a region-server and see <1s hand-off with no agent loss — if agents duplicate or vanish, your consistency/fencing policy is the culprit.

**Stage 4 — Optional real-cloud chaos: not implemented (optional, out of scope).** Only if you want a genuine network-partition demo, spin up a few Oracle Cloud always-free ARM VMs (or DigitalOcean $200 student credit) and repeat a partition experiment. Otherwise skip — it adds cost/complexity, not learning. **Final call for this project: skipped** — Stages 1-3 already demonstrate the core distributed-systems learning goals at $0 cost.

**Escalation triggers to change tooling:** if `kind` can't model the failure you need (e.g., real kernel/network faults), move that one experiment to `k3d` or cloud VMs; if Prometheus+Grafana is too heavy for a solo timeline, a minimal custom dashboard suffices; if hand-implementing Raft stalls the timeline, use the 6.5840 lab structure as a scaffold rather than starting blank.

---

## Caveats

- **Fast-moving space; verify before committing.** DiLoCo is an active 2026 research area (Decoupled DiLoCo shipped April 2026) — the "88% goodput vs 27%" and Gemma-accuracy figures come from DeepMind's own blog and press coverage (some via aggregators like blockchain.news and MarkTechPost), so treat them as reported claims, not independently benchmarked facts.
- **`torchft` internals:** the coordination-module quotes are from the official GitHub README and docs; the Rust attribution for the Lighthouse is inferred from the build toolchain (pyo3/maturin/cargo, RUST_BACKTRACE launch) rather than a single explicit sentence, and the official design doc (a Google Doc) could not be fully retrieved. No torchft doc discusses a multi-agent-simulation use case — the "reusable for any application" claim is genuine but framed around ML training.
- **SpatialOS** is essentially deprecated/criticized; use it only as a conceptual precedent, not a dependency. Academic spatial-agent frameworks (MASS, Repast HPC) are references, not turnkey tools.
- **Waymo** job-posting specifics were not fully confirmed in this research pass; the other six companies' postings are directly quoted.
- **Student credits change frequently.** The Oracle Always Free A1 allowance was halved June 15, 2026 (2 OCPU / 12 GB); the DigitalOcean $200 (through 7/31/26), Azure $100, and AWS Educate amounts were current in 2026 sources but verify on the official pages before relying on them; GitHub paused free Copilot student sign-ups in April 2026, a sign these terms shift.
- **Observability "newer tools" (OpenObserve, SigNoz)** claims of "60–90% savings" are vendor marketing; Prometheus/Grafana/OTel remain the safe, industry-standard default.
