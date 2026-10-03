// Runs the real dssd Python (swim.py, raft.py, sharding/…) in Pyodide, off the UI thread.
import type { ClusterName, FromWorker, ToWorker } from "./types";

const ctx = self as unknown as {
  postMessage(m: FromWorker): void;
  onmessage: ((e: MessageEvent<ToWorker>) => void) | null;
};
const send = (m: FromWorker) => ctx.postMessage(m);

interface PyodideLike {
  runPythonAsync(code: string): Promise<unknown>;
  runPython(code: string): unknown;
  globals: { set(k: string, v: unknown): void };
  unpackArchive(buf: ArrayBuffer, fmt: string): void;
  FS: { mkdirTree(p: string): void; writeFile(p: string, s: string): void };
  setStderr(o: { batched: (s: string) => void }): void;
}

let py: PyodideLike | null = null;
const pending: ToWorker[] = [];

const BOOTSTRAP = `
import json
from dssd.playground import Consensus, World, HUMAN

_clusters = {}

def start(name):
    if name not in _clusters:
        cls = Consensus if name == "consensus" else World
        _clusters[name] = cls(lambda s, n=name: post_snapshot(n, s), HUMAN)
        _clusters[name].start()

def stop(name):
    cluster = _clusters.pop(name, None)
    if cluster:
        cluster.stop()

def reset(name):
    if name in _clusters:
        _clusters[name].reset()

def call(name, method, args_json):
    if name in _clusters:
        getattr(_clusters[name], method)(*json.loads(args_json))
`;

async function boot(base: string) {
  send({ type: "progress", stage: "Loading the Python runtime" });
  const { loadPyodide } = (await import(/* @vite-ignore */ `${base}pyodide/pyodide.mjs`)) as {
    loadPyodide(o: { indexURL: string }): Promise<PyodideLike>;
  };
  const p = await loadPyodide({ indexURL: `${base}pyodide/` });
  p.setStderr({ batched: (s) => console.warn("[python]", s) });

  send({ type: "progress", stage: "Loading protobuf and the dssd source" });
  const [wheel, bundle] = await Promise.all([
    fetch(`${base}py/protobuf.whl`).then((r) => r.arrayBuffer()),
    fetch(`${base}py/dssd.json`).then((r) => r.json() as Promise<Record<string, string>>),
  ]);
  p.unpackArchive(wheel, "wheel");
  for (const [path, source] of Object.entries(bundle)) {
    const full = `/home/pyodide/${path}`;
    p.FS.mkdirTree(full.slice(0, full.lastIndexOf("/")));
    p.FS.writeFile(full, source);
  }
  p.runPython("import sys; sys.path.insert(0, '/home/pyodide')");
  p.globals.set("post_snapshot", (cluster: ClusterName, data: string) => send({ type: "snapshot", cluster, data }));
  send({ type: "progress", stage: "Starting the cluster" });
  await p.runPythonAsync(BOOTSTRAP);
  py = p;
  send({ type: "ready" });
  for (const m of pending.splice(0)) await handle(m);
}

async function handle(m: ToWorker) {
  if (!py) return void pending.push(m);
  try {
    if (m.type === "call") {
      py.globals.set("_args", JSON.stringify(m.args));
      await py.runPythonAsync(`call(${JSON.stringify(m.cluster)}, ${JSON.stringify(m.method)}, _args)`);
    } else if (m.type !== "init") {
      await py.runPythonAsync(`${m.type}(${JSON.stringify(m.cluster)})`);
    }
  } catch (e) {
    send({ type: "error", message: String(e) });
  }
}

ctx.onmessage = (e) => {
  const m = e.data;
  if (m.type === "init") boot(m.base).catch((err) => send({ type: "error", message: String(err) }));
  else void handle(m);
};
