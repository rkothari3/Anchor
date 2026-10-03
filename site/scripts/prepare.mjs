// Builds everything the site loads at runtime that isn't hand-written:
//  - the Pyodide runtime (self-hosted, no CDN),
//  - the vendored pure-Python protobuf wheel,
//  - the REAL dssd Python sources (+ a tiny grpc shim) as one JSON bundle,
//  - the REAL training results (results/*.csv) as JSON for the chart.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const site = path.resolve(here, "..");
const repo = path.resolve(site, "..");
const pub = path.join(site, "public");

const copy = (from, to) => { fs.mkdirSync(path.dirname(to), { recursive: true }); fs.copyFileSync(from, to); };

// 1. Pyodide runtime
const pyodide = path.join(site, "node_modules", "pyodide");
for (const f of ["pyodide.mjs", "pyodide.asm.mjs", "pyodide.asm.wasm", "python_stdlib.zip", "pyodide-lock.json"]) {
  copy(path.join(pyodide, f), path.join(pub, "pyodide", f));
}

// 2. protobuf wheel
const wheel = fs.readdirSync(path.join(site, "vendor")).find((f) => f.endsWith(".whl"));
copy(path.join(site, "vendor", wheel), path.join(pub, "py", "protobuf.whl"));

// 3. dssd sources + grpc shim
const bundle = {};
const addPy = (dir, prefix, skip = []) => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === "__pycache__") continue;
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) addPy(p, `${prefix}/${entry.name}`, skip);
    else if (entry.name.endsWith(".py") && !skip.includes(entry.name)) bundle[`${prefix}/${entry.name}`] = fs.readFileSync(p, "utf8");
  }
};
const src = path.join(repo, "src", "dssd");
for (const f of ["__init__.py", "addr.py", "raft.py", "swim.py", "playground.py"]) bundle[`dssd/${f}`] = fs.readFileSync(path.join(src, f), "utf8");
addPy(path.join(src, "spinepb"), "dssd/spinepb");
addPy(path.join(src, "regionpb"), "dssd/regionpb");
addPy(path.join(src, "sharding"), "dssd/sharding", ["cli.py"]);
addPy(path.join(site, "py", "grpc"), "grpc");
fs.mkdirSync(path.join(pub, "py"), { recursive: true });
fs.writeFileSync(path.join(pub, "py", "dssd.json"), JSON.stringify(bundle));

// 4. training results
const runs = [0, 5, 20].map((kills) => {
  const lines = fs.readFileSync(path.join(repo, "results", `kills_${kills}.csv`), "utf8").trim().split("\n").slice(1);
  const points = lines.map((l) => { const [t, y] = l.split(","); return [Number(t), y === "" ? null : Number(y)]; });
  return { kills, points };
});
fs.mkdirSync(path.join(site, "src", "data"), { recursive: true });
fs.writeFileSync(path.join(site, "src", "data", "training.json"), JSON.stringify({ runs }));

const files = Object.keys(bundle).length;
console.log(`prepare: ${files} python files, pyodide runtime, protobuf wheel, ${runs.length} training runs`);
