import { engine, useEngine } from "../engine/client";

/** Covers a playground until its first snapshot arrives, and explains failures. */
export function Boot() {
  const { status, stage, error } = useEngine();
  if (status === "error") {
    return (
      <div className="boot" role="alert">
        <p className="boot-title">The Python engine couldn't start</p>
        <p className="boot-sub mono">{error}</p>
        <button className="btn" onClick={() => location.reload()}>
          Reload
        </button>
      </div>
    );
  }
  return (
    <div className="boot" role="status">
      <div className="bar" aria-hidden="true">
        <span />
      </div>
      <p className="boot-title">{stage || "Waking up the cluster"}</p>
      <p className="boot-sub">Downloading CPython (WebAssembly) once, about 10&nbsp;MB. Everything after this is local.</p>
    </div>
  );
}

export { engine };
