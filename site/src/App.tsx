import { Consensus } from "./components/Consensus";
import { Hero } from "./components/Hero";
import { Hood } from "./components/Hood";
import { Training } from "./components/Training";
import { World } from "./components/World";

const REPO = "https://github.com/rkothari3/DSSD";

export function App() {
  return (
    <>
      <a className="skip" href="#main">Skip to content</a>
      <nav className="nav" aria-label="Primary">
        <div className="wrap nav-in">
          <a className="brand mono" href="#top" aria-label="DSSD, back to top">
            <svg width="20" height="20" viewBox="0 0 32 32" aria-hidden="true"><path d="M16 7 25 13 21.5 24H10.5L7 13Z" fill="none" stroke="#5eead4" strokeWidth="2" opacity=".7" /><circle cx="16" cy="7" r="3" fill="#5eead4" /><circle cx="25" cy="13" r="2.4" fill="#5eead4" /><circle cx="21.5" cy="24" r="2.4" fill="#5eead4" /><circle cx="10.5" cy="24" r="2.4" fill="#5eead4" /><circle cx="7" cy="13" r="2.4" fill="#5eead4" /></svg>
            DSSD
          </a>
          <div className="nav-links">
            <a href="#consensus">Consensus</a>
            <a href="#world">World</a>
            <a href="#training">Training</a>
            <a href="#hood">Under the hood</a>
          </div>
          <a className="btn" href={REPO} target="_blank" rel="noreferrer">GitHub</a>
        </div>
      </nav>
      <main id="main">
        <span id="top" />
        <Hero />
        <Consensus />
        <World />
        <Training />
        <Hood />
      </main>
      <footer className="footer">
        <div className="wrap">
          <p>Built from scratch in Python. The playgrounds run the real source in your browser via Pyodide, with nothing sent to a server.</p>
          <p><a href={REPO} target="_blank" rel="noreferrer">GitHub</a> · <a href={`${REPO}#readme`} target="_blank" rel="noreferrer">README</a></p>
        </div>
      </footer>
    </>
  );
}
