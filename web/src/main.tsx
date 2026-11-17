import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { readEnv } from "./env";
import { createRuntime } from "./state/runtime";
import "./styles.css";

const env = readEnv();
const runtime = createRuntime({
  env,
  fetch: window.fetch.bind(window),
  EventSource: window.EventSource,
  rand: Math.random,
  search: window.location.search,
  isHidden: () => document.hidden,
  onVisibility: (fn) => {
    document.addEventListener("visibilitychange", fn);
    return () => document.removeEventListener("visibilitychange", fn);
  },
});

void runtime.start();

const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <StrictMode>
      <App runtime={runtime} repoUrl={env.repoUrl} />
    </StrictMode>,
  );
}
