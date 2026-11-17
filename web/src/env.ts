// The only place import.meta.env is read. All values are public by nature.
export interface Env {
  gatewayUrl: string;
  replayOnly: boolean;
  repoUrl: string;
}

export function readEnv(): Env {
  return {
    gatewayUrl: (import.meta.env.VITE_GATEWAY_URL ?? "").replace(/\/+$/, ""),
    replayOnly: import.meta.env.VITE_REPLAY_ONLY === "true",
    repoUrl: import.meta.env.VITE_REPO_URL ?? "",
  };
}
