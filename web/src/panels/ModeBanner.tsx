import type { AppState } from "../state/store";

/** The persistent LIVE / REPLAY badge and what it means. */
export function ModeBanner({ state }: { state: AppState }) {
  if (state.mode === "connecting") {
    return (
      <div className="banner" role="status">
        <span>Connecting to the gateway…</span>
      </div>
    );
  }
  if (state.mode === "live") {
    return (
      <div className="banner" role="status">
        <span className="badge badge-live">LIVE</span>
        <span>Showing the live cluster.</span>
        {state.sse === "unavailable" ? (
          <span className="flag">live events unavailable (viewer limit?); polling continues</span>
        ) : null}
      </div>
    );
  }
  const why =
    state.replayReason === "unreachable"
      ? "live gateway unreachable; playing a recorded session"
      : state.replayReason === "no-recording"
        ? "no recording is available"
        : "replay only: playing a recorded session";
  return (
    <div className="banner" role="status">
      <span className="badge badge-replay">REPLAY</span>
      <span>{why}</span>
      {state.recordedAt ? (
        <span>recorded {new Date(state.recordedAt).toISOString().slice(0, 10)}</span>
      ) : null}
      {state.synthetic ? <span className="flag">synthetic sample recording</span> : null}
    </div>
  );
}
